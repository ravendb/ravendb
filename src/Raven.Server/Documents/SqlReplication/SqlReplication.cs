using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Server.Documents.Patch;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Documents.SqlReplication
{
    public class SqlReplication : IDisposable
    {
        public readonly SqlReplicationConfiguration Configuration;
        public readonly SqlReplicationStatistics Statistics;

        public string ReplicationUniqueName => Configuration.Name;
        public CancellationToken CancellationToken => _cancellationTokenSource.Token;

        private bool _shouldWaitForChanges;
        private PredefinedSqlConnection _predefinedSqlConnection;
        public readonly SqlReplicationMetricsCountersManager MetricsCountersManager;
        protected Logger _logger;
        protected DocumentDatabase _database;
        protected Thread _replicationThread;
        protected bool _disposed;
        protected CancellationTokenSource _cancellationTokenSource;
        public AsyncManualResetEvent WaitForChanges;

        public SqlReplication(DocumentDatabase database, SqlReplicationConfiguration configuration)
        {
            _logger = LoggingSource.Instance.GetLogger(database.Name, GetType().FullName);
            _database = database;
            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
            WaitForChanges = new AsyncManualResetEvent(_cancellationTokenSource.Token);

            Configuration = configuration;
            Statistics = new SqlReplicationStatistics(configuration.Name);
            MetricsCountersManager = new SqlReplicationMetricsCountersManager();
        }
      
        private void LoadLastEtag(DocumentsOperationContext context)
        {
            var sqlReplicationStatus = _database.DocumentsStorage.Get(context, Constants.SqlReplication.RavenSqlReplicationStatusPrefix + ReplicationUniqueName);
            if (sqlReplicationStatus == null)
            {
                Statistics.LastReplicatedEtag = 0;
                Statistics.LastTombstonesEtag = 0;
            }
            else
            {
                var replicationStatus = JsonDeserializationServer.SqlReplicationStatus(sqlReplicationStatus.Data);
                Statistics.LastReplicatedEtag = replicationStatus.LastReplicatedEtag;
                Statistics.LastTombstonesEtag = replicationStatus.LastTombstonesEtag;
            }
        }

        private void WriteLastEtag(DocumentsOperationContext context)
        {
            var key = Constants.SqlReplication.RavenSqlReplicationStatusPrefix + ReplicationUniqueName;
            var document = context.ReadObject(new DynamicJsonValue
            {
                ["Name"] = ReplicationUniqueName,
                ["LastReplicatedEtag"] = Statistics.LastReplicatedEtag,
                ["LastTombstonesEtag"] = Statistics.LastTombstonesEtag,
            }, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            _database.DocumentsStorage.Put(context, key, null, document);
        }

        protected Task ExecuteReplicationOnce()
        {
            if (Configuration.Disabled)
                return Task.CompletedTask;
            if (Statistics.SuspendUntil.HasValue && Statistics.SuspendUntil.Value > SystemTime.UtcNow)
                return Task.CompletedTask;

            int countOfReplicatedItems = 0;
            var startTime = SystemTime.UtcNow;
            var spRepTime = new Stopwatch();
            spRepTime.Start();

            _shouldWaitForChanges = false;
            try
            {
                DocumentsOperationContext context;
                bool hasReplicated;
                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenReadTransaction())
                {
                    LoadLastEtag(context);
                    
                    hasReplicated = ReplicateDeletionsToDestination(context) ||
                                 ReplicateChangesToDestination(context, out countOfReplicatedItems);

                    tx.Commit();
                }

                if (hasReplicated)
                {
                    using (var tx = context.OpenWriteTransaction())
                    {
                        WriteLastEtag(context);
                        tx.Commit();
                    }
                    _shouldWaitForChanges = true;
                }
            }
            finally
            {
                spRepTime.Stop();
                MetricsCountersManager.SqlReplicationBatchSizeMeter.Mark(countOfReplicatedItems);
                MetricsCountersManager.UpdateReplicationPerformance(new SqlReplicationPerformanceStats
                {
                    BatchSize = countOfReplicatedItems,
                    Duration = spRepTime.Elapsed,
                    Started = startTime
                });

                var afterReplicationCompleted = _database.SqlReplicationLoader.AfterReplicationCompleted;
                afterReplicationCompleted?.Invoke(Statistics);
            }

            return Task.CompletedTask;
        }

        protected bool HasMoreDocumentsToSend()
        {
            return _shouldWaitForChanges;
        }

        private bool ReplicateDeletionsToDestination(DocumentsOperationContext context)
        {
            var pageSize = int.MaxValue; // _database.Configuration.Indexing.MaxNumberOfTombstonesToFetch;

            var documents = _database.DocumentsStorage.GetTombstonesFrom(context, Configuration.Collection, Statistics.LastTombstonesEtag + 1, 0, pageSize).ToList();
            if (documents.Count == 0)
                return false;

            Statistics.LastTombstonesEtag = documents.Last().Etag;

            var documentsKeys = documents.Select(tombstone => (string)tombstone.Key).ToList();
            using (var writer = new RelationalDatabaseWriter(_database, context, _predefinedSqlConnection, this))
            {
                foreach (var sqlReplicationTable in Configuration.SqlReplicationTables)
                {
                    writer.DeleteItems(sqlReplicationTable.TableName, sqlReplicationTable.DocumentKeyColumn, Configuration.ParameterizeDeletesDisabled, documentsKeys);
                }
                writer.Commit();
                if (_logger.IsInfoEnabled)
                    _logger.Info("Replicated deletes of " + string.Join(", ", documentsKeys)  + " for config " + Configuration.Name);
            }
            return true;
        }

        private bool ReplicateChangesToDestination(DocumentsOperationContext context, out int countOfReplicatedItems)
        {
            countOfReplicatedItems = 0;
            var pageSize = int.MaxValue; // _database.Configuration.Indexing.MaxNumberOfDocumentsToFetchForMap;

            var documents = _database.DocumentsStorage.GetDocumentsFrom(context, Configuration.Collection, Statistics.LastReplicatedEtag + 1, 0, pageSize).ToList();
            if (documents.Count == 0)
                return false;

            Statistics.LastReplicatedEtag = documents.Last().Etag;

            var scriptResult = ApplyConversionScript(documents, context);
            if (scriptResult.Keys.Count == 0)
                return true;

            countOfReplicatedItems = scriptResult.Data.Sum(x => x.Value.Count);
            try
            {
                using (var writer = new RelationalDatabaseWriter(_database, context, _predefinedSqlConnection, this))
                {
                    if (writer.ExecuteScript(scriptResult))
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Replicated changes of " + string.Join(", ", documents.Select(d => d.Key)) + " for replication " +  Configuration.Name);
                        Statistics.CompleteSuccess(countOfReplicatedItems);
                    }
                    else
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Replicated changes (with some errors) of " + string.Join(", ", documents.Select(d => d.Key))  + " for replication " + Configuration.Name);
                        Statistics.Success(countOfReplicatedItems);
                    }
                }
                return true;
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Failure to replicate changes to relational database for: " + Configuration.Name, e);
                DateTime newTime;
                if (Statistics.LastErrorTime == null)
                {
                    newTime = SystemTime.UtcNow.AddSeconds(5);
                }
                else
                {
                    // double the fallback time (but don't cross 15 minutes)
                    var totalSeconds = (SystemTime.UtcNow - Statistics.LastErrorTime.Value).TotalSeconds;
                    newTime = SystemTime.UtcNow.AddSeconds(Math.Min(60*15, Math.Max(5, totalSeconds*2)));
                }
                Statistics.RecordWriteError(e, _database, countOfReplicatedItems, newTime);
                return false;
            }
        }

        public SqlReplicationScriptResult ApplyConversionScript(List<Document> documents, DocumentsOperationContext context)
        {
            var result = new SqlReplicationScriptResult();
            foreach (var replicatedDoc in documents)
            {
                _cancellationTokenSource.Token.ThrowIfCancellationRequested();
                var patcher = new SqlReplicationPatchDocument(_database, context, result, Configuration, replicatedDoc.Key);
                try
                {
                    var scope = patcher.Apply(context, replicatedDoc, new PatchRequest { Script = Configuration.Script });

                    if (_logger.IsInfoEnabled && scope.DebugInfo.Count > 0)
                        _logger.Info(string.Format("Debug output for doc: {0} for script {1}:\r\n.{2}", replicatedDoc.Key, Configuration.Name, string.Join("\r\n", scope.DebugInfo.Items)));

                    Statistics.ScriptSuccess();
                }
                catch (ParseException e)
                {
                    Statistics.MarkScriptAsInvalid(_database, Configuration.Script);

                    if (_logger.IsInfoEnabled)
                        _logger.Info("Could not parse SQL Replication script for " + Configuration.Name, e);

                    return result;
                }
                catch (Exception diffExceptionName)
                {
                    Statistics.RecordScriptError(_database, diffExceptionName);
                    if (_logger.IsInfoEnabled)
                        _logger.Info("Could not process SQL Replication script for " + Configuration.Name + ", skipping document: " + replicatedDoc.Key, diffExceptionName);
                }
            }
            return result;
        }

        public bool PrepareSqlReplicationConfig(BlittableJsonReaderObject connections, bool writeToLog = true)
        {
            if (string.IsNullOrWhiteSpace(Configuration.ConnectionStringName) == false)
            {
                object connection;
                if (connections.TryGetMember(Configuration.ConnectionStringName, out connection))
                {
                    _predefinedSqlConnection = JsonDeserializationServer.PredefinedSqlConnection(connection as BlittableJsonReaderObject);
                    if (_predefinedSqlConnection != null)
                    {
                        return true;
                    }
                }

                if (writeToLog)
                    if (_logger.IsInfoEnabled)
                        _logger.Info("Could not find connection string named '" + Configuration.ConnectionStringName
                            + "' for sql replication config: " + Configuration.Name + ", ignoring sql replication setting.");
                Statistics.LastAlert = new Alert
                {
                    CreatedAt = SystemTime.UtcNow,
                    Type = AlertType.SqlReplicationConnectionStringMissing,
                    Severity = AlertSeverity.Error,
                    Message = "Could not start replication",
                    Content = new ExceptionAlertContent
                    {
                        Message = $"Could not find connection string named '{Configuration.ConnectionStringName}' for sql replication config: {Configuration.Name}, ignoring sql replication setting.",
                    }
                };
                return false;
            }

            if (writeToLog)
                if (_logger.IsInfoEnabled)
                    _logger.Info("Connection string name cannot be empty for sql replication config: " + Configuration.ConnectionStringName +", ignoring sql replication setting.");
            Statistics.LastAlert = new Alert
            {
                Type = AlertType.SqlReplicationConnectionStringMissing,
                CreatedAt = SystemTime.UtcNow,
                Severity = AlertSeverity.Error,
                Message = "Could not start replication",
                Content = new ExceptionAlertContent
                {
                    Message = $"Connection string name cannot be empty for sql replication config: {Configuration.Name}, ignoring sql replication setting."
                }
            };
            return false;
        }

        public bool ValidateName()
        {
            if (string.IsNullOrWhiteSpace(Configuration.Name) == false)
                return true;

            if (_logger.IsInfoEnabled)
                _logger.Info($"Could not find name for sql replication document {Configuration.Name}, ignoring");
            Statistics.LastAlert = new Alert
            {
                Type = AlertType.SqlReplicationConnectionStringMissing,
                Severity = AlertSeverity.Error,
                Message = "Could not start replication",
                CreatedAt = DateTime.UtcNow,
                Content = new ExceptionAlertContent
                {
                    Message = $"Could not find name for sql replication document {Configuration.Name}, ignoring"
                }
            };
            return false;
        }

        public DynamicJsonValue Simulate(SimulateSqlReplication simulateSqlReplication, DocumentsOperationContext context, SqlReplicationScriptResult result)
        {
            if (simulateSqlReplication.PerformRolledBackTransaction)
            {
                using (var writer = new RelationalDatabaseWriter(_database, context, _predefinedSqlConnection, this))
                {
                    return new DynamicJsonValue
                    {
                        ["Results"] = new DynamicJsonArray(writer.RolledBackExecute(result).ToArray()),
                        ["LastAlert"] = Statistics.LastAlert,
                    };
                }
            }

            var simulatedwriter = new RelationalDatabaseWriterSimulator(_predefinedSqlConnection, this);
            var tableQuerySummaries = new List<RelationalDatabaseWriter.TableQuerySummary>
                {
                    new RelationalDatabaseWriter.TableQuerySummary
                    {
                        Commands = simulatedwriter.SimulateExecuteCommandText(result)
                            .Select(x => new RelationalDatabaseWriter.TableQuerySummary.CommandData
                            {
                                CommandText = x
                            }).ToArray()
                    }
                }.ToArray();
            return new DynamicJsonValue
            {
                ["Results"] = new DynamicJsonArray(tableQuerySummaries),
                ["LastAlert"] = Statistics.LastAlert,
            };
        }

        public void Start()
        {
            if (_replicationThread != null)
                return;

            _replicationThread = new Thread(() =>
            {
                // This has lower priority than request processing, so we let the OS
                // schedule this appropriately
                Threading.TryLowerCurrentThreadPriority();

                //haven't found better way to synchronize async method
                AsyncHelpers.RunSync(ExecuteReplicationLoop);
            })
            {
                Name = $"Replication thread, {ReplicationUniqueName}",
                IsBackground = true
            };

            _replicationThread.Start();
        }

        private async Task ExecuteReplicationLoop()
        {
            while (_cancellationTokenSource.IsCancellationRequested == false)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Starting replication for '{ReplicationUniqueName}'.");

                WaitForChanges.Reset();

                try
                {
                    _cancellationTokenSource.Token.ThrowIfCancellationRequested();

                    await ExecuteReplicationOnce();

                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Finished replication for '{ReplicationUniqueName}'.");
                }
                catch (OutOfMemoryException oome)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Out of memory occured for '{ReplicationUniqueName}'.", oome);
                    // TODO [ppekrol] GC?
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Exception occured for '{ReplicationUniqueName}'.", e);
                }

                if (HasMoreDocumentsToSend())
                    continue;

                try
                {
                    //if this returns false, this means canceled token is activated                    
                    if (await WaitForChanges.WaitAsync() == false)
                        //thus, if code reaches here, cancellation token source has "cancel" requested
                        return; 
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }

        public virtual void Dispose()
        {
            try
            {
                _cancellationTokenSource.Cancel();					
            }
            catch (ObjectDisposedException)
            {
                //precaution, should not happen
                if (_logger.IsInfoEnabled)
                    _logger.Info("ObjectDisposedException thrown during replication executer disposal, should not happen. Something is wrong here.");
            }
            catch (AggregateException e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error during replication executer disposal, most likely it is a bug.",e);
            }
            finally
            {
                _disposed = true;
            }
        }
    }
}