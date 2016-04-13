using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.Documents.Patch;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Metrics;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.SqlReplication
{
    public class SqlReplication : IDisposable
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(SqlReplication));

        private readonly DocumentDatabase _database;
        public readonly SqlReplicationConfiguration Configuration;
        public readonly SqlReplicationStatistics Statistics;

        public readonly ManualResetEventSlim WaitForChanges = new ManualResetEventSlim();

        public string Name => Configuration.Name;

        private readonly CancellationTokenSource _cancellationTokenSource;
        public CancellationToken CancellationToken => _cancellationTokenSource.Token;

        private Thread _sqlReplicationThread;
        private bool _disposed;
        private PredefinedSqlConnection _predefinedSqlConnection;
        public readonly SqlReplicationMetricsCountersManager MetricsCountersManager;

        public SqlReplication(DocumentDatabase database, SqlReplicationConfiguration configuration, MetricsScheduler metricsScheduler)
        {
            _database = database;
            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
            Configuration = configuration;
            Statistics = new SqlReplicationStatistics(configuration.Name);
            MetricsCountersManager = new SqlReplicationMetricsCountersManager(metricsScheduler);
        }

        public void Start()
        {
            _sqlReplicationThread = new Thread(ExecuteSqlReplication)
            {
                Name = "Sql replication of " + Name,
                IsBackground = true
            };

            _sqlReplicationThread.Start();
        }

        private void ExecuteSqlReplication()
        {
            while (_cancellationTokenSource.IsCancellationRequested == false)
            {
                if (Log.IsDebugEnabled)
                    Log.Debug($"Starting sql replication for '{Name}'.");

                WaitForChanges.Reset();

                try
                {
                    _cancellationTokenSource.Token.ThrowIfCancellationRequested();

                    DoWork();

                    if (Log.IsDebugEnabled)
                        Log.Debug($"Finished sql replication for '{Name}'.");
                }
                catch (OutOfMemoryException oome)
                {
                    Log.WarnException($"Out of memory occured for '{Name}'.", oome);
                    // TODO [ppekrol] GC?
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception e)
                {
                    Log.WarnException($"Exception occured for '{Name}'.", e);
                }

                try
                {
                    WaitForChanges.Wait(_cancellationTokenSource.Token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }

        private void LoadLastEtag(DocumentsOperationContext context)
        {
            var sqlReplicationStatus = _database.DocumentsStorage.Get(context, Constants.SqlReplication.RavenSqlReplicationStatusPrefix + Name);
            if (sqlReplicationStatus == null)
            {
                Statistics.LastReplicatedEtag = 0;
                Statistics.LastTombstonesEtag = 0;
            }
            else
            {
                var replicationStatus = JsonDeserialization.SqlReplicationStatus(sqlReplicationStatus.Data);
                Statistics.LastReplicatedEtag = replicationStatus.LastReplicatedEtag;
                Statistics.LastTombstonesEtag = replicationStatus.LastTombstonesEtag;
            }
        }

        private void WriteLastEtag(DocumentsOperationContext context)
        {
            var key = Constants.SqlReplication.RavenSqlReplicationStatusPrefix + Name;
            var document = context.ReadObject(new DynamicJsonValue
            {
                ["Name"] = Name,
                ["LastReplicatedEtag"] = Statistics.LastReplicatedEtag,
                ["LastTombstonesEtag"] = Statistics.LastTombstonesEtag,
            }, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            _database.DocumentsStorage.Put(context, key, null, document);
        }

        private void DoWork()
        {
            if (Configuration.Disabled)
                return;
            if (Statistics.SuspendUntil.HasValue && Statistics.SuspendUntil.Value > SystemTime.UtcNow)
                return;

            int countOfReplicatedItems = 0;
            var startTime = SystemTime.UtcNow;
            var spRepTime = new Stopwatch();
            spRepTime.Start();

            try
            {
                DocumentsOperationContext context;
                bool updateEtag;
                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenReadTransaction())
                {
                    LoadLastEtag(context);
                    
                    updateEtag = ReplicateDeletionsToDestination(context) ||
                                 ReplicateChangesToDestination(context, out countOfReplicatedItems);

                    tx.Commit();
                }

                if (updateEtag)
                {
                    using (var tx = context.OpenWriteTransaction())
                    {
                        WriteLastEtag(context);
                        tx.Commit();
                    }
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
        }

        private bool ReplicateDeletionsToDestination(DocumentsOperationContext context)
        {
            var pageSize = _database.Configuration.Indexing.MaxNumberOfTombstonesToFetch;

            var documents = _database.DocumentsStorage.GetTombstonesAfter(context, Configuration.Collection, Statistics.LastTombstonesEtag + 1, 0, pageSize).ToList();
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
                if (Log.IsDebugEnabled)
                    Log.Debug("Replicated deletes of {0} for config {1}", string.Join(", ", documentsKeys), Configuration.Name);
            }
            return true;
        }

        private bool ReplicateChangesToDestination(DocumentsOperationContext context, out int countOfReplicatedItems)
        {
            countOfReplicatedItems = 0;
            var pageSize = _database.Configuration.Indexing.MaxNumberOfDocumentsToFetchForMap;

            var documents = _database.DocumentsStorage.GetDocumentsAfter(context, Configuration.Collection, Statistics.LastReplicatedEtag + 1, 0, pageSize).ToList();
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
                        if (Log.IsDebugEnabled)
                            Log.Debug("Replicated changes of {0} for replication {1}", string.Join(", ", documents.Select(d => d.Key)), Configuration.Name);
                        Statistics.CompleteSuccess(countOfReplicatedItems);
                    }
                    else
                    {
                        if (Log.IsDebugEnabled)
                            Log.Debug("Replicated changes (with some errors) of {0} for replication {1}", string.Join(", ", documents.Select(d => d.Key)), Configuration.Name);
                        Statistics.Success(countOfReplicatedItems);
                    }
                }
                return true;
            }
            catch (Exception e)
            {
                Log.WarnException("Failure to replicate changes to relational database for: " + Configuration.Name, e);
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

                    if (Log.IsDebugEnabled && scope.DebugInfo.Count > 0)
                    {
                        Log.Debug("Debug output for doc: {0} for script {1}:\r\n.{2}", replicatedDoc.Key, Configuration.Name, string.Join("\r\n", scope.DebugInfo.Items));
                    }

                    Statistics.ScriptSuccess();
                }
                catch (ParseException e)
                {
                    Statistics.MarkScriptAsInvalid(_database, Configuration.Script);

                    Log.WarnException("Could not parse SQL Replication script for " + Configuration.Name, e);

                    return result;
                }
                catch (Exception diffExceptionName)
                {
                    Statistics.RecordScriptError(_database, diffExceptionName);
                    Log.WarnException("Could not process SQL Replication script for " + Configuration.Name + ", skipping document: " + replicatedDoc.Key, diffExceptionName);
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
                    _predefinedSqlConnection = JsonDeserialization.PredefinedSqlConnection(connection as BlittableJsonReaderObject);
                    if (_predefinedSqlConnection != null)
                    {
                        return true;
                    }
                }

                if (writeToLog)
                    Log.Warn("Could not find connection string named '{0}' for sql replication config: {1}, ignoring sql replication setting.",
                        Configuration.ConnectionStringName,
                        Configuration.Name);
                Statistics.LastAlert = new Alert
                {
                    IsError = true,
                    CreatedAt = DateTime.UtcNow,
                    Title = "Could not start replication",
                    Message = $"Could not find connection string named '{Configuration.ConnectionStringName}' for sql replication config: {Configuration.Name}, ignoring sql replication setting.",
                };
                return false;
            }

            if (writeToLog)
                Log.Warn("Connection string name cannot be empty for sql replication config: {1}, ignoring sql replication setting.",
                    Configuration.ConnectionStringName,
                    Configuration.Name);
            Statistics.LastAlert = new Alert
            {
                IsError = true,
                CreatedAt = DateTime.UtcNow,
                Title = "Could not start replication",
                Message = $"Connection string name cannot be empty for sql replication config: {Configuration.Name}, ignoring sql replication setting.",
            };
            return false;
        }

        public void Dispose()
        {
            _cancellationTokenSource.Cancel();
            _disposed = true;
        }

        public bool ValidateName()
        {
            if (string.IsNullOrWhiteSpace(Configuration.Name) == false)
                return true;

            Log.Warn($"Could not find name for sql replication document {Configuration.Name}, ignoring");
            Statistics.LastAlert = new Alert
            {
                IsError = true,
                CreatedAt = DateTime.UtcNow,
                Title = "Could not start replication",
                Message = $"Could not find name for sql replication document {Configuration.Name}, ignoring"
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
    }
}