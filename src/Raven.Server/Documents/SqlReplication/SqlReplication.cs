using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.Documents.Patch;
using Raven.Server.Json;
using Raven.Server.ReplicationUtil;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.SqlReplication
{
    public class SqlReplication : BaseReplicationExecuter
    {
        public readonly SqlReplicationConfiguration Configuration;
        public readonly SqlReplicationStatistics Statistics;

        public override string ReplicationUniqueName => "Sql replication of " + Configuration.Name;

        private bool _shouldWaitForChanges;
        private PredefinedSqlConnection _predefinedSqlConnection;
        public readonly SqlReplicationMetricsCountersManager MetricsCountersManager;

        public SqlReplication(DocumentDatabase database, SqlReplicationConfiguration configuration)
            : base(database)
        {
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

        protected override Task ExecuteReplicationOnce()
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

        protected override bool HasMoreDocumentsToSend()
        {
            return _shouldWaitForChanges;
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
                if (_logger.IsInfoEnabled)
                    _logger.Info("Replicated deletes of " + string.Join(", ", documentsKeys)  + " for config " + Configuration.Name);
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
                    IsError = true,
                    CreatedAt = DateTime.UtcNow,
                    Title = "Could not start replication",
                    Message = $"Could not find connection string named '{Configuration.ConnectionStringName}' for sql replication config: {Configuration.Name}, ignoring sql replication setting.",
                };
                return false;
            }

            if (writeToLog)
                if (_logger.IsInfoEnabled)
                    _logger.Info("Connection string name cannot be empty for sql replication config: " + Configuration.ConnectionStringName +", ignoring sql replication setting.");
            Statistics.LastAlert = new Alert
            {
                IsError = true,
                CreatedAt = DateTime.UtcNow,
                Title = "Could not start replication",
                Message = $"Connection string name cannot be empty for sql replication config: {Configuration.Name}, ignoring sql replication setting.",
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