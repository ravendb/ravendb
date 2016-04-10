using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.Documents.Patch;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
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

        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private Thread _sqlReplicationThread;
        private bool _disposed;
        public PredefinedSqlConnection PredefinedSqlConnection { get; private set; }

        public SqlReplication(DocumentDatabase database, SqlReplicationConfiguration configuration)
        {
            _database = database;
            Configuration = configuration;
            Statistics = new SqlReplicationStatistics(configuration.Name);
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
            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown, _cancellationTokenSource.Token))
            {
                LoadLastEtag();
                while (cts.IsCancellationRequested == false)
                {
                    if (Log.IsDebugEnabled)
                        Log.Debug($"Starting sql replication for '{Name}'.");

                    WaitForChanges.Reset();

                    var startTime = SystemTime.UtcNow;
                    try
                    {
                        cts.Token.ThrowIfCancellationRequested();

                        DoWork(cts.Token);

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

                    /* try
                        {
                            UpdateStats(startTime, stats);
                        }
                        catch (Exception e)
                        {
                            Log.ErrorException($"Could not update stats for '{Name} ({IndexId})'.", e);
                        }*/

                    try
                    {
                        WaitForChanges.Wait(cts.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                }
            }
        }

        private void LoadLastEtag()
        {
            DocumentsOperationContext context;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var sqlReplicationStatus = _database.DocumentsStorage.Get(context, Constants.SqlReplication.RavenSqlReplicationStatusPrefix + Name);
                Statistics.LastReplicatedEtag = sqlReplicationStatus == null ? 0 : JsonDeserialization.SqlReplicationStatus(sqlReplicationStatus.Data).LastReplicatedEtag;
            }
        }

        private void WriteLastEtag(DocumentsOperationContext context)
        {
            var key = Constants.SqlReplication.RavenSqlReplicationStatusPrefix + Name;
            var document = context.ReadObject(new DynamicJsonValue
            {
                ["Name"] = Name,
                ["LastReplicatedEtag"] = Statistics.LastReplicatedEtag,
            }, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            _database.DocumentsStorage.Put(context, key, null, document);
        }

        private void DoWork(CancellationToken cancellationToken)
        {
            try
            {
                DocumentsOperationContext databaseContext;
                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out databaseContext))
                using (var tx = databaseContext.OpenReadTransaction())
                {
                    // TODO: We should not have here a write transaction

                    // ReplicateDeletionsToDestination(databaseContext, cancellationToken);
                    ReplicateChangesToDestination(databaseContext, cancellationToken);

                    tx.Commit();
                }

                //TODO: only do this on success or if we actually replicated

                using (var tx = databaseContext.OpenWriteTransaction())
                {
                    WriteLastEtag(databaseContext);
                    tx.Commit();
                }
            }
            finally
            {
                var afterReplicationCompleted = _database.SqlReplicationLoader.AfterReplicationCompleted;
                afterReplicationCompleted?.Invoke(Statistics);
            }
        }

        private void ReplicateDeletionsToDestination(DocumentsOperationContext context, CancellationToken cancellationToken)
        {
            var pageSize = _database.Configuration.Indexing.MaxNumberOfTombstonesToFetch;

            var lastTombstoneEtag = _database.DocumentsStorage.GetLastTombstoneEtag(context, Configuration.Collection);
            // TODO: compare to latest etag
            var documents = _database.DocumentsStorage.GetTombstonesAfter(context, Configuration.Collection, lastTombstoneEtag + 1, 0, pageSize).ToList();

            if (documents.Count == 0)
                return;

            var documentsKeys = documents.Select(tombstone => (string)tombstone.Key).ToList();
            using (var writer = new RelationalDatabaseWriter(_database, context, Configuration, PredefinedSqlConnection, Statistics, cancellationToken))
            {
                foreach (var sqlReplicationTable in Configuration.SqlReplicationTables)
                {
                    writer.DeleteItems(sqlReplicationTable.TableName, sqlReplicationTable.DocumentKeyColumn, Configuration.ParameterizeDeletesDisabled, documentsKeys);
                }
                writer.Commit();
                if (Log.IsDebugEnabled)
                    Log.Debug("Replicated deletes of {0} for config {1}", string.Join(", ", documentsKeys), Configuration.Name);
            }
        }

        private bool ReplicateChangesToDestination(DocumentsOperationContext context, CancellationToken cancellationToken)
        {
            var pageSize = _database.Configuration.Indexing.MaxNumberOfDocumentsToFetchForMap;

            var documents = _database.DocumentsStorage.GetDocumentsAfter(context, Configuration.Collection, Statistics.LastReplicatedEtag + 1, 0, pageSize).ToList();
            if (documents.Count == 0)
                return true;

            Statistics.LastReplicatedEtag = documents.Last().Etag;

            var scriptResult = ApplyConversionScript(documents, context);
            if (scriptResult.Keys.Count == 0)
                return true;

            var countOfReplicatedItems = scriptResult.Data.Sum(x => x.Value.Count);
            try
            {
                using (var writer = new RelationalDatabaseWriter(_database, context, Configuration, PredefinedSqlConnection, Statistics, cancellationToken))
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

        public bool PrepareSqlReplicationConfig(SqlConnections connections, bool writeToLog = true)
        {
            if (string.IsNullOrWhiteSpace(Configuration.ConnectionStringName) == false)
            {
                PredefinedSqlConnection = connections.Connections[Configuration.ConnectionStringName];
                if (PredefinedSqlConnection != null)
                    return true;

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
    }
}