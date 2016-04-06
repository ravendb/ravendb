using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.SqlReplication
{
    public class SqlReplication : IDisposable
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof (SqlReplication));

        private readonly DocumentDatabase _database;
        private readonly SqlReplicationConfiguration _configuration;
        private readonly SqlReplicationStatistics _statistics;

        public readonly ManualResetEventSlim WaitForChanges = new ManualResetEventSlim();

        private string Name => _configuration.Name;

        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private Thread _sqlReplicationThread;
        private bool disposed;

        public SqlReplication(DocumentDatabase database, SqlReplicationConfiguration configuration)
        {
            _database = database;
            _configuration = configuration;
            _statistics = new SqlReplicationStatistics(configuration.Name);
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
                while (true)
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

        private void DoWork(CancellationToken cancellationToken)
        {
            try
            {
                DocumentsOperationContext databaseContext;
                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out databaseContext))
                using (var tx = databaseContext.OpenWriteTransaction())
                {
                    ReplicateDeletionsToDestination(databaseContext, cancellationToken);
                    ReplicateChangesToDestination(databaseContext, cancellationToken);

                    tx.Commit();
                }
            }
            finally
            {
                var afterReplicationCompleted = _database.SqlReplicationLoader.AfterReplicationCompleted;
                afterReplicationCompleted?.Invoke(_statistics);
            }
        }

        private void ReplicateDeletionsToDestination(DocumentsOperationContext context, CancellationToken cancellationToken)
        {
            var pageSize = _database.Configuration.Indexing.MaxNumberOfTombstonesToFetch;

            var lastTombstoneEtag = _database.DocumentsStorage.GetLastTombstoneEtag(context, _configuration.Collection);
            // TODO: compare to latest etag
            var documents = _database.DocumentsStorage.GetTombstonesAfter(context, _configuration.Collection, lastTombstoneEtag + 1, 0, pageSize).ToList();

            if (documents.Count == 0)
                return;

            var documentsKeys = documents.Select(tombstone => (string) tombstone.Key).ToList();
            using (var writer = new RelationalDatabaseWriter(_database, context, _configuration, _statistics, cancellationToken))
            {
                foreach (var sqlReplicationTable in _configuration.SqlReplicationTables)
                {
                    writer.DeleteItems(sqlReplicationTable.TableName, sqlReplicationTable.DocumentKeyColumn, _configuration.ParameterizeDeletesDisabled, documentsKeys);
                }
                writer.Commit();
                if (Log.IsDebugEnabled)
                    Log.Debug("Replicated deletes of {0} for config {1}", string.Join(", ", documentsKeys), _configuration.Name);
            }
        }

        private bool ReplicateChangesToDestination(DocumentsOperationContext context, CancellationToken cancellationToken)
        {
            var pageSize = _database.Configuration.Indexing.MaxNumberOfDocumentsToFetchForMap;

            var lastDocumentEtag = _database.DocumentsStorage.GetLastDocumentEtag(context, _configuration.Collection);
            // TODO: fix etag
            var documents = _database.DocumentsStorage.GetDocumentsAfter(context, _configuration.Collection, lastDocumentEtag + 1, 0, pageSize).ToList();

            var scriptResult = ApplyConversionScript(documents, context);
            if (scriptResult.Keys.Count == 0)
                return true;

            var countOfReplicatedItems = scriptResult.Data.Sum(x => x.Value.Count);
            try
            {
                using (var writer = new RelationalDatabaseWriter(_database, context, _configuration, _statistics, cancellationToken))
                {
                    if (writer.ExecuteScript(scriptResult))
                    {
                        if (Log.IsDebugEnabled)
                            Log.Debug("Replicated changes of {0} for replication {1}", string.Join(", ", documents.Select(d => d.Key)), _configuration.Name);
                        _statistics.CompleteSuccess(countOfReplicatedItems);
                    }
                    else
                    {
                        if (Log.IsDebugEnabled)
                            Log.Debug("Replicated changes (with some errors) of {0} for replication {1}", string.Join(", ", documents.Select(d => d.Key)), _configuration.Name);
                        _statistics.Success(countOfReplicatedItems);
                    }
                }
                return true;
            }
            catch (Exception e)
            {
                Log.WarnException("Failure to replicate changes to relational database for: " + _configuration.Name, e);
                DateTime newTime;
                if (_statistics.LastErrorTime == null)
                {
                    newTime = SystemTime.UtcNow.AddSeconds(5);
                }
                else
                {
                    // double the fallback time (but don't cross 15 minutes)
                    var totalSeconds = (SystemTime.UtcNow - _statistics.LastErrorTime.Value).TotalSeconds;
                    newTime = SystemTime.UtcNow.AddSeconds(Math.Min(60*15, Math.Max(5, totalSeconds*2)));
                }
                _statistics.RecordWriteError(e, _database, countOfReplicatedItems, newTime);
                return false;
            }
        }

        public SqlReplicationScriptResult ApplyConversionScript(List<Document> documents, DocumentsOperationContext context)
        {
            var result = new SqlReplicationScriptResult();
            foreach (var replicatedDoc in documents)
            {
                _cancellationTokenSource.Token.ThrowIfCancellationRequested();
                var patcher = new SqlReplicationPatchDocument(_database, result, _configuration, replicatedDoc.Key);
                try
                {
                    var scope = patcher.Apply(context, replicatedDoc, new PatchRequest { Script = _configuration.Script });

                    if (Log.IsDebugEnabled && scope.DebugInfo.Count > 0)
                    {
                        Log.Debug("Debug output for doc: {0} for script {1}:\r\n.{2}", replicatedDoc.Key, _configuration.Name, string.Join("\r\n", scope.DebugInfo.Items));
                    }

                    _statistics.ScriptSuccess();
                }
                catch (ParseException e)
                {
                    _statistics.MarkScriptAsInvalid(_database, _configuration.Script);

                    Log.WarnException("Could not parse SQL Replication script for " + _configuration.Name, e);

                    return result;
                }
                catch (Exception diffExceptionName)
                {
                    _statistics.RecordScriptError(_database, diffExceptionName);
                    Log.WarnException("Could not process SQL Replication script for " + _configuration.Name + ", skipping document: " + replicatedDoc.Key, diffExceptionName);
                }
            }
            return result;
        }

        public bool PrepareSqlReplicationConfig(PredefinedSqlConnections connections, bool writeToLog = true)
        {
            if (string.IsNullOrWhiteSpace(_configuration.PredefinedConnectionStringSettingName) == false)
            {
                 var connection = connections.Connections[_configuration.PredefinedConnectionStringSettingName];
                 if (connection != null)
                 {
                     _configuration.ConnectionString = connection.ConnectionString;
                     _configuration.FactoryName = connection.FactoryName;
                     return true;
                 }

                if (writeToLog)
                    Log.Warn("Could not find predefined connection string named '{0}' for sql replication config: {1}, ignoring sql replication setting.",
                        _configuration.PredefinedConnectionStringSettingName,
                        _configuration.Name);
                _statistics.LastAlert = new Alert
                {
                    IsError = true,
                    CreatedAt = DateTime.UtcNow,
                    Title = "Could not start replication",
                    Message = $"Could not find predefined connection string named '{_configuration.PredefinedConnectionStringSettingName}' for sql replication config: {_configuration.Name}, ignoring sql replication setting.",
                };
                return false;
            }

            throw new NotImplementedException();
            if (string.IsNullOrWhiteSpace(_configuration.ConnectionStringName) == false)
            {
                throw new NotImplementedException();
                /*var connectionString = JsonConfigurationManager.ConnectionStrings[_configuration.ConnectionStringName];
                if (connectionString == null)
                {
                if (writeToLog)
                    Log.Warn("Could not find connection string named '{0}' for sql replication config: {1}, ignoring sql replication setting.",
                        _configuration.ConnectionStringName, _configuration.Name);

                    _statistics.LastAlert = new Alert
                    {
                        IsError = true,
                        CreatedAt = DateTime.UtcNow,
                        Title = "Could not start replication",
                        Message = string.Format("Could not find connection string named '{0}' for sql replication config: {1}, ignoring sql replication setting.",
                            _configuration.ConnectionStringName,
                            _configuration.Name)
                    };
                    return false;
                }
                _configuration.ConnectionString = connectionString.ConnectionString;*/
            }
            else if (string.IsNullOrWhiteSpace(_configuration.ConnectionStringSettingName) == false)
            {
                throw new NotImplementedException();

                /*  var setting = _database.Configuration.Settings[_configuration.ConnectionStringSettingName];
                  if (string.IsNullOrWhiteSpace(setting))
                  {
                if (writeToLog)
                          Log.Warn("Could not find setting named '{0}' for sql replication config: {1}, ignoring sql replication setting.",
              _configuration.ConnectionStringSettingName,
              _configuration.Name);
                      _statistics.LastAlert = new Alert
                      {
                  IsError = true,
                          CreatedAt = DateTime.UtcNow,
                          Title = "Could not start replication",
                          Message = string.Format("Could not find setting named '{0}' for sql replication config: {1}, ignoring sql replication setting.",
                              _configuration.ConnectionStringSettingName,
                              _configuration.Name)
                      };
                      return false;
                  }
              }
              return true;*/
            }
        }

        public void Dispose()
        {
            _cancellationTokenSource.Cancel();
            disposed = true;
        }

        public bool ValidateName()
        {
            if (string.IsNullOrWhiteSpace(_configuration.Name) == false)
                return true;

            Log.Warn($"Could not find name for sql replication document {_configuration.Name}, ignoring");
            _statistics.LastAlert = new Alert
            {
                IsError = true,
                CreatedAt = DateTime.UtcNow,
                Title = "Could not start replication",
                Message = $"Could not find name for sql replication document {_configuration.Name}, ignoring"
            };
            return false;
        }
    }
}