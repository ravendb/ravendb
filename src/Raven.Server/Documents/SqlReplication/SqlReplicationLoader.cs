using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.SqlReplication
{
    public class SqlReplicationLoader : IDisposable
    {
        private readonly ILog Log = LogManager.GetLogger(typeof(SqlReplicationLoader));

        private readonly DocumentDatabase _database;
        private const int MaxSupportedSqlReplication = int.MaxValue; // TODO: Maybe this should be 128 or 1024

        private readonly ConcurrentDictionary<string, SqlReplication> _replications = new ConcurrentDictionary<string, SqlReplication>(StringComparer.OrdinalIgnoreCase);
        private PredefinedSqlConnections connections;

        public Action<SqlReplicationStatistics> AfterReplicationCompleted;

        public SqlReplicationLoader(DocumentDatabase database)
        {
            _database = database;
            _database.Notifications.OnDocumentChange += HandleDocumentChange;
        }

        private void HandleDocumentChange(DocumentChangeNotification notification)
        {
            try
            {
                if (notification.Key.StartsWith(Constants.SqlReplication.SqlReplicationConfigurationPrefix, StringComparison.OrdinalIgnoreCase))
                {
                    if (Log.IsDebugEnabled)
                        Log.Debug($"Sql Replication configuration was changed: {notification.Key}");

                    if (notification.Type == DocumentChangeTypes.Delete)
                    {
                        SqlReplication sqlReplication;
                        if (_replications.TryRemove(notification.Key, out sqlReplication))
                        {
                            sqlReplication.Dispose();
                        }
                    }
                    else if (notification.Type == DocumentChangeTypes.Put)
                    {
                        DocumentsOperationContext context;
                        using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                        {
                            context.OpenReadTransaction();

                            var configuration = _database.DocumentsStorage.Get(context, notification.Key);
                            if (configuration == null) // Should not happen, but can
                            {
                                SqlReplication sqlReplication;
                                if (_replications.TryRemove(notification.Key, out sqlReplication))
                                {
                                    sqlReplication.Dispose();
                                }
                            }
                            else
                            {
                                var newSqlReplication = new SqlReplication(_database, JsonDeserialization.SqlReplicationConfiguration(configuration.Data));
                                _replications.AddOrUpdate(notification.Key, s => newSqlReplication,
                                    (s, replication) =>
                                    {
                                        replication.Dispose();
                                        return newSqlReplication;
                                    });
                                if (newSqlReplication.ValidateName() == false ||
                                    newSqlReplication.PrepareSqlReplicationConfig(connections) == false)
                                    return;
                                newSqlReplication.Start();
                            }
                        }
                    }
                    else
                    {
                        Log.Warn($"Got notification for {notification.Key} of type '{notification.Type}' which is not supported");
                    }
                }
                else if (notification.Key.Equals(Constants.SqlReplication.SqlReplicationConnections, StringComparison.OrdinalIgnoreCase))
                {
                    if (Log.IsDebugEnabled)
                        Log.Debug("Sql replication connections was changed.");

                    if (notification.Type == DocumentChangeTypes.Delete)
                    {
                        connections = null;
                    }
                    else if (notification.Type == DocumentChangeTypes.Put)
                    {
                        DocumentsOperationContext context;
                        using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                        {
                            context.OpenReadTransaction();

                            var sqlReplicationConnections = _database.DocumentsStorage.Get(context, Constants.SqlReplication.SqlReplicationConnections);
                            if (sqlReplicationConnections != null)
                            {
                                connections = JsonDeserialization.PredefinedSqlConnections(sqlReplicationConnections.Data);
                            }
                        }
                    }
                    else
                    {
                        Log.Warn($"Got notification for {notification.Key} of type '{notification.Type}' which is not supported");
                    }
                }
            }
            finally
            {
                foreach (var replication in _replications)
                {
                    replication.Value.WaitForChanges.Set();
                }
            }
        }


        public void Initialize()
        {
            LoadConfigurations();
        }

        private void LoadConfigurations()
        {
            DocumentsOperationContext context;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var sqlReplicationConnections = _database.DocumentsStorage.Get(context, Constants.SqlReplication.SqlReplicationConnections);
                if (sqlReplicationConnections != null)
                {
                    connections = JsonDeserialization.PredefinedSqlConnections(sqlReplicationConnections.Data);
                }

                var documents = _database.DocumentsStorage.GetDocumentsStartingWith(context, Constants.SqlReplication.SqlReplicationConfigurationPrefix, null, null, 0, MaxSupportedSqlReplication);
                foreach (var document in documents)
                {
                    var configuration = JsonDeserialization.SqlReplicationConfiguration(document.Data);
                    var sqlReplication = new SqlReplication(_database, configuration);
                    _replications.TryAdd(document.Key, sqlReplication);
                    if (sqlReplication.ValidateName() == false ||
                        sqlReplication.PrepareSqlReplicationConfig(connections) == false)
                        return;
                    sqlReplication.Start();
                }
            }
        }

        public void Dispose()
        {
            _database.Notifications.OnDocumentChange -= HandleDocumentChange;
        }


        public DynamicJsonValue SimulateSqlReplicationSqlQueries(SimulateSqlReplication simulateSqlReplication, DocumentsOperationContext context)
        {
            try
            {
                var stats = new SqlReplicationStatistics(simulateSqlReplication.Configuration.Name, false);
                var document = _database.DocumentsStorage.Get(context, simulateSqlReplication.DocumentId);
                var sqlReplication = new SqlReplication(_database, simulateSqlReplication.Configuration);

                var result = sqlReplication.ApplyConversionScript(new List<Document> {document}, context);

                if (sqlReplication.PrepareSqlReplicationConfig(connections, false) == false)
                {
                    return new DynamicJsonValue
                    {
                        ["LastAlert"] = stats.LastAlert,
                    };
                }

                if (simulateSqlReplication.PerformRolledBackTransaction)
                {
                    using (var writer = new RelationalDatabaseWriter(_database, context, simulateSqlReplication.Configuration, stats, _database.DatabaseShutdown))
                    {
                        return new DynamicJsonValue
                        {
                            ["Results"] = new DynamicJsonArray(writer.RolledBackExecute(result).ToArray()),
                            ["LastAlert"] = stats.LastAlert,
                        };
                    }
                }

                var simulatedwriter = new RelationalDatabaseWriterSimulator(_database, simulateSqlReplication.Configuration, stats);
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
                    ["LastAlert"] = stats.LastAlert,
                };
            }
            catch (Exception e)
            {
                return new DynamicJsonValue
                {
                    ["LastAlert"] = new Alert
                    {
                        IsError = true,
                        CreatedAt = SystemTime.UtcNow,
                        Message = "Last SQL replication operation for " + simulateSqlReplication.Configuration.Name + " was failed",
                        Title = "SQL replication error",
                        Exception = e.ToString(),
                        UniqueKey = "Sql Replication Error: " + simulateSqlReplication.Configuration.Name
                    },
                };
            }
        }
    }
}