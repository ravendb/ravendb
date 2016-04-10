using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.SqlReplication
{
    public class SqlReplicationLoader : IDisposable
    {
        private readonly ILog Log = LogManager.GetLogger(typeof(SqlReplicationLoader));

        private readonly DocumentDatabase _database;
        private const int MaxSupportedSqlReplication = int.MaxValue; // TODO: Maybe this should be 128 or 1024

        public readonly List<SqlReplication> Replications = new List<SqlReplication>();
        private SqlConnections _connections;

        public Action<SqlReplicationStatistics> AfterReplicationCompleted;

        public SqlReplicationLoader(DocumentDatabase database)
        {
            _database = database;
            _database.Notifications.OnSystemDocumentChange += HandleSystemDocumentChange;
            _database.Notifications.OnDocumentChange += WakeSqlReplication;
        }

        private void WakeSqlReplication(DocumentChangeNotification documentChangeNotification)
        {
            foreach (var replication in Replications)
            {
                replication.WaitForChanges.Set();
            }
        }

        private void HandleSystemDocumentChange(DocumentChangeNotification notification)
        {
            if (notification.Key.StartsWith(Constants.SqlReplication.SqlReplicationConfigurationPrefix, StringComparison.OrdinalIgnoreCase) ||
                notification.Key.Equals(Constants.SqlReplication.SqlReplicationConnections, StringComparison.OrdinalIgnoreCase))
            {
                _connections = null;
                foreach (var replication in Replications)
                {
                    replication.Dispose();
                }
                Replications.Clear();

                LoadConfigurations();

                if (Log.IsDebugEnabled)
                    Log.Debug(() => $"Sql Replication configuration was changed: {notification.Key}");
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
                if (sqlReplicationConnections == null)
                    return;
                _connections = JsonDeserialization.PredefinedSqlConnections(sqlReplicationConnections.Data);

                var documents = _database.DocumentsStorage.GetDocumentsStartingWith(context, Constants.SqlReplication.SqlReplicationConfigurationPrefix, null, null, 0, MaxSupportedSqlReplication);
                foreach (var document in documents)
                {
                    var configuration = JsonDeserialization.SqlReplicationConfiguration(document.Data);
                    var sqlReplication = new SqlReplication(_database, configuration);
                    Replications.Add(sqlReplication);
                    if (sqlReplication.ValidateName() == false ||
                        sqlReplication.PrepareSqlReplicationConfig(_connections) == false)
                        return;
                    sqlReplication.Start();
                }
            }
        }

        public void Dispose()
        {
            _database.Notifications.OnDocumentChange -= WakeSqlReplication;
            _database.Notifications.OnSystemDocumentChange -= HandleSystemDocumentChange;
        }

        public DynamicJsonValue SimulateSqlReplicationSqlQueries(SimulateSqlReplication simulateSqlReplication, DocumentsOperationContext context)
        {
            try
            {
                var stats = new SqlReplicationStatistics(simulateSqlReplication.Configuration.Name, false);
                var document = _database.DocumentsStorage.Get(context, simulateSqlReplication.DocumentId);
                var sqlReplication = new SqlReplication(_database, simulateSqlReplication.Configuration);

                var result = sqlReplication.ApplyConversionScript(new List<Document> {document}, context);

                if (sqlReplication.PrepareSqlReplicationConfig(_connections, false) == false)
                {
                    return new DynamicJsonValue
                    {
                        ["LastAlert"] = stats.LastAlert,
                    };
                }

                if (simulateSqlReplication.PerformRolledBackTransaction)
                {
                    using (var writer = new RelationalDatabaseWriter(_database, context, simulateSqlReplication.Configuration, 
                        sqlReplication.PredefinedSqlConnection, stats, _database.DatabaseShutdown))
                    {
                        return new DynamicJsonValue
                        {
                            ["Results"] = new DynamicJsonArray(writer.RolledBackExecute(result).ToArray()),
                            ["LastAlert"] = stats.LastAlert,
                        };
                    }
                }

                var simulatedwriter = new RelationalDatabaseWriterSimulator(_database, simulateSqlReplication.Configuration, sqlReplication.PredefinedSqlConnection, stats, _database.DatabaseShutdown);
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