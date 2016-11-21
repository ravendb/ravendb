using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Server.Alerts;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.SqlReplication
{
    public class SqlReplicationLoader : IDisposable
    {
        private const int MaxSupportedSqlReplication = int.MaxValue; 

        private BlittableJsonReaderObject _connections;

        public Action<SqlReplicationStatistics> AfterReplicationCompleted;
        protected Logger _logger;
        protected DocumentDatabase _database;
        public readonly ConcurrentSet<SqlReplication> Replications = new ConcurrentSet<SqlReplication>();

        public SqlReplicationLoader(DocumentDatabase database)
        {
            _database = database;
            _logger = LoggingSource.Instance.GetLogger(_database.Name, GetType().FullName);
            _database.Notifications.OnDocumentChange += WakeReplication;
            _database.Notifications.OnSystemDocumentChange += HandleSystemDocumentChange;

        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected bool ShouldReloadConfiguration(string systemDocumentKey)
        {
            return
                systemDocumentKey.StartsWith(Constants.SqlReplication.SqlReplicationConfigurationPrefix,
                    StringComparison.OrdinalIgnoreCase) ||
                systemDocumentKey.Equals(Constants.SqlReplication.SqlReplicationConnections, StringComparison.OrdinalIgnoreCase);
        }

        protected void LoadConfigurations()
        {
            DocumentsOperationContext context;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var sqlReplicationConnections = _database.DocumentsStorage.Get(context, Constants.SqlReplication.SqlReplicationConnections);
                if (sqlReplicationConnections != null)
                {
                    object connections;
                    if (sqlReplicationConnections.Data.TryGetMember("Connections", out connections))
                    {
                        _connections = connections as BlittableJsonReaderObject;
                    }
                }

                var documents = _database.DocumentsStorage.GetDocumentsStartingWith(context, Constants.SqlReplication.SqlReplicationConfigurationPrefix, null, null, 0, MaxSupportedSqlReplication);
                foreach (var document in documents)
                {
                    var configuration = JsonDeserializationServer.SqlReplicationConfiguration(document.Data);
                    var sqlReplication = new SqlReplication(_database, configuration);
                    Replications.Add(sqlReplication);
                    if (sqlReplication.ValidateName() == false ||
                        sqlReplication.PrepareSqlReplicationConfig(_connections) == false)
                        return;
                    sqlReplication.Start();
                }
            }
        }

        public DynamicJsonValue SimulateSqlReplicationSqlQueries(SimulateSqlReplication simulateSqlReplication, DocumentsOperationContext context)
        {
            try
            {
                var document = _database.DocumentsStorage.Get(context, simulateSqlReplication.DocumentId);
                var sqlReplication = new SqlReplication(_database, simulateSqlReplication.Configuration);

                var result = sqlReplication.ApplyConversionScript(new List<Document> { document }, context);

                if (sqlReplication.PrepareSqlReplicationConfig(_connections, false) == false)
                {
                    return new DynamicJsonValue
                    {
                        ["LastAlert"] = sqlReplication.Statistics.LastAlert,
                    };
                }

                return sqlReplication.Simulate(simulateSqlReplication, context, result);
            }
            catch (Exception e)
            {
                return new DynamicJsonValue
                {
                    ["LastAlert"] = new Alert
                    {
                        Type = AlertType.SqlReplicationError, 
                        Severity = AlertSeverity.Error,
                        CreatedAt = SystemTime.UtcNow,
                        Key = simulateSqlReplication.Configuration.Name,
                        Message = "SQL replication error",
                        Content = new ExceptionAlertContent
                        {
                            Message = "Last SQL replication operation for " + simulateSqlReplication.Configuration.Name + " was failed",
                            Exception = e.ToString()
                        }
                    }
                };
            }
        }

        private void WakeReplication(DocumentChangeNotification documentChangeNotification)
        {
            foreach (var replication in Replications)
                replication.WaitForChanges.SetByAsyncCompletion();
        }

        private void HandleSystemDocumentChange(DocumentChangeNotification notification)
        {
            if (ShouldReloadConfiguration(notification.Key))
            {
                foreach (var replication in Replications)
                    replication.Dispose();

                Replications.Clear();
                LoadConfigurations();

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Replication configuration was changed: {notification.Key}");
            }
        }

        public void Initialize()
        {
            LoadConfigurations();
        }

        public virtual void Dispose()
        {
            _database.Notifications.OnDocumentChange -= WakeReplication;
            _database.Notifications.OnSystemDocumentChange -= HandleSystemDocumentChange;
        }
    }
}