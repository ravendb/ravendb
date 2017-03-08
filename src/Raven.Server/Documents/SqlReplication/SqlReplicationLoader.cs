using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
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
        protected Logger Logger;
        protected DocumentDatabase Database;
        private SqlReplication[] _replications = new SqlReplication[0];

        public IEnumerable<SqlReplication> Replications => _replications;

        public SqlReplicationLoader(DocumentDatabase database)
        {
            Database = database;
            Logger = LoggingSource.Instance.GetLogger(Database.Name, GetType().FullName);
            Database.Changes.OnDocumentChange += WakeReplication;
            Database.Changes.OnSystemDocumentChange += HandleSystemDocumentChange;

        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected bool ShouldReloadConfiguration(string systemDocumentKey)
        {
            return
                systemDocumentKey.StartsWith(Constants.Documents.SqlReplication.SqlReplicationConfigurationPrefix,
                    StringComparison.OrdinalIgnoreCase) ||
                systemDocumentKey.Equals(Constants.Documents.SqlReplication.SqlReplicationConnections, StringComparison.OrdinalIgnoreCase);
        }

        protected void LoadConfigurations()
        {
            DocumentsOperationContext context;
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var sqlReplicationConnections = Database.DocumentsStorage.Get(context, Constants.Documents.SqlReplication.SqlReplicationConnections);
                if (sqlReplicationConnections != null)
                {
                    object connections;
                    if (sqlReplicationConnections.Data.TryGetMember("Connections", out connections))
                    {
                        _connections = connections as BlittableJsonReaderObject;
                    }
                }
                var replications = new List<SqlReplication>();
                var documents = Database.DocumentsStorage.GetDocumentsStartingWith(context, Constants.Documents.SqlReplication.SqlReplicationConfigurationPrefix, null, null, null, 0, MaxSupportedSqlReplication);
                foreach (var document in documents)
                {
                    var configuration = JsonDeserializationServer.SqlReplicationConfiguration(document.Data);
                    var sqlReplication = new SqlReplication(Database, configuration);
                    replications.Add(sqlReplication);
                    if (sqlReplication.ValidateName() == false ||
                        sqlReplication.PrepareSqlReplicationConfig(_connections) == false)
                        return;
                    sqlReplication.Start();
                }
                _replications = replications.ToArray();



            }
        }

        public DynamicJsonValue SimulateSqlReplicationSqlQueries(SimulateSqlReplication simulateSqlReplication, DocumentsOperationContext context)
        {
            try
            {
                var document = Database.DocumentsStorage.Get(context, simulateSqlReplication.DocumentId);
                var sqlReplication = new SqlReplication(Database, simulateSqlReplication.Configuration);

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
                    ["LastAlert"] =
                    AlertRaised.Create(SqlReplication.AlertTitle,
                        $"Last SQL replication operation for {simulateSqlReplication.Configuration.Name} was failed",
                        AlertType.SqlReplication_Error,
                        NotificationSeverity.Error,
                        key: simulateSqlReplication.Configuration.Name,
                        details: new ExceptionDetails(e)).ToJson()
                };
            }
        }

        private void WakeReplication(DocumentChange documentChange)
        {
            // ReSharper disable once ForCanBeConvertedToForeach
            for (var i = 0; i < _replications.Length; i++)
            {
                _replications[i].WaitForChanges.Set();
            }
        }

        private void HandleSystemDocumentChange(DocumentChange change)
        {
            if (ShouldReloadConfiguration(change.Key))
            {
                foreach (var replication in _replications)
                    replication.Dispose();

                _replications = new SqlReplication[0];

                LoadConfigurations();

                if (Logger.IsInfoEnabled)
                    Logger.Info($"Replication configuration was changed: {change.Key}");
            }
        }

        public void Initialize()
        {
            LoadConfigurations();
        }

        public virtual void Dispose()
        {
            Database.Changes.OnDocumentChange -= WakeReplication;
            Database.Changes.OnSystemDocumentChange -= HandleSystemDocumentChange;
        }
    }
}