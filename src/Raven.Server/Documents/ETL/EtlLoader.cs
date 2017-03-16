using System;
using System.Collections.Generic;
using Raven.Client.Documents.Changes;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.ETL.Providers.SQL;
using Raven.Server.Documents.ETL.Providers.SQL.Connections;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;

namespace Raven.Server.Documents.ETL
{
    public class EtlLoader
    {
        private EtlProcess[] _processes = new EtlProcess[0];

        private readonly DocumentDatabase _database;

        protected Logger Logger;

        public Action<string, EtlStatistics> BatchCompleted;

        public EtlLoader(DocumentDatabase database)
        {
            _database = database;
            Logger = LoggingSource.Instance.GetLogger(_database.Name, GetType().FullName);

            _database.Changes.OnDocumentChange += NotifyAboutWork;
            // TODO arek - RavennDB-6555 _serverStore.Cluster.DatabaseChanged += HandleDatabaseRecordChange;
            // configuration stored in a system document under Raven/ETL key - temporary
            _database.Changes.OnSystemDocumentChange += HandleSystemDocumentChange;
        }

        public void Initialize()
        {
            LoadProcesses();
        }

        private void LoadProcesses()
        {
            var configuration = LoadConfiguration();

            if (configuration == null)
                return;

            var processes = new List<EtlProcess>();

            foreach (var config in configuration.RavenTargets)
            {
                processes.Add(new RavenEtl(_database, config));
            }

            foreach (var config in configuration.SqlTargets)
            {
                if (string.IsNullOrEmpty(config.ConnectionStringName))
                {
                    var emptyConnectionStringMsg = $"Connection string name cannot be empty for SQL ETL config: {config.Name}, ignoring SQL ETL setting.";

                    if (Logger.IsInfoEnabled)
                        Logger.Info(emptyConnectionStringMsg);

                    var alert = AlertRaised.Create("ETL loader", emptyConnectionStringMsg, AlertType.Etl_Error, NotificationSeverity.Error);

                    _database.NotificationCenter.Add(alert);

                    continue;
                }

                PredefinedSqlConnection predefinedConnection;
                if (configuration.SqlConnections.TryGetValue(config.ConnectionStringName, out predefinedConnection) == false)
                {
                    var message =
                        $"Could not find connection string named '{config.ConnectionStringName}' for SQL ETL config: " +
                        $"{config.Name}, ignoring SQL ETL setting.";

                    if (Logger.IsInfoEnabled)
                        Logger.Info(message);

                    var alert = AlertRaised.Create("ETL loader", message, AlertType.SqlEtl_ConnectionStringMissing, NotificationSeverity.Error);

                    _database.NotificationCenter.Add(alert);

                    continue;
                }

                var sql = new SqlEtl(_database, config, predefinedConnection);

                processes.Add(sql);
            }

            _processes = processes.ToArray();

            // ReSharper disable once ForCanBeConvertedToForeach
            for (var i = 0; i < _processes.Length; i++)
            {
                _processes[i].Start();
            }
        }

        private EtlConfiguration LoadConfiguration()
        {
            DocumentsOperationContext context;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var etlConfigDocument = _database.DocumentsStorage.Get(context, "Raven/ETL");

                if (etlConfigDocument == null)
                    return null;

                return JsonDeserializationServer.EtlConfiguration(etlConfigDocument.Data);
            }
        }

        private void NotifyAboutWork(DocumentChange documentChange)
        {
            // ReSharper disable once ForCanBeConvertedToForeach
            for (var i = 0; i < _processes.Length; i++)
            {
                _processes[i].NotifyAboutWork();
            }
        }

        private void HandleSystemDocumentChange(DocumentChange change)
        {
            if (change.Key.Equals("Raven/ETL", StringComparison.OrdinalIgnoreCase) == false)
                return;
            // TODO arek
            //var configuration = LoadConfiguration();

            foreach (var replication in _processes)
                replication.Dispose();

            _processes = new EtlProcess[0];

            LoadProcesses();
        }

        public virtual void Dispose()
        {
            _database.Changes.OnDocumentChange -= NotifyAboutWork;
            // TODO arek - RavennDB-6555 _serverStore.Cluster.DatabaseChanged += HandleDatabaseRecordChange;
            _database.Changes.OnSystemDocumentChange -= HandleSystemDocumentChange;
        }
    }
}