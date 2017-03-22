using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
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
        private const string AlertTitle = "ETL loader";

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

        public EtlProcess[] Processes => _processes;

        public EtlConfiguration CurrentConfiguration { get; private set; }

        public void Initialize()
        {
            LoadProcesses();
        }

        private void LoadProcesses()
        {
            LoadConfiguration();

            if (CurrentConfiguration == null)
                return;

            var processes = new List<EtlProcess>();
            var uniqueNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var config in CurrentConfiguration.RavenTargets)
            {
                if (ValidateConfiguration(config, uniqueNames) == false)
                    continue;

                var etlProcess = new RavenEtl(_database, config);

                processes.Add(etlProcess);
            }

            foreach (var config in CurrentConfiguration.SqlTargets)
            {
                if (ValidateConfiguration(config, uniqueNames) == false)
                    continue;

                PredefinedSqlConnection predefinedConnection;
                if (CurrentConfiguration.SqlConnections.TryGetValue(config.ConnectionStringName, out predefinedConnection) == false)
                {
                    var message =
                        $"Could not find connection string named '{config.ConnectionStringName}' for SQL ETL config: " +
                        $"{config.Name}, ignoring SQL ETL setting.";

                    if (Logger.IsInfoEnabled)
                        Logger.Info(message);

                    var alert = AlertRaised.Create(AlertTitle, message, AlertType.SqlEtl_ConnectionStringMissing, NotificationSeverity.Error);

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

        private bool ValidateConfiguration(EtlProcessConfiguration config, HashSet<string> uniqueNames)
        {
            List<string> errors;
            if (config.Validate(out errors) == false)
            {
                LogConfigurationError(config, errors);
                return false;
            }

            if (string.IsNullOrEmpty(config.Name) == false && uniqueNames.Add(config.Name) == false)
            {
                LogConfigurationError(config, new List<string> { $"'{config.Name}' name is already defined for different ETL process" });
                return false;
            }

            return true;
        }

        private void LogConfigurationError(EtlProcessConfiguration config, List<string> errors)
        {
            var errorMessage = $"Invalid ETL configuration for: '{config.Name}'. " +
                               $"Reason{(errors.Count > 1 ? "s" : string.Empty)}: {string.Join(";", errors)}.";

            if (Logger.IsInfoEnabled)
                Logger.Info(errorMessage);

            var alert = AlertRaised.Create(AlertTitle, errorMessage, AlertType.Etl_Error, NotificationSeverity.Error);

            _database.NotificationCenter.Add(alert);
        }

        private void LoadConfiguration()
        {
            DocumentsOperationContext context;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var etlConfigDocument = _database.DocumentsStorage.Get(context, Constants.Documents.ETL.RavenEtlDocument);

                if (etlConfigDocument == null)
                {
                    CurrentConfiguration = null;
                    return;
                }

                CurrentConfiguration = JsonDeserializationServer.EtlConfiguration(etlConfigDocument.Data);
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
            if (change.Key.Equals(Constants.Documents.ETL.RavenEtlDocument, StringComparison.OrdinalIgnoreCase) == false)
                return;

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