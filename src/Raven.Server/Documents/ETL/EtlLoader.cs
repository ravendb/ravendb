using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Extensions;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.ETL.Providers.SQL;
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

        public Action<string, EtlProcessStatistics> BatchCompleted;

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

        public EtlDestinationsConfig Destinations { get; private set; }

        public void Initialize()
        {
            LoadProcesses();
        }

        private void LoadProcesses()
        {
            LoadConfiguration();

            if (Destinations == null)
                return;

            var processes = new List<EtlProcess>();

            var uniqueDestinations = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var config in Destinations.RavenDestinations)
            {
                if (ValidateConfiguration(config, uniqueDestinations) == false)
                    continue;

                foreach (var transform in config.Transforms)
                {
                    var etlProcess = new RavenEtl(transform, config.Destination, _database);

                    processes.Add(etlProcess);
                }
            }

            foreach (var config in Destinations.SqlDestinations)
            {
                if (ValidateConfiguration(config, uniqueDestinations) == false)
                    continue;

                foreach (var transform in config.Transforms)
                {
                    var sql = new SqlEtl(transform, config.Destination, _database);

                    processes.Add(sql);
                }
            }

            _processes = processes.ToArray();

            // ReSharper disable once ForCanBeConvertedToForeach
            for (var i = 0; i < _processes.Length; i++)
            {
                _database.DocumentTombstoneCleaner.Subscribe(_processes[i]);
                _processes[i].Start();
            }
        }

        private bool ValidateConfiguration<T>(EtlConfiguration<T> config, HashSet<string> uniqueDestinations) where T : EtlDestination
        {
            List<string> errors;
            if (config.Validate(out errors) == false)
            {
                LogConfigurationError(config, errors);
                return false;
            }

            if (uniqueDestinations.Add(config.Destination.UniqueName) == false)
            {
                LogConfigurationError(config,
                    new List<string>
                    {
                        "ETL to this destination is already defined. Please just combine transformation scripts for the same destination"
                    });
                return false;
            }

            return true;
        }

        private void LogConfigurationError<T>(EtlConfiguration<T> config, List<string> errors) where T : EtlDestination
        {
            var errorMessage = $"Invalid ETL configuration for destination: {config.Destination}. " +
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
                    Destinations = null;
                    return;
                }

                Destinations = JsonDeserializationServer.EtlConfiguration(etlConfigDocument.Data);
            }
        }

        private void NotifyAboutWork(DocumentChange documentChange)
        {
            // ReSharper disable once ForCanBeConvertedToForeach
            for (var i = 0; i < _processes.Length; i++)
            {
                _processes[i].NotifyAboutWork(documentChange);
            }
        }

        private void HandleSystemDocumentChange(DocumentChange change)
        {
            if (change.Id.Equals(Constants.Documents.ETL.RavenEtlDocument, StringComparison.OrdinalIgnoreCase) == false)
                return;

            var old = _processes;

            Parallel.ForEach(old, x => x.Dispose());

            _processes = new EtlProcess[0];

            LoadProcesses();

            // unsubscribe old etls _after_ we start new processes to ensure the tombsone cleaner 
            // constantly keeps track of tombstones processed by ETLs so it won't delete them during etl processes reloading

            old.ForEach(x => _database.DocumentTombstoneCleaner.Unsubscribe(x)); 
        }

        public virtual void Dispose()
        {
            _database.Changes.OnDocumentChange -= NotifyAboutWork;
            // TODO arek - RavennDB-6555 _serverStore.Cluster.DatabaseChanged += HandleDatabaseRecordChange;
            _database.Changes.OnSystemDocumentChange -= HandleSystemDocumentChange;

            Parallel.ForEach(_processes, x => x.Dispose());
        }
    }
}