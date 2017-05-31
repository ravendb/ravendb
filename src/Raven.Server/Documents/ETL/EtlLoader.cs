using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Extensions;
using Raven.Client.Server;
using Raven.Client.Server.ETL;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.ETL.Providers.SQL;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Logging;

namespace Raven.Server.Documents.ETL
{
    public class EtlLoader
    {
        private const string AlertTitle = "ETL loader";

        private EtlProcess[] _processes = new EtlProcess[0];
        private DatabaseRecord _databaseRecord;

        private readonly object _loadProcessedLock = new object();
        private readonly DocumentDatabase _database;
        private readonly ServerStore _serverStore;

        protected Logger Logger;

        public Action<string, EtlProcessStatistics> BatchCompleted;

        public EtlLoader(DocumentDatabase database, ServerStore serverStore)
        {
            Logger = LoggingSource.Instance.GetLogger(database.Name, GetType().FullName);

            _database = database;
            _serverStore = serverStore;
            _database.Changes.OnDocumentChange += NotifyAboutWork;
        }

        public EtlProcess[] Processes => _processes;

        public List<EtlConfiguration<RavenDestination>> RavenDestinations;

        public List<EtlConfiguration<SqlDestination>> SqlDestinations;

        public void Initialize()
        {
            LoadProcesses();
        }
        
        private void LoadProcesses()
        {
            lock (_loadProcessedLock)
            {
                LoadConfiguration();

                var processes = new List<EtlProcess>();

                var uniqueDestinations = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                if (RavenDestinations != null)
                {
                    foreach (var config in RavenDestinations)
                    {
                        if (ValidateConfiguration(config, uniqueDestinations) == false)
                            continue;

                        if (_databaseRecord.Topology.WhoseTaskIsIt(config) != _serverStore.NodeTag)
                            continue;

                        foreach (var transform in config.Transforms)
                        {
                            var etlProcess = new RavenEtl(transform, config.Destination, _database);

                            processes.Add(etlProcess);
                        }
                    }
                }

                if (SqlDestinations != null)
                {
                    foreach (var config in SqlDestinations)
                    {
                        if (ValidateConfiguration(config, uniqueDestinations) == false)
                            continue;

                        if (_databaseRecord.Topology.WhoseTaskIsIt(config) != _serverStore.NodeTag)
                            continue;

                        foreach (var transform in config.Transforms)
                        {
                            var sql = new SqlEtl(transform, config.Destination, _database);

                            processes.Add(sql);
                        }
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
        }

        private bool ValidateConfiguration<T>(EtlConfiguration<T> config, HashSet<string> uniqueDestinations) where T : EtlDestination
        {
            List<string> errors;
            if (config.Validate(out errors) == false)
            {
                LogConfigurationError(config, errors);
                return false;
            }

            if (uniqueDestinations.Add(config.Destination.Name) == false)
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
            var errorMessage = $"Invalid ETL configuration for destination: {config.Destination.Name}. " +
                               $"Reason{(errors.Count > 1 ? "s" : string.Empty)}: {string.Join(";", errors)}.";

            if (Logger.IsInfoEnabled)
                Logger.Info(errorMessage);

            var alert = AlertRaised.Create(AlertTitle, errorMessage, AlertType.Etl_Error, NotificationSeverity.Error);

            _database.NotificationCenter.Add(alert);
        }

        private void LoadConfiguration()
        {
            _databaseRecord = _serverStore.LoadDatabaseRecord(_database.Name);

            RavenDestinations = _databaseRecord.RavenEtls;
            SqlDestinations = _databaseRecord.SqlEtls;

            // TODO arek remove destinations which has been removed or modified - EtlStorage.Remove
        }

        private void NotifyAboutWork(DocumentChange documentChange)
        {
            // ReSharper disable once ForCanBeConvertedToForeach
            for (var i = 0; i < _processes.Length; i++)
            {
                _processes[i].NotifyAboutWork(documentChange);
            }
        }

        public virtual void Dispose()
        {
            _database.Changes.OnDocumentChange -= NotifyAboutWork;

            var ea = new ExceptionAggregator(Logger, "Could not dispose ETL loader");
            
            Parallel.ForEach(_processes, x => ea.Execute(x.Dispose));
            
            ea.ThrowIfNeeded();
        }

        public void HandleDatabaseRecordChange()
        {
            var old = _processes;

            Parallel.ForEach(old, x =>
            {
                try
                {
                    x.Dispose();
                }
                catch (Exception e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Failed to dispose ETL process {x.Name} on the database record change", e);
                }
            });

            _processes = new EtlProcess[0];

            LoadProcesses();

            // unsubscribe old etls _after_ we start new processes to ensure the tombstone cleaner 
            // constantly keeps track of tombstones processed by ETLs so it won't delete them during etl processes reloading

            old.ForEach(x => _database.DocumentTombstoneCleaner.Unsubscribe(x));
        }
    }
}