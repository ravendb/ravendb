﻿using System;
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
        
        public List<RavenEtlConfiguration> RavenDestinations;

        public List<SqlEtlConfiguration> SqlDestinations;

        public void Initialize(DatabaseRecord record)
        {
            LoadProcesses(record);
        }
        
        private void LoadProcesses(DatabaseRecord record)
        {
            lock (_loadProcessedLock)
            {
                LoadConfiguration(record);

                RavenDestinations = _databaseRecord.RavenEtls;
                SqlDestinations = _databaseRecord.SqlEtls;

                var processes = new List<EtlProcess>();

                var uniqueNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                if (RavenDestinations != null)
                    processes.AddRange(GetRelevantProcesses<RavenEtlConfiguration, RavenConnectionString>(RavenDestinations, uniqueNames));

                if (SqlDestinations != null)
                    processes.AddRange(GetRelevantProcesses<SqlEtlConfiguration, SqlConnectionString>(SqlDestinations, uniqueNames));

                _processes = processes.ToArray();

                // ReSharper disable once ForCanBeConvertedToForeach
                for (var i = 0; i < _processes.Length; i++)
                {
                    _database.DocumentTombstoneCleaner.Subscribe(_processes[i]);
                    _processes[i].Start();
                }
            }
        }

        private IEnumerable<EtlProcess> GetRelevantProcesses<T, TConnectionString>(List<T> configurations, HashSet<string> uniqueNames) where T : EtlConfiguration<TConnectionString> where TConnectionString : ConnectionString
        {
            foreach (var config in configurations)
            {
                SqlEtlConfiguration sqlConfig = null;
                RavenEtlConfiguration ravenConfig = null;

                switch (config.EtlType)
                {
                    case EtlType.Raven:
                        ravenConfig = config as RavenEtlConfiguration;
                        if (_databaseRecord.RavenConnectionStrings.TryGetValue(config.ConnectionStringName, out var ravenConnection) == false)
                        {
                            LogConfigurationError(config,
                                new List<string>
                                {
                                    $"Connection string named '{config.ConnectionStringName}' was not found for '{config.Name}' ETL"
                                });
                            continue;
                        }
                        ravenConfig.Initialize(ravenConnection);
                        break;
                    case EtlType.Sql:
                        sqlConfig = config as SqlEtlConfiguration;
                        if (_databaseRecord.SqlConnectionStrings.TryGetValue(config.ConnectionStringName, out var sqlConnection) == false)
                        {
                            LogConfigurationError(config,
                                new List<string>
                                {
                                    $"Connection string named '{config.ConnectionStringName}' was not found for '{config.Name}' ETL"
                                });
                            continue;
                        }
                        sqlConfig.Initialize(sqlConnection);
                        break;
                    default:
                        ThrownUnknownEtlConfiguration(config.GetType());
                        break;
                }

                if (ValidateConfiguration(config, uniqueNames) == false)
                    continue;

                if (config.Disabled)
                    continue;

                if (_databaseRecord.Topology.WhoseTaskIsIt(config,_serverStore.IsPassive()) != _serverStore.NodeTag)
                    continue;

                foreach (var transform in config.Transforms)
                {
                    if (transform.Disabled)
                        continue;

                    EtlProcess process = null;

                    if (sqlConfig != null)
                        process = new SqlEtl(transform, sqlConfig, _database, _serverStore);

                    if (ravenConfig != null)
                        process = new RavenEtl(transform, ravenConfig, _database, _serverStore);

                    yield return process;
                }
            }
        }

        public static void ThrownUnknownEtlConfiguration(Type type)
        {
            throw new InvalidOperationException($"Unknown config type: {type}");
        }

        private bool ValidateConfiguration<T>(EtlConfiguration<T> config, HashSet<string> uniqueNames) where T : ConnectionString
        {
            List<string> errors;
            if (config.Validate(out errors) == false)
            {
                LogConfigurationError(config, errors);
                return false;
            }

            if (_databaseRecord.Encrypted && config.UsingEncryptedCommunicationChannel() == false)
            {
                LogConfigurationError(config,
                    new List<string>
                    {
                        $"{_database.Name} is encrypted, but connection to ETL destination {config.GetDestination()} does not use encryption, so cannot be used"
                    });
                return false;
            }

            if (uniqueNames.Add(config.Name) == false)
            {
                LogConfigurationError(config,
                    new List<string>
                    {
                        $"ETL with name '{config.Name}' is already defined. Please just combine transformation scripts for the same destination"
                    });
                return false;
            }

            return true;
        }

        private void LogConfigurationError<T>(EtlConfiguration<T> config, List<string> errors) where T : ConnectionString
        {
            var errorMessage = $"Invalid ETL configuration for: {config.Name} to {config.GetDestination()}. " +
                               $"Reason{(errors.Count > 1 ? "s" : string.Empty)}: {string.Join(";", errors)}.";

            if (Logger.IsInfoEnabled)
                Logger.Info(errorMessage);

            var alert = AlertRaised.Create(AlertTitle, errorMessage, AlertType.Etl_Error, NotificationSeverity.Error);

            _database.NotificationCenter.Add(alert);
        }

        private void LoadConfiguration(DatabaseRecord record)
        {
            _databaseRecord = record;

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

        public void HandleDatabaseRecordChange(DatabaseRecord record)
        {
            if (record == null)
                return;

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

            LoadProcesses(record);

            // unsubscribe old etls _after_ we start new processes to ensure the tombstone cleaner 
            // constantly keeps track of tombstones processed by ETLs so it won't delete them during etl processes reloading

            old.ForEach(x => _database.DocumentTombstoneCleaner.Unsubscribe(x));
        }
    }
}