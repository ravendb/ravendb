using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.ServerWide;
using Raven.Server.Documents.ETL.Providers.Parquet;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.ETL.Providers.SQL;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Logging;

namespace Raven.Server.Documents.ETL
{
    public class EtlLoader : IDisposable, ITombstoneAware
    {
        private const string AlertTitle = "ETL loader";

        private EtlProcess[] _processes = new EtlProcess[0];
        private readonly HashSet<string> _uniqueConfigurationNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase); // read and modified under a lock.

        private DatabaseRecord _databaseRecord;

        private readonly object _loadProcessedLock = new object();
        private readonly DocumentDatabase _database;
        private readonly ServerStore _serverStore;
        private bool _isSubscribedToDocumentChanges;
        private bool _isSubscribedToCounterChanges;
        private bool _isSubscribedToTimeSeriesChanges;

        protected Logger Logger;

        public event Action<(string ConfigurationName, string TransformationName, EtlProcessStatistics Statistics)> BatchCompleted;

        public void OnBatchCompleted(string configurationName, string transformationName, EtlProcessStatistics statistics)
        {
            BatchCompleted?.Invoke((configurationName, transformationName, statistics));
        }

        public EtlLoader(DocumentDatabase database, ServerStore serverStore)
        {
            Logger = LoggingSource.Instance.GetLogger(database.Name, GetType().FullName);

            _database = database;
            _serverStore = serverStore;

            database.TombstoneCleaner.Subscribe(this);
        }

        public EtlProcess[] Processes => _processes;

        public List<RavenEtlConfiguration> RavenDestinations;

        public List<SqlEtlConfiguration> SqlDestinations;

        public List<ParquetEtlConfiguration> S3Destinations;

        public void Initialize(DatabaseRecord record)
        {
            LoadProcesses(record, record.RavenEtls, record.SqlEtls, record.ParquetEtls, toRemove: null);
        }

        public event Action<EtlProcess> ProcessAdded;

        public event Action<EtlProcess> ProcessRemoved;

        private void OnProcessAdded(EtlProcess process)
        {
            ProcessAdded?.Invoke(process);
        }

        private void OnProcessRemoved(EtlProcess process)
        {
            ProcessRemoved?.Invoke(process);
        }

        private void LoadProcesses(DatabaseRecord record,
            List<RavenEtlConfiguration> newRavenDestinations,
            List<SqlEtlConfiguration> newSqlDestinations,
            List<ParquetEtlConfiguration> newS3Destinations,
            List<EtlProcess> toRemove)
        {
            lock (_loadProcessedLock)
            {
                _databaseRecord = record;
                RavenDestinations = _databaseRecord.RavenEtls;
                SqlDestinations = _databaseRecord.SqlEtls;
                S3Destinations = _databaseRecord.ParquetEtls;
                var processes = new List<EtlProcess>(_processes);

                if (toRemove != null && toRemove.Count > 0)
                {
                    foreach (var process in toRemove)
                    {
                        processes.Remove(process);
                        _uniqueConfigurationNames.Remove(process.ConfigurationName);

                        OnProcessRemoved(process);
                    }
                }

                var ensureUniqueConfigurationNames = _uniqueConfigurationNames.ToHashSet(StringComparer.OrdinalIgnoreCase);

                var newProcesses = new List<EtlProcess>();
                if (newRavenDestinations != null && newRavenDestinations.Count > 0)
                    newProcesses.AddRange(GetRelevantProcesses<RavenEtlConfiguration, RavenConnectionString>(newRavenDestinations, ensureUniqueConfigurationNames));

                if (newSqlDestinations != null && newSqlDestinations.Count > 0)
                    newProcesses.AddRange(GetRelevantProcesses<SqlEtlConfiguration, SqlConnectionString>(newSqlDestinations, ensureUniqueConfigurationNames));

                if (newS3Destinations != null && newS3Destinations.Count > 0)
                    newProcesses.AddRange(GetRelevantProcesses<ParquetEtlConfiguration, ParquetEtlConnectionString>(newS3Destinations, ensureUniqueConfigurationNames));

                processes.AddRange(newProcesses);
                _processes = processes.ToArray();

                HandleChangesSubscriptions();

                foreach (var process in newProcesses)
                {
                    process.Start();

                    OnProcessAdded(process);

                    _uniqueConfigurationNames.Add(process.ConfigurationName);
                }
            }
        }

        private void HandleChangesSubscriptions()
        {
            // this is supposed to be called only under lock

            if (_processes.Length > 0)
            {
                if (_isSubscribedToDocumentChanges == false)
                {
                    _database.Changes.OnDocumentChange += OnDocumentChange;
                    _isSubscribedToDocumentChanges = true;
                }

                var needToWatchCounters = _processes.Any(x => x.ShouldTrackCounters());

                if (needToWatchCounters)
                {
                    if (_isSubscribedToCounterChanges == false)
                    {
                        _database.Changes.OnCounterChange += OnCounterChange;
                        _isSubscribedToCounterChanges = true;
                    }
                }
                else
                {
                    _database.Changes.OnCounterChange -= OnCounterChange;
                    _isSubscribedToCounterChanges = false;
                }

                var needToWatchTimeSeries = _processes.Any(x => x.ShouldTrackTimeSeries());
                if (needToWatchTimeSeries)
                {
                    if (_isSubscribedToTimeSeriesChanges == false)
                    {
                        _database.Changes.OnTimeSeriesChange += OnTimeSeriesChange;
                        _isSubscribedToTimeSeriesChanges = true;
                    }
                }
                else
                {
                    _database.Changes.OnTimeSeriesChange -= OnTimeSeriesChange;
                    _isSubscribedToTimeSeriesChanges = false;
                }
            }
            else
            {
                _database.Changes.OnDocumentChange -= OnDocumentChange;
                _isSubscribedToDocumentChanges = false;

                _database.Changes.OnCounterChange -= OnCounterChange;
                _isSubscribedToCounterChanges = false;

                _database.Changes.OnTimeSeriesChange -= OnTimeSeriesChange;
                _isSubscribedToTimeSeriesChanges = false;
            }
        }

        private IEnumerable<EtlProcess> GetRelevantProcesses<T, TConnectionString>(List<T> configurations, HashSet<string> uniqueNames) where T : EtlConfiguration<TConnectionString> where TConnectionString : ConnectionString
        {
            foreach (var config in configurations)
            {
                SqlEtlConfiguration sqlConfig = null;
                RavenEtlConfiguration ravenConfig = null;
                ParquetEtlConfiguration s3Config = null;

                var connectionStringNotFound = false;

                switch (config.EtlType)
                {
                    case EtlType.Raven:
                        ravenConfig = config as RavenEtlConfiguration;
                        if (_databaseRecord.RavenConnectionStrings.TryGetValue(config.ConnectionStringName, out var ravenConnection))
                            ravenConfig.Initialize(ravenConnection);
                        else
                            connectionStringNotFound = true;

                        break;

                    case EtlType.Sql:
                        sqlConfig = config as SqlEtlConfiguration;
                        if (_databaseRecord.SqlConnectionStrings.TryGetValue(config.ConnectionStringName, out var sqlConnection))
                            sqlConfig.Initialize(sqlConnection);
                        else
                            connectionStringNotFound = true;

                        break;
                    case EtlType.Parquet:
                        s3Config = config as ParquetEtlConfiguration;
                        if (_databaseRecord.ParquetEtlConnectionStrings.TryGetValue(config.ConnectionStringName, out var s3Connection))
                            s3Config.Initialize(s3Connection);
                        else
                            connectionStringNotFound = true;
                        break;
                    default:
                        ThrownUnknownEtlConfiguration(config.GetType());
                        break;
                }

                if (connectionStringNotFound)
                {
                    LogConfigurationError(config,
                        new List<string>
                        {
                            $"Connection string named '{config.ConnectionStringName}' was not found for {config.EtlType} ETL"
                        });

                    continue;
                }

                if (ValidateConfiguration(config, uniqueNames) == false)
                    continue;

                var processState = GetProcessState(config.Transforms, _database, config.Name);
                var whoseTaskIsIt = _database.WhoseTaskIsIt(_databaseRecord.Topology, config, processState);
                if (whoseTaskIsIt != _serverStore.NodeTag)
                    continue;

                foreach (var transform in config.Transforms)
                {
                    EtlProcess process = null;

                    if (sqlConfig != null)
                        process = new SqlEtl(transform, sqlConfig, _database, _serverStore);
                    if (ravenConfig != null)
                        process = new RavenEtl(transform, ravenConfig, _database, _serverStore); 
                    if (s3Config != null)
                        process = new ParquetEtl(transform, s3Config, _database, _serverStore);

                    yield return process;
                }
            }
        }

        public static EtlProcessState GetProcessState(List<Transformation> configTransforms, DocumentDatabase database, string configurationName)
        {
            EtlProcessState processState = null;

            foreach (var transform in configTransforms)
            {
                if (transform.Name == null)
                    continue;

                processState = EtlProcess.GetProcessState(database, configurationName, transform.Name);
                if (processState.NodeTag != null)
                    break;
            }

            return processState ?? new EtlProcessState();
        }

        public static void ThrownUnknownEtlConfiguration(Type type)
        {
            throw new InvalidOperationException($"Unknown config type: {type}");
        }

        private bool ValidateConfiguration<T>(EtlConfiguration<T> config, HashSet<string> uniqueNames) where T : ConnectionString
        {
            if (config.Validate(out List<string> errors) == false)
            {
                LogConfigurationError(config, errors);
                return false;
            }

            if (_databaseRecord.Encrypted && config.UsingEncryptedCommunicationChannel() == false && config.AllowEtlOnNonEncryptedChannel == false)
            {
                LogConfigurationError(config,
                    new List<string>
                    {
                        $"{_database.Name} is encrypted, but connection to ETL destination {config.GetDestination()} does not use encryption, so ETL is not allowed. " +
                        $"You can change this behavior by setting {nameof(config.AllowEtlOnNonEncryptedChannel)} when creating the ETL configuration"
                    });
                return false;
            }

            if (_databaseRecord.Encrypted && config.UsingEncryptedCommunicationChannel() == false && config.AllowEtlOnNonEncryptedChannel)
            {
                LogConfigurationWarning(config,
                    new List<string>
                    {
                        $"{_database.Name} is encrypted and connection to ETL destination {config.GetDestination()} does not use encryption, " +
                        $"but {nameof(config.AllowEtlOnNonEncryptedChannel)} is set to true, so ETL is allowed"
                    });
                return true;
            }

            if (uniqueNames.Add(config.Name) == false)
            {
                LogConfigurationError(config,
                    new List<string>
                    {
                        $"ETL with name '{config.Name}' is already defined"
                    });
                return false;
            }

            return true;
        }

        private void LogConfigurationError<T>(EtlConfiguration<T> config, List<string> errors) where T : ConnectionString
        {
            var errorMessage = $"Invalid ETL configuration for '{config.Name}'{(config.Connection != null ? $" ({config.GetDestination()})" : string.Empty)}. " +
                               $"Reason{(errors.Count > 1 ? "s" : string.Empty)}: {string.Join(";", errors)}.";

            if (Logger.IsInfoEnabled)
                Logger.Info(errorMessage);

            var alert = AlertRaised.Create(_database.Name, AlertTitle, errorMessage, AlertType.Etl_Error, NotificationSeverity.Error);

            _database.NotificationCenter.Add(alert);
        }

        private void LogConfigurationWarning<T>(EtlConfiguration<T> config, List<string> warnings) where T : ConnectionString
        {
            var warnMessage = $"Warning about ETL configuration for '{config.Name}'{(config.Connection != null ? $" ({config.GetDestination()})" : string.Empty)}. " +
                               $"Reason{(warnings.Count > 1 ? "s" : string.Empty)}: {string.Join(";", warnings)}.";

            if (Logger.IsInfoEnabled)
                Logger.Info(warnMessage);

            var alert = AlertRaised.Create(_database.Name, AlertTitle, warnMessage, AlertType.Etl_Warning, NotificationSeverity.Warning);

            _database.NotificationCenter.Add(alert);
        }

        private void OnCounterChange(CounterChange change) => NotifyAboutWork(change);

        private void OnDocumentChange(DocumentChange change) => NotifyAboutWork(change);

        private void OnTimeSeriesChange(TimeSeriesChange change) => NotifyAboutWork(change);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void NotifyAboutWork(DatabaseChange change)
        {
            var processes = _processes;

            // ReSharper disable once ForCanBeConvertedToForeach
            for (var i = 0; i < processes.Length; i++)
            {
                try
                {
                    processes[i].NotifyAboutWork(change);
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }

        public virtual void Dispose()
        {
            _database.Changes.OnDocumentChange -= OnDocumentChange;
            _database.Changes.OnCounterChange -= OnCounterChange;

            var ea = new ExceptionAggregator(Logger, "Could not dispose ETL loader");

            Parallel.ForEach(_processes, x => ea.Execute(x.Dispose));

            ea.Execute(() => _database.TombstoneCleaner.Unsubscribe(this));

            ea.ThrowIfNeeded();
        }

        private bool IsMyEtlTask<T, TConnectionString>(DatabaseRecord record, T etlTask, ref Dictionary<string, string> responsibleNodes)
            where TConnectionString : ConnectionString
            where T : EtlConfiguration<TConnectionString>
        {
            var processState = GetProcessState(etlTask.Transforms, _database, etlTask.Name);
            var whoseTaskIsIt = _database.WhoseTaskIsIt(record.Topology, etlTask, processState);

            responsibleNodes[etlTask.Name] = whoseTaskIsIt;

            return whoseTaskIsIt == _serverStore.NodeTag;
        }

        public void HandleDatabaseRecordChange(DatabaseRecord record)
        {
            if (record == null)
                return;

            var myRavenEtl = new List<RavenEtlConfiguration>();
            var mySqlEtl = new List<SqlEtlConfiguration>();
            var myS3Etl = new List<ParquetEtlConfiguration>();


            var responsibleNodes = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (var config in record.RavenEtls)
            {
                if (IsMyEtlTask<RavenEtlConfiguration, RavenConnectionString>(record, config, ref responsibleNodes))
                {
                    myRavenEtl.Add(config);
                }
            }

            foreach (var config in record.SqlEtls)
            {
                if (IsMyEtlTask<SqlEtlConfiguration, SqlConnectionString>(record, config, ref responsibleNodes))
                {
                    mySqlEtl.Add(config);
                }
            }

            foreach (var config in record.ParquetEtls)
            {
                if (IsMyEtlTask<ParquetEtlConfiguration, ParquetEtlConnectionString>(record, config, ref responsibleNodes))
                {
                    myS3Etl.Add(config);
                }
            }

            var toRemove = _processes.GroupBy(x => x.ConfigurationName).ToDictionary(x => x.Key, x => x.ToList());

            foreach (var processesPerConfig in _processes.GroupBy(x => x.ConfigurationName))
            {
                var process = processesPerConfig.First();

                Debug.Assert(processesPerConfig.All(x => x.GetType() == process.GetType()));

                switch (process)
                {
                    case RavenEtl ravenEtl:
                    {
                        RavenEtlConfiguration existing = null;

                        foreach (var config in myRavenEtl)
                        {
                            var diff = ravenEtl.Configuration.Compare(config);

                            if (diff == EtlConfigurationCompareDifferences.None)
                            {
                                existing = config;
                                break;
                            }
                        }

                        if (existing != null)
                        {
                            toRemove.Remove(processesPerConfig.Key);
                            myRavenEtl.Remove(existing);
                        }

                        break;
                    }
                    case SqlEtl sqlEtl:
                    {
                        SqlEtlConfiguration existing = null;
                        foreach (var config in mySqlEtl)
                        {
                            var diff = sqlEtl.Configuration.Compare(config);

                            if (diff == EtlConfigurationCompareDifferences.None)
                            {
                                existing = config;
                                break;
                            }
                        }
                        if (existing != null)
                        {
                            toRemove.Remove(processesPerConfig.Key);
                            mySqlEtl.Remove(existing);
                        }

                        break;
                    }
                    case ParquetEtl s3Etl:
                    {
                        ParquetEtlConfiguration existing = null;

                        foreach (var config in myS3Etl)
                        {
                            var diff = s3Etl.Configuration.Compare(config);

                            if (diff == EtlConfigurationCompareDifferences.None)
                            {
                                existing = config;
                                break;
                            }
                        }

                        if (existing != null)
                        {
                            toRemove.Remove(processesPerConfig.Key);
                            myS3Etl.Remove(existing);
                        }

                        break;
                    }
                    default:
                        throw new InvalidOperationException($"Unknown ETL process type: {process.GetType()}");
                }
            }

            Parallel.ForEach(toRemove, x =>
            {
                foreach (var process in x.Value)
                {
                    _database.DatabaseShutdown.ThrowIfCancellationRequested();

                    try
                    {
                        string reason = GetStopReason(process, myRavenEtl, mySqlEtl, responsibleNodes);

                        process.Stop(reason);
                    }
                    catch (Exception e)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info($"Failed to stop ETL process {process.Name} on the database record change", e);
                    }
                }
            });

            LoadProcesses(record, myRavenEtl, mySqlEtl, myS3Etl, toRemove.SelectMany(x => x.Value).ToList());

            Parallel.ForEach(toRemove, x =>
            {
                foreach (var process in x.Value)
                {
                    _database.DatabaseShutdown.ThrowIfCancellationRequested();

                    try
                    {
                        process.Dispose();
                    }
                    catch (Exception e)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info($"Failed to dispose ETL process {process.Name} on the database record change", e);
                    }
                }
            });
        }

        private static string GetStopReason(EtlProcess process, List<RavenEtlConfiguration> myRavenEtl, List<SqlEtlConfiguration> mySqlEtl, Dictionary<string, string> responsibleNodes)
        {
            EtlConfigurationCompareDifferences? differences = null;
            var transformationDiffs = new List<(string TransformationName, EtlConfigurationCompareDifferences Difference)>();

            var reason = "Database record change. ";

            if (process is RavenEtl ravenEtl)
            {
                var existing = myRavenEtl.FirstOrDefault(x => x.Name.Equals(ravenEtl.ConfigurationName, StringComparison.OrdinalIgnoreCase));

                if (existing != null)
                    differences = ravenEtl.Configuration.Compare(existing, transformationDiffs);
            }
            else if (process is SqlEtl sqlEtl)
            {
                var existing = mySqlEtl.FirstOrDefault(x => x.Name.Equals(sqlEtl.ConfigurationName, StringComparison.OrdinalIgnoreCase));

                if (existing != null)
                    differences = sqlEtl.Configuration.Compare(existing, transformationDiffs);
            }
            else
            {
                throw new InvalidOperationException($"Unknown ETL process type: " + process.GetType().FullName);
            }

            if (differences != null)
            {
                reason += $"Configuration changes: {differences}. Details: ";

                foreach (var transformationDiff in transformationDiffs)
                {
                    reason += $"Script '{transformationDiff.TransformationName}' - {transformationDiff.Difference}. ";
                }
            }
            else
            {
                if (responsibleNodes.TryGetValue(process.ConfigurationName, out var responsibleNode))
                {
                    reason += $"ETL was moved to another node. Responsible node is: {responsibleNode}";
                }
                else
                {
                    reason += $"ETL was deleted or moved to another node (no configuration named '{process.ConfigurationName}' was found). ";
                }
            }

            return reason;
        }

        public void HandleDatabaseValueChanged(DatabaseRecord record)
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var process in _processes)
                {
                    var state = _serverStore.Cluster.Read(context, EtlProcessState.GenerateItemName(record.DatabaseName, process.ConfigurationName, process.TransformationName));

                    if (state == null)
                    {
                        process.Reset();
                    }
                }
            }
        }

        public string TombstoneCleanerIdentifier => $"ETL loader for {_database.Name}";

        public Dictionary<string, long> GetLastProcessedTombstonesPerCollection(ITombstoneAware.TombstoneType tombstoneType)
        {
            var lastProcessedTombstones = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);

            var ravenEtls = _databaseRecord.RavenEtls;
            if (tombstoneType == ITombstoneAware.TombstoneType.TimeSeries)
            {
                foreach (var config in ravenEtls)
                    MarkTimeSeriesTombstonesForDeletion(config, lastProcessedTombstones);
            }
            else
            {
                var sqlEtls = _databaseRecord.SqlEtls;

                foreach (var config in ravenEtls)
                    MarkDocumentTombstonesForDeletion(config, lastProcessedTombstones);

                foreach (var config in sqlEtls)
                    MarkDocumentTombstonesForDeletion(config, lastProcessedTombstones);
            }
            return lastProcessedTombstones;
        }

        private void MarkDocumentTombstonesForDeletion<T>(EtlConfiguration<T> config, Dictionary<string, long> lastProcessedTombstones) where T : ConnectionString
        {
            foreach (var transform in config.Transforms)
            {
                var state = EtlProcess.GetProcessState(_database, config.Name, transform.Name);
                var etag = ChangeVectorUtils.GetEtagById(state.ChangeVector, _database.DbBase64Id);

                // the default in this case is '0', which means that nothing of this node was consumed and therefore we cannot delete anything
                if (transform.ApplyToAllDocuments)
                {
                    AddOrUpdate(lastProcessedTombstones, Constants.Documents.Collections.AllDocumentsCollection, etag);
                    continue;
                }

                foreach (var collection in transform.Collections)
                    AddOrUpdate(lastProcessedTombstones, collection, etag);

                if (typeof(T) == typeof(RavenConnectionString))
                {
                    if (RavenEtl.ShouldTrackAttachmentTombstones(transform))
                        AddOrUpdate(lastProcessedTombstones, AttachmentsStorage.AttachmentsTombstones, etag);
                }
            }
        }

        private void MarkTimeSeriesTombstonesForDeletion<T>(EtlConfiguration<T> config, Dictionary<string, long> lastProcessedTombstones) where T : ConnectionString
        {
            foreach (var transform in config.Transforms)
            {
                var state = EtlProcess.GetProcessState(_database, config.Name, transform.Name);
                var etag = ChangeVectorUtils.GetEtagById(state.ChangeVector, _database.DbBase64Id);

                AddOrUpdate(lastProcessedTombstones, Constants.TimeSeries.All, etag);
            }
        }

        private void AddOrUpdate(Dictionary<string, long> dic, string key, long value)
        {
            if (dic.TryGetValue(key, out var old) == false)
            {
                dic[key] = value;
                return;
            }

            var min = Math.Min(value, old);
            dic[key] = min;
        }
    }
}
