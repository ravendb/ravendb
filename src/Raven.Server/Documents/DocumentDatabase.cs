using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.Expiration;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Exceptions;
using Voron.Impl.Backup;
using DatabaseInfo = Raven.Client.ServerWide.Operations.DatabaseInfo;
using Size = Raven.Client.Util.Size;

namespace Raven.Server.Documents
{
    public class DocumentDatabase : IDisposable
    {
        private readonly ServerStore _serverStore;
        private readonly Logger _logger;

        private readonly CancellationTokenSource _databaseShutdown = new CancellationTokenSource();

        private readonly object _idleLocker = new object();
        /// <summary>
        /// The current lock, used to make sure indexes/transformers have a unique names
        /// </summary>
        private readonly SemaphoreSlim _indexAndTransformerLocker = new SemaphoreSlim(1, 1);
        private Task _indexStoreTask;
        private long _usages;
        private readonly ManualResetEventSlim _waitForUsagesOnDisposal = new ManualResetEventSlim(false);
        private long _lastIdleTicks = DateTime.UtcNow.Ticks;
        private long _lastTopologyIndex = -1;
        private long _lastClientConfigurationIndex = -1;

        public void ResetIdleTime()
        {
            _lastIdleTicks = DateTime.MinValue.Ticks;
        }

        internal void HandleNonDurableFileSystemError(object sender, NonDurabilitySupportEventArgs e)
        {
            _serverStore?.NotificationCenter.Add(AlertRaised.Create($"Non Durable File System - {Name ?? "Unknown Database"}",
                e.Message,
                AlertType.NonDurableFileSystem,
                NotificationSeverity.Warning,
                Name,
                details: new MessageDetails { Message = e.Details }));
        }

        internal void HandleOnRecoveryError(object sender, RecoveryErrorEventArgs e)
        {
            _serverStore?.NotificationCenter.Add(AlertRaised.Create($"Database Recovery Error - {Name ?? "Unknown Database"}",
                e.Message,
                AlertType.RecoveryError,
                NotificationSeverity.Error,
                Name));
        }

        public DocumentDatabase(string name, RavenConfiguration configuration, ServerStore serverStore)
        {
            _logger = LoggingSource.Instance.GetLogger<DocumentDatabase>(Name);
            _serverStore = serverStore;
            StartTime = SystemTime.UtcNow;
            Name = name;
            Configuration = configuration;

            try
            {
                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    MasterKey = serverStore.GetSecretKey(ctx, Name);

                    var databaseRecord = _serverStore.Cluster.ReadDatabase(ctx, Name);
                    if (databaseRecord != null)
                    {
                        // can happen when we are in the process of restoring a database
                        if (databaseRecord.Encrypted && MasterKey == null)
                            throw new InvalidOperationException($"Attempt to create encrypted db {Name} without supplying the secret key");
                        if (databaseRecord.Encrypted == false && MasterKey != null)
                            throw new InvalidOperationException($"Attempt to create a non-encrypted db {Name}, but a secret key exists for this db.");
                    }
                }

                QueryMetadataCache = new QueryMetadataCache();
                IoChanges = new IoChangesNotifications();
                Changes = new DocumentsChanges();
                DocumentTombstoneCleaner = new DocumentTombstoneCleaner(this);
                DocumentsStorage = new DocumentsStorage(this);
                IndexStore = new IndexStore(this, serverStore, _indexAndTransformerLocker);
                EtlLoader = new EtlLoader(this, serverStore);
                ReplicationLoader = new ReplicationLoader(this, serverStore);
                SubscriptionStorage = new SubscriptionStorage(this, serverStore);
                Metrics = new MetricsCountersManager();
                Patcher = new DocumentPatcher(this);
                TxMerger = new TransactionOperationsMerger(this, DatabaseShutdown);
                HugeDocuments = new HugeDocuments(configuration.PerformanceHints.HugeDocumentsCollectionSize,
                    configuration.PerformanceHints.HugeDocumentSize.GetValue(SizeUnit.Bytes));
                ConfigurationStorage = new ConfigurationStorage(this);
                NotificationCenter = new NotificationCenter.NotificationCenter(ConfigurationStorage.NotificationsStorage, Name, _databaseShutdown.Token);
                Operations = new Operations.Operations(Name, ConfigurationStorage.OperationsStorage, NotificationCenter, Changes);
                DatabaseInfoCache = serverStore.DatabaseInfoCache;
                RachisLogIndexNotifications = new RachisLogIndexNotifications(DatabaseShutdown);
                CatastrophicFailureNotification = new CatastrophicFailureNotification(e =>
                {
                    serverStore.DatabasesLandlord.UnloadResourceOnCatastrophicFailure(name, e);
                });
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        public ServerStore ServerStore => _serverStore;

        public DateTime LastIdleTime => new DateTime(_lastIdleTicks);

        public DatabaseInfoCache DatabaseInfoCache { get; set; }

        public readonly SystemTime Time = new SystemTime();

        public DocumentPatcher Patcher { get; private set; }

        public readonly TransactionOperationsMerger TxMerger;

        public SubscriptionStorage SubscriptionStorage { get; }

        public string Name { get; }

        public Guid DbId => DocumentsStorage.Environment?.DbId ?? Guid.Empty;

        public RavenConfiguration Configuration { get; }

        public CancellationToken DatabaseShutdown => _databaseShutdown.Token;

        public AsyncManualResetEvent DatabaseShutdownCompleted { get; } = new AsyncManualResetEvent();

        public DocumentsStorage DocumentsStorage { get; private set; }

        public ExpiredDocumentsCleaner ExpiredDocumentsCleaner { get; private set; }

        public PeriodicBackupRunner PeriodicBackupRunner { get; private set; }

        public DocumentTombstoneCleaner DocumentTombstoneCleaner { get; private set; }

        public DocumentsChanges Changes { get; }

        public IoChangesNotifications IoChanges { get; }

        public CatastrophicFailureNotification CatastrophicFailureNotification { get; }

        public NotificationCenter.NotificationCenter NotificationCenter { get; private set; }

        public Operations.Operations Operations { get; private set; }

        public HugeDocuments HugeDocuments { get; }

        public MetricsCountersManager Metrics { get; }

        public IndexStore IndexStore { get; private set; }

        public ConfigurationStorage ConfigurationStorage { get; }

        public ReplicationLoader ReplicationLoader { get; private set; }

        public EtlLoader EtlLoader { get; private set; }

        public readonly ConcurrentSet<TcpConnectionOptions> RunningTcpConnections = new ConcurrentSet<TcpConnectionOptions>();

        public DateTime StartTime { get; }

        public readonly RachisLogIndexNotifications RachisLogIndexNotifications;

        public readonly byte[] MasterKey;

        public ClientConfiguration ClientConfiguration { get; private set; }

        public long LastDatabaseRecordIndex { get; private set; }

        public readonly QueryMetadataCache QueryMetadataCache;

        public void Initialize(InitializeOptions options = InitializeOptions.None)
        {
            try
            {
                NotificationCenter.Initialize(this);

                DocumentsStorage.Initialize((options & InitializeOptions.GenerateNewDatabaseId) == InitializeOptions.GenerateNewDatabaseId);
                TxMerger.Start();
                ConfigurationStorage.Initialize();

                if ((options & InitializeOptions.SkipLoadingDatabaseRecord) == InitializeOptions.SkipLoadingDatabaseRecord)
                    return;

                long index;
                DatabaseRecord record;
                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                    record = _serverStore.Cluster.ReadDatabase(context, Name, out index);

                if (record == null)
                    DatabaseDoesNotExistException.Throw(Name);

                PeriodicBackupRunner = new PeriodicBackupRunner(this, _serverStore);

                _indexStoreTask = IndexStore.InitializeAsync(record);
                ReplicationLoader?.Initialize(record);
                EtlLoader.Initialize(record);
                Patcher.Initialize(record);

                DocumentTombstoneCleaner.Start();

                try
                {
                    _indexStoreTask.Wait(DatabaseShutdown);
                }
                finally
                {
                    _indexStoreTask = null;
                }

                SubscriptionStorage.Initialize();

                NotifyFeaturesAboutStateChange(record, index);
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        public DatabaseUsage DatabaseInUse(bool skipUsagesCount)
        {
            return new DatabaseUsage(this, skipUsagesCount);
        }

        internal void ThrowDatabaseShutdown(Exception e = null)
        {
            throw new DatabaseDisabledException("The database " + Name + " is shutting down", e);
        }

        public struct DatabaseUsage : IDisposable
        {
            private readonly DocumentDatabase _parent;
            private readonly bool _skipUsagesCount;

            public DatabaseUsage(DocumentDatabase parent, bool skipUsagesCount)
            {
                _parent = parent;
                _skipUsagesCount = skipUsagesCount;

                if (_skipUsagesCount == false)
                    Interlocked.Increment(ref _parent._usages);

                if (_parent.DatabaseShutdown.IsCancellationRequested)
                {
                    Dispose();
                    _parent.ThrowDatabaseShutdown();
                }
            }

            public void Dispose()
            {
                if (_skipUsagesCount)
                    return;

                var currentUsagesCount = Interlocked.Decrement(ref _parent._usages);

                if (_parent._databaseShutdown.IsCancellationRequested && currentUsagesCount == 0)
                    _parent._waitForUsagesOnDisposal.Set();
            }
        }

        public unsafe void Dispose()
        {
            if (_databaseShutdown.IsCancellationRequested)
                return; // double dispose?

            lock (this)
            {
                if (_databaseShutdown.IsCancellationRequested)
                    return; // double dispose?

                //before we dispose of the database we take its latest info to be displayed in the studio
                try
                {
                    var databaseInfo = GenerateDatabaseInfo();
                    if (databaseInfo != null)
                        DatabaseInfoCache?.InsertDatabaseInfo(databaseInfo, Name);
                }
                catch (Exception e)
                {
                    // if we encountered a catastrophic failure we might not be able to retrieve database info

                    if (_logger.IsInfoEnabled)
                        _logger.Info("Failed to generate and store database info", e);
                }

                _databaseShutdown.Cancel();

                // we'll wait for 1 minute to drain all the requests
                // from the database

                var sp = Stopwatch.StartNew();
                while (sp.ElapsedMilliseconds < 60 * 1000)
                {
                    if (Interlocked.Read(ref _usages) == 0)
                        break;

                    if (_waitForUsagesOnDisposal.Wait(1000))
                        _waitForUsagesOnDisposal.Reset();
                }

                var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(DocumentDatabase)} {Name}");

                foreach (var connection in RunningTcpConnections)
                {
                    exceptionAggregator.Execute(() =>
                    {
                        connection.Dispose();
                    });
                }

                exceptionAggregator.Execute(() =>
                {
                    TxMerger?.Dispose();
                });

                if (_indexStoreTask != null)
                {
                    exceptionAggregator.Execute(() =>
                    {
                        _indexStoreTask.Wait(DatabaseShutdown);
                        _indexStoreTask = null;
                    });
                }

                exceptionAggregator.Execute(() =>
                {
                    IndexStore?.Dispose();
                    IndexStore = null;
                });

                exceptionAggregator.Execute(() =>
                {
                    ExpiredDocumentsCleaner?.Dispose();
                    ExpiredDocumentsCleaner = null;
                });

                exceptionAggregator.Execute(() =>
                {
                    PeriodicBackupRunner?.Dispose();
                    PeriodicBackupRunner = null;
                });

                exceptionAggregator.Execute(() =>
                {
                    DocumentTombstoneCleaner?.Dispose();
                    DocumentTombstoneCleaner = null;
                });

                exceptionAggregator.Execute(() =>
                {
                    ReplicationLoader?.Dispose();
                    ReplicationLoader = null;
                });

                exceptionAggregator.Execute(() =>
                {
                    EtlLoader?.Dispose();
                    EtlLoader = null;
                });

                exceptionAggregator.Execute(() =>
                {
                    Operations?.Dispose(exceptionAggregator);
                    Operations = null;
                });

                exceptionAggregator.Execute(() =>
                {
                    NotificationCenter?.Dispose();
                    NotificationCenter = null;
                });

                exceptionAggregator.Execute(() =>
                {
                    Patcher?.Dispose();
                    Patcher = null;
                });

                exceptionAggregator.Execute(() =>
                {
                    SubscriptionStorage?.Dispose();
                });

                exceptionAggregator.Execute(() =>
                {
                    ConfigurationStorage?.Dispose();
                });

                exceptionAggregator.Execute(() =>
                {
                    DocumentsStorage?.Dispose();
                    DocumentsStorage = null;
                });

                exceptionAggregator.Execute(() =>
                {
                    _databaseShutdown.Dispose();
                });

                exceptionAggregator.Execute(() =>
                {
                    if (MasterKey == null)
                        return;
                    fixed (byte* pKey = MasterKey)
                    {
                        Sodium.ZeroMemory(pKey, MasterKey.Length);
                    }
                });

                exceptionAggregator.ThrowIfNeeded();
            }
        }

        public DynamicJsonValue GenerateDatabaseInfo()
        {
            var envs = GetAllStoragesEnvironment().ToList();
            if (envs.Any(x => x.Environment == null))
                return null;
            var size = new Size(envs.Sum(env => env.Environment.Stats().AllocatedDataFileSizeInBytes));
            var databaseInfo = new DynamicJsonValue
            {
                [nameof(DatabaseInfo.HasRevisionsConfiguration)] = DocumentsStorage.RevisionsStorage.Configuration != null,
                [nameof(DatabaseInfo.HasExpirationConfiguration)] = ExpiredDocumentsCleaner != null,
                [nameof(DatabaseInfo.IsAdmin)] = true, //TODO: implement me!
                [nameof(DatabaseInfo.Name)] = Name,
                [nameof(DatabaseInfo.Disabled)] = false, //TODO: this value should be overwritten by the studio since it is cached
                [nameof(DatabaseInfo.TotalSize)] = new DynamicJsonValue
                {
                    [nameof(Size.HumaneSize)] = size.HumaneSize,
                    [nameof(Size.SizeInBytes)] = size.SizeInBytes
                },
                [nameof(DatabaseInfo.IndexingErrors)] = IndexStore.GetIndexes().Sum(index => index.GetErrorCount()),
                [nameof(DatabaseInfo.Alerts)] = NotificationCenter.GetAlertCount(),
                [nameof(DatabaseInfo.UpTime)] = null, //it is shutting down
                [nameof(DatabaseInfo.BackupInfo)] = PeriodicBackupRunner.GetBackupInfo(),
                [nameof(DatabaseInfo.DocumentsCount)] = DocumentsStorage.GetNumberOfDocuments(),
                [nameof(DatabaseInfo.IndexesCount)] = IndexStore.GetIndexes().Count(),
                [nameof(DatabaseInfo.RejectClients)] = false, //TODO: implement me!
                [nameof(DatabaseInfo.IndexingStatus)] = IndexStore.Status.ToString(),
                ["CachedDatabaseInfo"] = true
            };
            return databaseInfo;
        }

        public void RunIdleOperations()
        {
            if (Monitor.TryEnter(_idleLocker) == false)
                return;

            try
            {
                _lastIdleTicks = DateTime.UtcNow.Ticks;
                IndexStore?.RunIdleOperations();
                Operations?.CleanupOperations();
                PeriodicBackupRunner?.RemoveInactiveCompletedTasks();
            }
            finally
            {
                Monitor.Exit(_idleLocker);
            }
        }

        public IEnumerable<StorageEnvironmentWithType> GetAllStoragesEnvironment()
        {
            // TODO :: more storage environments ?
            var documentsStorage = DocumentsStorage;
            if (documentsStorage != null)
                yield return
                    new StorageEnvironmentWithType(Name, StorageEnvironmentWithType.StorageEnvironmentType.Documents,
                        documentsStorage.Environment);
            var configurationStorage = ConfigurationStorage;
            if (configurationStorage != null)
                yield return
                    new StorageEnvironmentWithType("Configuration",
                        StorageEnvironmentWithType.StorageEnvironmentType.Configuration, configurationStorage.Environment);

            //check for null to prevent NRE when disposing the DocumentDatabase
            foreach (var index in (IndexStore?.GetIndexes()).EmptyIfNull())
            {
                var env = index._indexStorage?.Environment();
                if (env != null)
                    yield return
                        new StorageEnvironmentWithType(index.Name,
                            StorageEnvironmentWithType.StorageEnvironmentType.Index, env);
            }
        }

        private IEnumerable<FullBackup.StorageEnvironmentInformation> GetAllStoragesEnvironmentInformation()
        {
            var i = 1;
            foreach (var index in IndexStore.GetIndexes())
            {
                var env = index._indexStorage.Environment();
                if (env != null)
                    yield return new FullBackup.StorageEnvironmentInformation
                    {
                        Name = i++.ToString(),
                        Folder = "Indexes",
                        Env = env
                    };
            }
            yield return new FullBackup.StorageEnvironmentInformation
            {
                Name = "",
                Folder = "",
                Env = DocumentsStorage.Environment
            };
        }

        public void FullBackupTo(string backupPath)
        {
            using (var file = new FileStream(backupPath, FileMode.Create))
            using (var package = new ZipArchive(file, ZipArchiveMode.Create, leaveOpen: true))
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var databaseRecord = _serverStore.Cluster.ReadDatabase(context, Name);
                Debug.Assert(databaseRecord != null);

                var zipArchiveEntry = package.CreateEntry(RestoreSettings.FileName, CompressionLevel.Optimal);
                using (var zipStream = zipArchiveEntry.Open())
                using (var writer = new BlittableJsonTextWriter(context, zipStream))
                {
                    //TODO: encrypt this file using the MasterKey
                    //http://issues.hibernatingrhinos.com/issue/RavenDB-7546

                    writer.WriteStartObject();

                    // save the database record
                    writer.WritePropertyName(nameof(RestoreSettings.DatabaseRecord));
                    var databaseRecordBlittable = EntityToBlittable.ConvertEntityToBlittable(databaseRecord, DocumentConventions.Default, context);
                    context.Write(writer, databaseRecordBlittable);

                    // save the database values (subscriptions, periodic backups statuses, etl states...)
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(RestoreSettings.DatabaseValues));
                    writer.WriteStartObject();

                    var first = true;
                    foreach (var keyValue in ClusterStateMachine.ReadValuesStartingWith(context,
                        Helpers.ClusterStateMachineValuesPrefix(Name)))
                    {
                        if (first == false)
                        {
                            writer.WriteComma();
                        }

                        first = false;

                        writer.WritePropertyName(keyValue.Key.ToString());
                        context.Write(writer, keyValue.Value);
                    }
                    writer.WriteEndObject();
                    // end of dictionary

                    writer.WriteEndObject();
                }

                BackupMethods.Full.ToFile(GetAllStoragesEnvironmentInformation(), package);

                file.Flush(true); // make sure that we fully flushed to disk
            }
        }

        public void IncrementalBackupTo(string backupPath)
        {
            BackupMethods.Incremental.ToFile(GetAllStoragesEnvironmentInformation(), backupPath);
        }

        /// <summary>
        /// this event is intended for entities that are not singletons 
        /// per database and still need to be informed on changes to the database record.
        /// </summary>
        public event Action<DatabaseRecord> DatabaseRecordChanged;

        public void ValueChanged(long index)
        {
            try
            {
                if (_databaseShutdown.IsCancellationRequested)
                    ThrowDatabaseShutdown();

                DatabaseRecord record;
                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    record = _serverStore.Cluster.ReadDatabase(context, Name);
                }

                NotifyFeaturesAboutValueChange(record);
                RachisLogIndexNotifications.NotifyListenersAbout(index, null);

            }
            catch(Exception e)
            {
                RachisLogIndexNotifications.NotifyListenersAbout(index, e);

                if (_databaseShutdown.IsCancellationRequested)
                    ThrowDatabaseShutdown();

                throw;
            }
        }

        public void StateChanged(long index)
        {
            try
            {
                if (_databaseShutdown.IsCancellationRequested)
                    ThrowDatabaseShutdown();

                DatabaseRecord record;
                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    record = _serverStore.Cluster.ReadDatabase(context, Name);
                }

                if (_lastTopologyIndex < record.Topology.Stamp.Index)
                    _lastTopologyIndex = record.Topology.Stamp.Index;

                ClientConfiguration = record.Client;
                _lastClientConfigurationIndex = record.Client?.Etag ?? -1;

                NotifyFeaturesAboutStateChange(record, index);
                RachisLogIndexNotifications.NotifyListenersAbout(index, null);
            }
            catch (Exception e)
            {
                RachisLogIndexNotifications.NotifyListenersAbout(index, e);

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Got exception during StateChanged({index}).", e);

                if (_databaseShutdown.IsCancellationRequested)
                    ThrowDatabaseShutdown(e);

                throw;
            }
        }

        private void NotifyFeaturesAboutStateChange(DatabaseRecord record, long index)
        {
            lock (this)
            {
                Debug.Assert(string.Equals(Name, record.DatabaseName, StringComparison.OrdinalIgnoreCase),
                    $"{Name} != {record.DatabaseName}");

                // index and LastDatabaseRecordIndex could have equal values when we transit from/to passive and want to update the tasks. 
                if (LastDatabaseRecordIndex > index)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Skipping record {index} (current {LastDatabaseRecordIndex}) for {record.DatabaseName} because it was already precessed.");
                    return;
                }

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Starting to process record {index} (current {LastDatabaseRecordIndex}) for {record.DatabaseName}.");


                try
                {
                    InitializeFromDatabaseRecord(record);
                    LastDatabaseRecordIndex = index;

                    IndexStore.HandleDatabaseRecordChange(record, index);
                    ReplicationLoader?.HandleDatabaseRecordChange(record);
                    EtlLoader?.HandleDatabaseRecordChange(record);
                    OnDatabaseRecordChanged(record);
                    SubscriptionStorage?.HandleDatabaseValueChange(record);
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Finish to process record {index} for {record.DatabaseName}.");
                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Encounter an error while processing record {index} for {record.DatabaseName}.", e);
                    throw;
                }
            }
        }

        private void NotifyFeaturesAboutValueChange(DatabaseRecord record)
        {
            SubscriptionStorage?.HandleDatabaseValueChange(record);
        }

        public void RefreshFeatures()
        {
            if (_databaseShutdown.IsCancellationRequested)
                ThrowDatabaseShutdown();

            DatabaseRecord record;
            long index;
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                record = _serverStore.Cluster.ReadDatabase(context, Name, out index);
            }
            NotifyFeaturesAboutStateChange(record, index);
        }

        private void InitializeFromDatabaseRecord(DatabaseRecord record)
        {
            if (record == null)
                return;

            ClientConfiguration = record.Client;
            DocumentsStorage.RevisionsStorage.InitializeFromDatabaseRecord(record);
            ExpiredDocumentsCleaner = ExpiredDocumentsCleaner.LoadConfigurations(this, record, ExpiredDocumentsCleaner);
            PeriodicBackupRunner.UpdateConfigurations(record);
        }

        public IEnumerable<DatabasePerformanceMetrics> GetAllPerformanceMetrics()
        {
            yield return TxMerger.GeneralWaitPerformanceMetrics;
            yield return TxMerger.TransactionPerformanceMetrics;
        }

        private void OnDatabaseRecordChanged(DatabaseRecord record)
        {
            DatabaseRecordChanged?.Invoke(record);
        }

        public bool HasTopologyChanged(long index)
        {
            return _lastTopologyIndex != index;
        }

        public bool HasClientConfigurationChanged(long index)
        {
            var actual = Hashing.Combine(_lastClientConfigurationIndex, ServerStore.LastClientConfigurationIndex);

            return index != actual;
        }
    }

    public class StorageEnvironmentWithType
    {
        public string Name { get; set; }
        public StorageEnvironmentType Type { get; set; }
        public StorageEnvironment Environment { get; set; }

        public StorageEnvironmentWithType(string name, StorageEnvironmentType type, StorageEnvironment environment)
        {
            Name = name;
            Type = type;
            Environment = environment;
        }

        public enum StorageEnvironmentType
        {
            Documents,
            Subscriptions,
            Index,
            Configuration
        }
    }
}
