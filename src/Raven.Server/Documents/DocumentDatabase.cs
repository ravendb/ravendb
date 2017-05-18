using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Extensions;
using Raven.Client.Server;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Documents.Transformers;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Exceptions;
using Voron.Impl.Backup;
using DatabaseInfo = Raven.Client.Server.Operations.DatabaseInfo;
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
        private Task _transformerStoreTask;
        private long _usages;
        private readonly ManualResetEventSlim _waitForUsagesOnDisposal = new ManualResetEventSlim(false);
        private long _lastIdleTicks = DateTime.UtcNow.Ticks;

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
            IoChanges = new IoChangesNotifications();
            Changes = new DocumentsChanges();
            DocumentTombstoneCleaner = new DocumentTombstoneCleaner(this);
            DocumentsStorage = new DocumentsStorage(this);
            IndexStore = new IndexStore(this, serverStore, _indexAndTransformerLocker);
            TransformerStore = new TransformerStore(this, serverStore, _indexAndTransformerLocker);
            EtlLoader = new EtlLoader(this);
            if(serverStore != null)
                ReplicationLoader = new ReplicationLoader(this, serverStore);
            SubscriptionStorage = new SubscriptionStorage(this, serverStore);
            Operations = new DatabaseOperations(this);
            Metrics = new MetricsCountersManager();
            Patcher = new DocumentPatcher(this);
            TxMerger = new TransactionOperationsMerger(this, DatabaseShutdown);
            HugeDocuments = new HugeDocuments(configuration.PerformanceHints.HugeDocumentsCollectionSize,
                configuration.PerformanceHints.HugeDocumentSize.GetValue(SizeUnit.Bytes));
            ConfigurationStorage = new ConfigurationStorage(this);
            NotificationCenter = new NotificationCenter.NotificationCenter(ConfigurationStorage.NotificationsStorage, Name, _databaseShutdown.Token);
            DatabaseInfoCache = serverStore?.DatabaseInfoCache;
            RachisLogIndexNotifications = new RachisLogIndexNotifications(DatabaseShutdown);
            CatastrophicFailureNotification = new CatastrophicFailureNotification(e =>
            {
                serverStore?.DatabasesLandlord.UnloadResourceOnCatastrophicFailue(name, e);
            });
        }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        public DateTime LastIdleTime => new DateTime(_lastIdleTicks);

        public DatabaseInfoCache DatabaseInfoCache { get; set; }

        public SystemTime Time = new SystemTime();

        public DocumentPatcher Patcher { get; private set; }

        public readonly TransactionOperationsMerger TxMerger;

        public SubscriptionStorage SubscriptionStorage { get; }

        public string Name { get; }

        public Guid DbId => DocumentsStorage.Environment?.DbId ?? Guid.Empty;

        public RavenConfiguration Configuration { get; }

        public CancellationToken DatabaseShutdown => _databaseShutdown.Token;

        public DocumentsStorage DocumentsStorage { get; private set; }

        public BundleLoader BundleLoader { get; private set; }

        public DocumentTombstoneCleaner DocumentTombstoneCleaner { get; private set; }

        public DocumentsChanges Changes { get; }

        public IoChangesNotifications IoChanges { get; }

        public CatastrophicFailureNotification CatastrophicFailureNotification { get; }

        public NotificationCenter.NotificationCenter NotificationCenter { get; private set; }

        public DatabaseOperations Operations { get; private set; }

        public HugeDocuments HugeDocuments { get; }

        public MetricsCountersManager Metrics { get; }

        public IndexStore IndexStore { get; private set; }

        public TransformerStore TransformerStore { get; }

        public ConfigurationStorage ConfigurationStorage { get; }

        public ReplicationLoader ReplicationLoader { get; private set; }

        public EtlLoader EtlLoader { get; private set; }

        public ConcurrentSet<TcpConnectionOptions> RunningTcpConnections = new ConcurrentSet<TcpConnectionOptions>();

        public DateTime StartTime { get; }

        public void Initialize()
        {
            try
            {
                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    MasterKey = _serverStore.GetSecretKey(ctx, Name);

                    var databaseRecord = _serverStore.Cluster.ReadDatabase(ctx, Name);
                    if (databaseRecord.Encrypted && MasterKey == null)
                        throw new InvalidOperationException($"Attempt to create encrypted db {Name} without supplying the secret key");
                    if (databaseRecord.Encrypted == false && MasterKey != null)
                        throw new InvalidOperationException($"Attempt to create a non-encrypted db {Name}, but a secret key exists for this db.");
                }
                DocumentsStorage.Initialize();
                InitializeInternal();
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        public void Initialize(StorageEnvironmentOptions options)
        {
            try
            {
                DocumentsStorage.Initialize(options);
                InitializeInternal();
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

        internal void ThrowOperationCancelled()
        {
            throw new OperationCanceledException("The database " + Name + " is shutting down");
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

                if (_parent._databaseShutdown.IsCancellationRequested)
                {
                    Dispose();
                    _parent.ThrowOperationCancelled();
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

        private void InitializeInternal()
        {
            TxMerger.Start();

            ConfigurationStorage.InitializeNotificationsStorage();

            DatabaseRecord record;
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
                record = _serverStore.Cluster.ReadDatabase(context, Name);
            

            _indexStoreTask = IndexStore.InitializeAsync(record);
            _transformerStoreTask = TransformerStore.InitializeAsync(record);

            BundleLoader = new BundleLoader(this, _serverStore);
            Patcher.Initialize();
            EtlLoader.Initialize();

            DocumentTombstoneCleaner.Start();

            try
            {
                _indexStoreTask.Wait(DatabaseShutdown);
            }
            finally
            {
                _indexStoreTask = null;
            }

            try
            {
                _transformerStoreTask.Wait(DatabaseShutdown);
            }
            finally
            {
                _transformerStoreTask = null;
            }

            SubscriptionStorage.Initialize();

            //Index Metadata Store shares Voron env and context pool with documents storage, 
            //so replication of both documents and indexes/transformers can be made within one transaction
            ConfigurationStorage.Initialize(IndexStore, TransformerStore);

            ReplicationLoader?.Initialize();

            NotificationCenter.Initialize(this);
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

                exceptionAggregator.Execute(() =>
                {
                    TransformerStore.Dispose();
                });

                if (_indexStoreTask != null)
                {
                    exceptionAggregator.Execute(() =>
                    {
                        _indexStoreTask.Wait(DatabaseShutdown);
                        _indexStoreTask = null;
                    });
                }

                if (_transformerStoreTask != null)
                {
                    exceptionAggregator.Execute(() =>
                    {
                        _transformerStoreTask.Wait(DatabaseShutdown);
                        _transformerStoreTask = null;
                    });
                }

                exceptionAggregator.Execute(() =>
                {
                    IndexStore?.Dispose();
                    IndexStore = null;
                });

                exceptionAggregator.Execute(() =>
                {
                    BundleLoader?.Dispose();
                    BundleLoader = null;
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

        private static readonly string CachedDatabaseInfo = "CachedDatabaseInfo";

        public DynamicJsonValue GenerateDatabaseInfo()
        {
            var envs = GetAllStoragesEnvironment().ToList();
            if (envs.Any(x => x.Environment == null))
                return null;
            Size size = new Size(envs.Sum(env => env.Environment.Stats().AllocatedDataFileSizeInBytes));
            var databaseInfo = new DynamicJsonValue
            {
                [nameof(DatabaseInfo.Bundles)] = BundleLoader != null ? new DynamicJsonArray(BundleLoader.GetActiveBundles()) : null,
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
                [nameof(DatabaseInfo.BackupInfo)] = BundleLoader?.GetBackupInfo(),
                [nameof(DatabaseInfo.DocumentsCount)] = DocumentsStorage.GetNumberOfDocuments(),
                [nameof(DatabaseInfo.IndexesCount)] = IndexStore.GetIndexes().Count(),
                [nameof(DatabaseInfo.RejectClients)] = false, //TODO: implement me!
                [nameof(DatabaseInfo.IndexingStatus)] = IndexStore.Status.ToString(),
                [CachedDatabaseInfo] = true
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
            }

            finally
            {
                Monitor.Exit(_idleLocker);
            }
        }

        public IEnumerable<StorageEnvironmentWithType> GetAllStoragesEnvironment()
        {
            // TODO :: more storage environments ?
            yield return
                new StorageEnvironmentWithType(Name, StorageEnvironmentWithType.StorageEnvironmentType.Documents,
                    DocumentsStorage.Environment);            
            yield return
                new StorageEnvironmentWithType("Configuration",
                    StorageEnvironmentWithType.StorageEnvironmentType.Configuration, ConfigurationStorage.Environment);

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
                    yield return (new FullBackup.StorageEnvironmentInformation()
                    {
                        Name = i++.ToString(),
                        Folder = "Indexes",
                        Env = env
                    });
            }
            yield return (new FullBackup.StorageEnvironmentInformation()
            {
                Name = "",
                Folder = "",
                Env = DocumentsStorage.Environment
            });
        }

        public void FullBackupTo(string backupPath)
        {
            BackupMethods.Full.ToFile(GetAllStoragesEnvironmentInformation(), backupPath);
        }

        public void IncrementalBackupTo(string backupPath)
        {
            BackupMethods.Incremental.ToFile(GetAllStoragesEnvironmentInformation(), backupPath);
        }

        public void StateChanged(long index)
        {
            try
            {
                if (_databaseShutdown.IsCancellationRequested)
                    ThrowOperationCancelled();

                TransformerStore.HandleDatabaseRecordChange();
                BundleLoader.HandleDatabaseRecordChange();
                IndexStore.HandleDatabaseRecordChange();
                ReplicationLoader?.HandleDatabaseRecordChange();
                SubscriptionStorage?.HandleDatabaseValueChange();
            }
            catch
            {
                if (_databaseShutdown.IsCancellationRequested)
                    ThrowOperationCancelled();
            }
            finally
            {
                RachisLogIndexNotifications.NotifyListenersAbout(index);
            }
        }

        public Task WaitForIndexNotification(long index) => RachisLogIndexNotifications.WaitForIndexNotification(index, Configuration.Cluster.ClusterOperationTimeout.AsTimeSpan);

        public readonly RachisLogIndexNotifications RachisLogIndexNotifications;
        public byte[] MasterKey;

        public IEnumerable<DatabasePerformanceMetrics> GetAllPerformanceMetrics()
        {
            yield return TxMerger.GeneralWaitPerformanceMetrics;
            yield return TxMerger.TransactionPerformanceMetrics;
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