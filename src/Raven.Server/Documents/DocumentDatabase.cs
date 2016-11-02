using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.SqlReplication;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Documents.Transformers;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Logging;
using Voron;
using Voron.Impl.Backup;

namespace Raven.Server.Documents
{
    public class DocumentDatabase : IResourceStore
    {
        private readonly Logger _logger;

        private readonly CancellationTokenSource _databaseShutdown = new CancellationTokenSource();

        private readonly object _idleLocker = new object();
        private Task _indexStoreTask;
        private Task _transformerStoreTask;
        private readonly ConfigurationStorage _configurationStorage;
        private long _usages;
        private readonly ManualResetEventSlim _waitForUsagesOnDisposal = new ManualResetEventSlim(false);

        public DocumentDatabase(string name, RavenConfiguration configuration, IoMetrics ioMetrics)
        {
            StartTime = SystemTime.UtcNow;
            Name = name;
            ResourceName = "db/" + name;
            Configuration = configuration;
            _logger = LoggingSource.Instance.GetLogger<DocumentDatabase>(Name);
            Notifications = new DocumentsNotifications();
            DocumentsStorage = new DocumentsStorage(this);
            IndexStore = new IndexStore(this);
            TransformerStore = new TransformerStore(this);
            SqlReplicationLoader = new SqlReplicationLoader(this);
            DocumentReplicationLoader = new DocumentReplicationLoader(this);
            DocumentTombstoneCleaner = new DocumentTombstoneCleaner(this);
            SubscriptionStorage = new SubscriptionStorage(this);
            Operations = new DatabaseOperations(this);
            Metrics = new MetricsCountersManager();
            IoMetrics = ioMetrics;
            Patch = new PatchDocument(this);
            TxMerger = new TransactionOperationsMerger(this, DatabaseShutdown);
            HugeDocuments = new HugeDocuments(configuration.Databases.MaxCollectionSizeHugeDocuments,
                configuration.Databases.MaxWarnSizeHugeDocuments);
            _configurationStorage = new ConfigurationStorage(this);
        }

        public SystemTime Time = new SystemTime();

        public readonly PatchDocument Patch;

        public TransactionOperationsMerger TxMerger;

        public SubscriptionStorage SubscriptionStorage { get; }

        public string Name { get; }

        public Guid DbId => DocumentsStorage.Environment?.DbId ?? Guid.Empty;

        public string ResourceName { get; }

        public RavenConfiguration Configuration { get; }

        public CancellationToken DatabaseShutdown => _databaseShutdown.Token;

        public DocumentsStorage DocumentsStorage { get; private set; }

        public BundleLoader BundleLoader { get; private set; }

        public DocumentTombstoneCleaner DocumentTombstoneCleaner { get; private set; }

        public DocumentsNotifications Notifications { get; }

        public DatabaseOperations Operations { get; private set; }

        public HugeDocuments HugeDocuments { get; }

        public MetricsCountersManager Metrics { get; }

        public IoMetrics IoMetrics { get; }

        public IndexStore IndexStore { get; private set; }

        public TransformerStore TransformerStore { get; }

        public IndexesAndTransformersStorage IndexMetadataPersistence => _configurationStorage.IndexesAndTransformersStorage;

        public AlertsStorage Alerts => _configurationStorage.AlertsStorage;

        public SqlReplicationLoader SqlReplicationLoader { get; private set; }

        public DocumentReplicationLoader DocumentReplicationLoader { get; private set; }

        public ConcurrentSet<TcpConnectionOptions> RunningTcpConnections = new ConcurrentSet<TcpConnectionOptions>();

        public DateTime StartTime { get; }

        public void Initialize()
        {
            DocumentsStorage.Initialize();
            InitializeInternal();
        }

        public void Initialize(StorageEnvironmentOptions options)
        {
            DocumentsStorage.Initialize(options);
            InitializeInternal();
        }

        public DatabaseUsage DatabaseInUse()
        {
            return new DatabaseUsage(this);
        }

        public struct DatabaseUsage : IDisposable
        {
            private readonly DocumentDatabase _parent;

            public DatabaseUsage(DocumentDatabase parent)
            {
                _parent = parent;
                Interlocked.Increment(ref _parent._usages);
                if (_parent._databaseShutdown.IsCancellationRequested)
                {
                    Dispose();
                    ThrowOperationCancelled();
                }
            }

            private void ThrowOperationCancelled()
            {
                throw new OperationCanceledException("The database " + _parent.Name + " is shutting down");
            }


            public void Dispose()
            {
                Interlocked.Decrement(ref _parent._usages);
                if (_parent._databaseShutdown.IsCancellationRequested)
                    _parent._waitForUsagesOnDisposal.Set();

            }
        }

        private void InitializeInternal()
        {
            TxMerger.Start();
            _indexStoreTask = IndexStore.InitializeAsync();
            _transformerStoreTask = TransformerStore.InitializeAsync();
            SqlReplicationLoader.Initialize();
            DocumentReplicationLoader.Initialize();
            DocumentTombstoneCleaner.Initialize();
            BundleLoader = new BundleLoader(this);

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
            _configurationStorage.Initialize();
        }

        public void Dispose()
        {
            _databaseShutdown.Cancel();
            // we'll wait for 1 minute to drain all the requests
            // from the database
            for (int i = 0; i < 60; i++)
            {
                if (Interlocked.Read(ref _usages) == 0)
                    break;
                _waitForUsagesOnDisposal.Wait(100);
            }
            var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(DocumentDatabase)}");

            exceptionAggregator.Execute(() =>
            {
                TxMerger.Dispose();
            });

            exceptionAggregator.Execute(() =>
            {
                DocumentReplicationLoader.Dispose();
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
                DocumentReplicationLoader?.Dispose();
                DocumentReplicationLoader = null;
            });

            exceptionAggregator.Execute(() =>
            {
                SqlReplicationLoader?.Dispose();
                SqlReplicationLoader = null;
            });

            exceptionAggregator.Execute(() =>
            {
                Operations?.Dispose(exceptionAggregator);
                Operations = null;
            });

            exceptionAggregator.Execute(() =>
            {
                SubscriptionStorage?.Dispose();
            });

            exceptionAggregator.Execute(() =>
            {
                _configurationStorage?.Dispose();
            });

            exceptionAggregator.Execute(() =>
            {
                DocumentsStorage?.Dispose();
                DocumentsStorage = null;
            });

            exceptionAggregator.ThrowIfNeeded();
        }

        public void RunIdleOperations()
        {
            if (Monitor.TryEnter(_idleLocker) == false)
                return;

            try
            {
                IndexStore?.RunIdleOperations();
                Operations?.CleanupOperations();
            }

            finally
            {
                Monitor.Exit(_idleLocker);
            }
        }

        public IEnumerable<StorageEnvironment> GetAllStoragesEnvironment()
        {
            // TODO :: more storage environments ?
            yield return DocumentsStorage.Environment;
            yield return SubscriptionStorage.Environment();
            yield return _configurationStorage.Environment;
            foreach (var index in IndexStore.GetIndexes())
            {
                var env = index._indexStorage.Environment();
                if (env != null)
                    yield return env;
            }
        }

        private IEnumerable<FullBackup.StorageEnvironmentInformation> GetAllStoragesEnvironmentInformation()
        {
            yield return (new FullBackup.StorageEnvironmentInformation()
            {
                Name = "",
                Folder = "Subscriptions",
                Env = SubscriptionStorage.Environment()
            });
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
    }
}