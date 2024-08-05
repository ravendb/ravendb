using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents.Changes;
using Raven.Server.Documents.DataArchival;
using Raven.Server.Config.Settings;
using Raven.Server.Dashboard;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.Expiration;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.QueueSink;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Smuggler;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Documents.TransactionMerger;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Utils;
using Raven.Server.Utils.IoMetrics;
using Sparrow;
using Sparrow.Backups;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server;
using Sparrow.Server.Meters;
using Sparrow.Threading;
using Voron;
using Voron.Data.Tables;
using Voron.Exceptions;
using Voron.Impl.Backup;
using Constants = Raven.Client.Constants;
using MountPointUsage = Raven.Client.ServerWide.Operations.MountPointUsage;
using Size = Raven.Client.Util.Size;
using System.Diagnostics.CodeAnalysis;
using Sparrow.Server.Utils;
using Sparrow.Utils;

namespace Raven.Server.Documents
{
    public class DocumentDatabase : IDisposable
    {
        private readonly ServerStore _serverStore;
        private readonly Action<LogMode, string> _addToInitLog;
        private readonly Logger _logger;
        private readonly DisposeOnce<SingleAttempt> _disposeOnce;
        internal TestingStuff ForTestingPurposes;

        private readonly CancellationTokenSource _databaseShutdown;

        private readonly object _idleLocker = new object();

        private readonly SemaphoreSlim _updateValuesLocker = new(1, 1);

        public Action<LogMode, string> AddToInitLog => _addToInitLog;

        /// <summary>
        /// The current lock, used to make sure indexes have a unique names
        /// </summary>
        private Task _indexStoreTask;

        private long _usages;
        private readonly ManualResetEventSlim _waitForUsagesOnDisposal = new ManualResetEventSlim(false);
        private long _lastIdleTicks = DateTime.UtcNow.Ticks;
        private DateTime _nextIoMetricsCleanupTime;
        private long _lastTopologyIndex = -1;
        private long _preventUnloadCounter;

        public string DatabaseGroupId;
        public string ClusterTransactionId;

        private Lazy<RequestExecutor> _proxyRequestExecutor;

        private readonly DatabasesLandlord.StateChange _databaseStateChange;

        public DocumentsCompressionConfiguration DocumentsCompression => _documentsCompression;
        private DocumentsCompressionConfiguration _documentsCompression = new(compressRevisions: false, collections: Array.Empty<string>());
        private HashSet<string> _compressedCollections = new(StringComparer.OrdinalIgnoreCase);

        internal Sparrow.Size _maxTransactionSize = new(16, SizeUnit.Megabytes);

        public void ResetIdleTime()
        {
            _lastIdleTicks = DateTime.MinValue.Ticks;
        }

        public DocumentDatabase(string name, RavenConfiguration configuration, ServerStore serverStore, Action<LogMode, string> addToInitLog)
        {
            Name = name;
            _logger = LoggingSource.Instance.GetLogger<DocumentDatabase>(Name);
            _serverStore = serverStore;
            _addToInitLog = addToInitLog;
            StartTime = Time.GetUtcNow();
            LastAccessTime = Time.GetUtcNow();
            Configuration = configuration;
            _nextIoMetricsCleanupTime = DateTime.UtcNow.Add(Configuration.Storage.IoMetricsCleanupInterval.AsTimeSpan);
            Scripts = new ScriptRunnerCache(this, Configuration);

            Is32Bits = PlatformDetails.Is32Bits || Configuration.Storage.ForceUsing32BitsPager;

            _databaseShutdown = CancellationTokenSource.CreateLinkedTokenSource(serverStore.ServerShutdown);
            _disposeOnce = new DisposeOnce<SingleAttempt>(DisposeInternal);

            _databaseStateChange = new DatabasesLandlord.StateChange(ServerStore, name, _logger, UpdateOnStateChange, 0, _databaseShutdown.Token);

            try
            {
                if (configuration.Initialized == false)
                    throw new InvalidOperationException("Cannot create a new document database instance without initialized configuration");

                if (Configuration.Core.RunInMemory == false)
                {
                    _addToInitLog(LogMode.Information, "Creating db.lock file");
                    _fileLocker = new FileLocker(Configuration.Core.DataDirectory.Combine("db.lock").FullPath);
                    _fileLocker.TryAcquireWriteLock(_logger);

                    var disableFileMarkerPath = Configuration.Core.DataDirectory.Combine("disable.tasks.marker").FullPath;
                    DisableOngoingTasks = File.Exists(disableFileMarkerPath);
                    if (DisableOngoingTasks)
                    {
                        var msg = $"MAINTENANCE WARNING: Found disable.tasks.marker file. All tasks will not start. Please remove the file and restart the '{Name}' database.";
                        _addToInitLog(LogMode.Information, msg);
                        if (_logger.IsOperationsEnabled)
                            _logger.Operations(msg);
                    }
                }

                Smuggler = new DatabaseSmugglerFactory(this);
                QueryMetadataCache = new QueryMetadataCache();
                IoChanges = new IoChangesNotifications
                {
                    DisableIoMetrics = configuration.Storage.EnableIoMetrics == false
                };
                Changes = new DocumentsChanges();
                TombstoneCleaner = new TombstoneCleaner(this);
                DocumentsStorage = CreateDocumentsStorage(addToInitLog);
                CompareExchangeStorage = new CompareExchangeStorage(this);
                IndexStore = CreateIndexStore(serverStore);
                QueryRunner = new QueryRunner(this);
                EtlLoader = new EtlLoader(this, serverStore);
                SubscriptionStorage = CreateSubscriptionStorage(serverStore);
                OngoingTasks = new OngoingTasks.OngoingTasks(this);
                Metrics = new MetricCounters();
                MetricCacher = new DatabaseMetricCacher(this);
                TxMerger = new DocumentsTransactionOperationsMerger(this);
                ConfigurationStorage = new ConfigurationStorage(this);
                NotificationCenter = new DatabaseNotificationCenter(this);
                HugeDocuments = new HugeDocuments(NotificationCenter, configuration.PerformanceHints.HugeDocumentsCollectionSize,
                    configuration.PerformanceHints.HugeDocumentSize.GetValue(SizeUnit.Bytes));
                Operations = new DatabaseOperations(this);
                DatabaseInfoCache = serverStore.DatabaseInfoCache;

                RachisLogIndexNotifications = new DatabaseRaftIndexNotifications(_serverStore.Engine.StateMachine._rachisLogIndexNotifications, DatabaseShutdown);
                ClusterWideTransactionIndexWaiter = new RaftIndexWaiter(DatabaseShutdown);
                CatastrophicFailureNotification = new CatastrophicFailureNotification((environmentId, environmentPath, e, stacktrace) =>
                {
                    serverStore.DatabasesLandlord.CatastrophicFailureHandler.Execute(name, e, environmentId, environmentPath, stacktrace);
                });
                _hasClusterTransaction = new ManualResetEventSlim(false);
                QueueSinkLoader = new QueueSinkLoader(this, serverStore);
                _proxyRequestExecutor = CreateRequestExecutor();
                _serverStore.Server.ServerCertificateChanged += OnCertificateChange;
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        public readonly bool DisableOngoingTasks;

        protected virtual DocumentsStorage CreateDocumentsStorage(Action<LogMode, string> addToInitLog)
        {
            return new DocumentsStorage(this, addToInitLog);
        }

        protected virtual IndexStore CreateIndexStore(ServerStore serverStore)
        {
            return new IndexStore(this, serverStore);
        }

        protected virtual SubscriptionStorage CreateSubscriptionStorage(ServerStore serverStore)
        {
            return new SubscriptionStorage(this, serverStore, Name);
        }

        protected virtual byte[] ReadSecretKey(TransactionOperationContext context) => ServerStore.GetSecretKey(context, Name);

        private void EnsureValidSecretKey()
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                MasterKey = ReadSecretKey(ctx);

                var record = _serverStore.Cluster.ReadRawDatabaseRecord(ctx, Name);
                if (record == null)
                    return;

                var isEncrypted = record.IsEncrypted;
                // can happen when we are in the process of restoring a database
                if (isEncrypted && MasterKey == null)
                    throw new InvalidOperationException($"Attempt to create encrypted db {Name} without supplying the secret key");
                if (isEncrypted == false && MasterKey != null)
                    throw new InvalidOperationException($"Attempt to create a non-encrypted db {Name}, but a secret key exists for this db.");
            }
        }

        public void SetIds(RawDatabaseRecord record) => SetIds(record.Topology, record.Sharding?.ShardedDatabaseId);
        public void SetIds(DatabaseRecord record) => SetIds(record.Topology, record.Sharding?.DatabaseId);

        internal virtual void SetIds(DatabaseTopology topology, string shardedDatabaseId)
        {
            DatabaseGroupId = topology.DatabaseTopologyIdBase64;
            ClusterTransactionId = topology.ClusterTransactionIdBase64;
        }

        public ServerStore ServerStore => _serverStore;

        public RequestExecutor RequestExecutor => _proxyRequestExecutor.Value;

        private bool IsRequestExecutorCreated => _proxyRequestExecutor.IsValueCreated;

        public DateTime LastIdleTime => new DateTime(_lastIdleTicks);

        public DateTime LastAccessTime;

        public DatabaseInfoCache DatabaseInfoCache { get; set; }

        public readonly SystemTime Time = new SystemTime();

        public ScriptRunnerCache Scripts;

        public readonly DocumentsTransactionOperationsMerger TxMerger;

        public SubscriptionStorage SubscriptionStorage { get; }

        public readonly string Name;

        public Guid DbId => DocumentsStorage.Environment?.DbId ?? Guid.Empty;

        public string DbBase64Id => DocumentsStorage.Environment?.Base64Id ?? string.Empty;

        public RavenConfiguration Configuration { get; }

        public QueryRunner QueryRunner { get; }

        public CancellationToken DatabaseShutdown => _databaseShutdown.Token;

        public AsyncManualResetEvent DatabaseShutdownCompleted { get; } = new AsyncManualResetEvent();

        public DocumentsStorage DocumentsStorage { get; private set; }

        public ExpiredDocumentsCleaner ExpiredDocumentsCleaner { get; private set; }

        public DataArchivist DataArchivist { get; private set; }

        public TimeSeriesPolicyRunner TimeSeriesPolicyRunner { get; private set; }

        public PeriodicBackupRunner PeriodicBackupRunner { get; private set; }

        public TombstoneCleaner TombstoneCleaner { get; private set; }

        public DocumentsChanges Changes { get; }

        public IoChangesNotifications IoChanges { get; }

        public CatastrophicFailureNotification CatastrophicFailureNotification { get; }

        public DatabaseNotificationCenter NotificationCenter { get; private set; }

        public DatabaseOperations Operations { get; private set; }

        public HugeDocuments HugeDocuments { get; }

        public MetricCounters Metrics { get; }

        public IndexStore IndexStore { get; private set; }

        public ConfigurationStorage ConfigurationStorage { get; }

        public ReplicationLoader ReplicationLoader { get; internal set; }

        public EtlLoader EtlLoader { get; private set; }

        public QueueSinkLoader QueueSinkLoader { get; private set; }

        public readonly ConcurrentSet<TcpConnectionOptions> RunningTcpConnections = new ConcurrentSet<TcpConnectionOptions>();

        public readonly DateTime StartTime;

        public readonly DatabaseRaftIndexNotifications RachisLogIndexNotifications;

        public byte[] MasterKey { get; private set; }

        private char? _localIdentityPartsSeparator;

        public char IdentityPartsSeparator
        {
            get => _localIdentityPartsSeparator ?? _serverStore.DefaultIdentityPartsSeparator;
        }

        public ClientConfiguration ClientConfiguration { get; private set; }

        public StudioConfiguration StudioConfiguration { get; private set; }

        public CompareExchangeStorage CompareExchangeStorage { get; private set; }

        public OngoingTasks.OngoingTasks OngoingTasks { get; private set; }

        public bool Is32Bits { get; }

        private long _lastValueChangeIndex;

        public long LastValueChangeIndex
        {
            get => Volatile.Read(ref _lastValueChangeIndex);
            private set => _lastValueChangeIndex = value; // we write this always under lock
        }

        public bool CanUnload => Interlocked.Read(ref _preventUnloadCounter) == 0;

        public readonly QueryMetadataCache QueryMetadataCache;

        public long LastTransactionId => DocumentsStorage.Environment.CurrentReadTransactionId;

        public AbstractDatabaseSmugglerFactory Smuggler { get; protected set; }


        protected virtual void InitializeCompareExchangeStorage()
        {
            CompareExchangeStorage.Initialize(Name);
        }

        protected virtual void InitializeAndStartDocumentsMigration()
        {
        }

        protected virtual ReplicationLoader CreateReplicationLoader()
        {
            return new ReplicationLoader(this, _serverStore);
        }

        public void Initialize(InitializeOptions options = InitializeOptions.None, DateTime? wakeup = null)
        {
            try
            {
                EnsureValidSecretKey();

                Configuration.CheckDirectoryPermissions();

                InitializeCompareExchangeStorage();

                _addToInitLog(LogMode.Information, "Initializing NotificationCenter");
                NotificationCenter.Initialize();

                _addToInitLog(LogMode.Information, "Initializing DocumentStorage");
                DocumentsStorage.Initialize((options & InitializeOptions.GenerateNewDatabaseId) == InitializeOptions.GenerateNewDatabaseId);
                _addToInitLog(LogMode.Information, "Starting Transaction Merger");
                TxMerger.Initialize(DocumentsStorage.ContextPool, IsEncrypted, Is32Bits);
                TxMerger.Start();
                _addToInitLog(LogMode.Information, "Initializing ConfigurationStorage");
                ConfigurationStorage.Initialize();

                if ((options & InitializeOptions.SkipLoadingDatabaseRecord) == InitializeOptions.SkipLoadingDatabaseRecord)
                    return;

                _addToInitLog(LogMode.Information, "Loading Database");

                MetricCacher.Initialize();

                long index;
                DatabaseRecord record;

                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                    record = _serverStore.Cluster.ReadDatabase(context, Name, out index);

                if (record == null)
                    DatabaseDoesNotExistException.Throw(Name);

                SetIds(record);
                OnDatabaseRecordChanged(record);

                ReplicationLoader = CreateReplicationLoader();
                PeriodicBackupRunner = new PeriodicBackupRunner(this, _serverStore, wakeup);

                _addToInitLog(LogMode.Information, "Initializing IndexStore (async)");
                _indexStoreTask = IndexStore.InitializeAsync(record, index, _addToInitLog);
                _addToInitLog(LogMode.Information, "Initializing Replication");
                ReplicationLoader?.Initialize(record, index);
                _addToInitLog(LogMode.Information, "Initializing ETL");
                EtlLoader.Initialize(record);
                QueueSinkLoader.Initialize(record);


                InitializeAndStartDocumentsMigration();

                try
                {
                    // we need to wait here for the task to complete
                    // if we will not do that the process will continue
                    // and we will be left with opened files
                    // we are checking cancellation token before each index initialization
                    // so in worst case we will have to wait for 1 index to be opened
                    // if the cancellation is requested during index store initialization
                    _indexStoreTask.Wait();
                }
                finally
                {
                    _indexStoreTask = null;
                }

                DatabaseShutdown.ThrowIfCancellationRequested();

                _addToInitLog(LogMode.Information, "Initializing SubscriptionStorage completed");

                TombstoneCleaner.Start();

                _serverStore.StorageSpaceMonitor.Subscribe(this);

                using (DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var lastCompletedClusterTransactionIndex = DocumentsStorage.ReadLastCompletedClusterTransactionIndex(ctx.Transaction.InnerTransaction);
                    ClusterWideTransactionIndexWaiter.SetAndNotifyListenersIfHigher(lastCompletedClusterTransactionIndex);
                }

                _ = Task.Run(async () =>
                {
                    try
                    {
                        await DatabasesLandlord.NotifyFeaturesAboutStateChangeAsync(record, index, _databaseStateChange);
                        RachisLogIndexNotifications.NotifyListenersAbout(index, e: null);
                    }
                    catch (Exception e)
                    {
                        RachisLogIndexNotifications.NotifyListenersAbout(index, e);
                    }
                });
                var clusterTransactionThreadName = ThreadNames.GetNameToUse(ThreadNames.ForClusterTransactions($"Cluster Transaction Thread {Name}", Name));
                _clusterTransactionsThread = PoolOfThreads.GlobalRavenThreadPool.LongRunning(x =>
                {
                    ThreadHelper.TrySetThreadPriority(ThreadPriority.AboveNormal, clusterTransactionThreadName
                        ,
                        _logger);
                    try
                    {
                        _hasClusterTransaction.Set();
                        ExecuteClusterTransaction();
                    }
                    catch (OperationCanceledException)
                    {
                        // database shutdown
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsInfoEnabled)
                        {
                            _logger.Info("An unhandled exception closed the cluster transaction task", e);
                        }
                    }
                }, null, ThreadNames.ForClusterTransactions(
                    clusterTransactionThreadName,
                    Name));

                _serverStore.LicenseManager.LicenseChanged += LoadTimeSeriesPolicyRunnerConfigurations;
                IoChanges.OnIoChange += CheckWriteRateAndNotifyIfNecessary;
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        public IDisposable PreventFromUnloadingByIdleOperations()
        {
            Interlocked.Increment(ref _preventUnloadCounter);

            return new DisposableAction(() => Interlocked.Decrement(ref _preventUnloadCounter));
        }

        public DatabaseUsage DatabaseInUse(bool skipUsagesCount)
        {
            return new DatabaseUsage(this, skipUsagesCount);
        }

        [DoesNotReturn]
        internal void ThrowDatabaseShutdown(Exception e = null)
        {
            throw CreateDatabaseShutdownException(e);
        }

        internal DatabaseDisabledException CreateDatabaseShutdownException(Exception e = null)
        {
            return new DatabaseDisabledException("The database " + Name + " is shutting down", e);
        }

        private readonly ManualResetEventSlim _hasClusterTransaction;
        public readonly DatabaseMetricCacher MetricCacher;

        public void NotifyOnPendingClusterTransaction()
        {
            _hasClusterTransaction.Set();
        }

        protected long? _nextClusterCommand;
        private long _lastCompletedClusterTransaction;
        public long LastCompletedClusterTransaction => _lastCompletedClusterTransaction;

        public readonly RaftIndexWaiter ClusterWideTransactionIndexWaiter;
        public bool IsEncrypted => MasterKey != null;

        private PoolOfThreads.LongRunningWork _clusterTransactionsThread;
        private int _clusterTransactionDelayOnFailure = 1000;
        private FileLocker _fileLocker;

        private static readonly List<StorageEnvironmentWithType.StorageEnvironmentType> DefaultStorageEnvironmentTypes = new()
        {
            StorageEnvironmentWithType.StorageEnvironmentType.Documents,
            StorageEnvironmentWithType.StorageEnvironmentType.Configuration,
            StorageEnvironmentWithType.StorageEnvironmentType.Index
        };

        private static readonly List<StorageEnvironmentWithType.StorageEnvironmentType> DefaultStorageEnvironmentTypesForBackup = new()
        {
            StorageEnvironmentWithType.StorageEnvironmentType.Index,
            StorageEnvironmentWithType.StorageEnvironmentType.Documents,
            StorageEnvironmentWithType.StorageEnvironmentType.Configuration,
        };

        private void ExecuteClusterTransaction()
        {
            while (DatabaseShutdown.IsCancellationRequested == false)
            {
                var topology = ServerStore.LoadDatabaseTopology(Name);
                if (topology.Promotables.Contains(ServerStore.NodeTag))
                {
                    DatabaseShutdown.WaitHandle.WaitOne(1000);
                    continue;
                }

                //To make sure we mark compare exchange tombstone for cleaning  
                _hasClusterTransaction.Wait(Configuration.Cluster.MaxClusterTransactionCompareExchangeTombstoneCheckInterval.AsTimeSpan, DatabaseShutdown);
                if (DatabaseShutdown.IsCancellationRequested)
                    return;

                _hasClusterTransaction.Reset();

                try
                {
                    using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var batchSize = Configuration.Cluster.MaxClusterTransactionsBatchSize;
                        var executed = ExecuteClusterTransaction(context, batchSize);
                        if (executed.BatchSize != 0)
                        {
                            // We might have more to execute if we read full batch
                            // If we didn't read full batch we may want to update the last completed index for new cluster wide transaction with no database command so we need to open a new read transaction 
                            _hasClusterTransaction.Set();
                        }
                    }
                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info($"Can't perform cluster transaction on database '{Name}'.", e);
                    }
                }
            }
        }
        protected class ClusterTransactionBatchCollector : IDisposable
        {
            private readonly int _maxSize;
            private readonly ClusterTransactionCommand.SingleClusterDatabaseCommand[] _data;
            public int Count = 0;
            public bool AllCommandsBeenProcessed;

            public ClusterTransactionBatchCollector(int maxSize)
            {
                _maxSize = maxSize;
                _data = ArrayPool<ClusterTransactionCommand.SingleClusterDatabaseCommand>.Shared.Rent(maxSize);
            }

            public ArraySegment<ClusterTransactionCommand.SingleClusterDatabaseCommand> GetData()
                => new ArraySegment<ClusterTransactionCommand.SingleClusterDatabaseCommand>(_data, 0, Count);

            public void Add(IEnumerable<ClusterTransactionCommand.SingleClusterDatabaseCommand> data)
            {
                foreach (var command in data)
                {
                    Add(command);
                }
            }

            public void Add(ClusterTransactionCommand.SingleClusterDatabaseCommand command)
            {
                if (Count >= _maxSize)
                    throw new ArgumentOutOfRangeException($"Tried to add beyong the capacity - {nameof(Count)}:{Count}, {nameof(_maxSize)}:{_maxSize}");
                _data[Count++] = command;
            }

            public virtual void Dispose() => ArrayPool<ClusterTransactionCommand.SingleClusterDatabaseCommand>.Shared.Return(_data);
        }

        protected virtual ClusterTransactionBatchCollector CollectCommandsBatch(ClusterOperationContext context, long lastCompletedClusterTransactionIndex, int take)
        {
            var batchCollector = new ClusterTransactionBatchCollector(take);
            var readCommands = ClusterTransactionCommand.ReadCommandsBatch(context, Name, fromCount: _nextClusterCommand, lastCompletedClusterTransactionIndex, take);
            batchCollector.Add(readCommands);
            return batchCollector;
        }

        public (long BatchSize, long CommandsCount) ExecuteClusterTransaction(ClusterOperationContext context, int batchSize)
        {
            using var batchCollector = CollectCommandsBatch(context, ClusterWideTransactionIndexWaiter.LastIndex, batchSize);

            ServerStore.ForTestingPurposes?.BeforeExecuteClusterTransactionBatch?.Invoke(Name, batchCollector.GetData().ToList());

            Stopwatch stopwatch = null;
            try
            {
                if (_logger.IsInfoEnabled)
                {
                    stopwatch = Stopwatch.StartNew();
                    //_nextClusterCommand refers to each individual put/delete while batch size refers to number of transaction (each contains multiple commands)
                    _logger.Info($"Read {batchCollector.Count:#,#;;0} cluster transaction commands - fromCount: {_nextClusterCommand}, take: {batchSize}");
                }

                if (batchCollector.Count == 0)
                {
                    var cmpXchgIndex = CompareExchangeStorage.GetLastCompareExchangeIndex(context);
                    var tombstoneCmpxchgIndex = CompareExchangeStorage.GetLastCompareExchangeTombstoneIndex(context);
                    var index = Math.Max(cmpXchgIndex, tombstoneCmpxchgIndex);
                
                    ClusterWideTransactionIndexWaiter.SetAndNotifyListenersIfHigher(index);
                    return (0, 0);
                }

                var batch = batchCollector.GetData();
                var mergedCommands = new ClusterTransactionMergedCommand(this, batch);

                ForTestingPurposes?.BeforeExecutingClusterTransactions?.Invoke();

                try
                {
                    try
                    {
                        //If we get a database shutdown while we process a cluster tx command this
                        //will cause us to stop running and disposing the context while its memory is still been used by the merger execution
                        TxMerger.EnqueueSync(mergedCommands);
                        batchCollector.AllCommandsBeenProcessed = true;
                    }
                    catch (Exception e) when (_databaseShutdown.IsCancellationRequested == false)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Failed to execute cluster transaction batch (count: {batchCollector.Count}), will retry them one-by-one.", e);

                        if (ExecuteClusterTransactionOneByOne(batch))
                            batchCollector.AllCommandsBeenProcessed = true;
                    }
                }
                catch
                {
                    if (_databaseShutdown.IsCancellationRequested == false)
                        throw;

                    // we got an exception while the database was shutting down
                    // setting it only for commands that we didn't process yet (can only be if we used ExecuteClusterTransactionOneByOne)
                    var exception = CreateDatabaseShutdownException();
                    foreach (var command in batch)
                    {
                        if (command.Processed)
                            continue;

                        OnClusterTransactionCompletion(command, exception);
                    }

                    return (0, 0);
                }

                var commandsCount = 0;
                long maxIndex = 0;
                foreach (var command in batch)
                {
                    commandsCount += command.Commands.Count;
                    OnClusterTransactionCompletion(command, mergedCommands);
                    maxIndex = command.Index;
                }
                ClusterWideTransactionIndexWaiter.SetAndNotifyListenersIfHigher(maxIndex);

                return (batch.Count, commandsCount);
            }
            finally
            {
                if (_logger.IsInfoEnabled && stopwatch != null)
                    _logger.Info($"cluster transaction batch took {stopwatch.Elapsed:c}");
            }
        }

        private bool ExecuteClusterTransactionOneByOne(ArraySegment<ClusterTransactionCommand.SingleClusterDatabaseCommand> batch)
        {
            for (int i = 0; i < batch.Count; i++)
            {
                var command = batch[i];
                var singleCommand = batch.Slice(i, 1);
                var mergedCommand = new ClusterTransactionMergedCommand(this, singleCommand);
                try
                {
                    TxMerger.EnqueueSync(mergedCommand);
                    OnClusterTransactionCompletion(command, mergedCommand);
                    ClusterWideTransactionIndexWaiter.SetAndNotifyListenersIfHigher(command.Index);

                    _clusterTransactionDelayOnFailure = 1000;
                    command.Processed = true;
                }
                catch (Exception e) when (_databaseShutdown.IsCancellationRequested == false)
                {
                    ClusterWideTransactionIndexWaiter.NotifyListenersAboutError(e);
                    OnClusterTransactionCompletion(command, e);
                    NotificationCenter.Add(AlertRaised.Create(
                        Name,
                        "Cluster transaction failed to execute",
                        $"Failed to execute cluster transactions with raft index: {command.Index}. {Environment.NewLine}" +
                        $"With the following document ids involved: {string.Join(", ", command.Commands.Select(item => item.Id))} {Environment.NewLine}" +
                        "Performing cluster transactions on this database will be stopped until the issue is resolved.",
                        AlertType.ClusterTransactionFailure,
                        NotificationSeverity.Error,
                        $"{Name}/ClusterTransaction",
                        new ExceptionDetails(e)));

                    DatabaseShutdown.WaitHandle.WaitOne(_clusterTransactionDelayOnFailure);
                    _clusterTransactionDelayOnFailure = Math.Min(_clusterTransactionDelayOnFailure * 2, 15000);
                    return false;
                }
            }

            return true;
        }

        private void OnClusterTransactionCompletion(ClusterTransactionCommand.SingleClusterDatabaseCommand command, ClusterTransactionMergedCommand mergedCommands)
        {
            try
            {
                var index = command.Index;
                var options = mergedCommands.Options[index];

                ServerStore.Cluster.ClusterTransactionWaiter.TrySetResult(options.TaskId, mergedCommands.ModifiedCollections);

                _nextClusterCommand = command.PreviousCount + command.Commands.Count;
                _lastCompletedClusterTransaction = _nextClusterCommand.Value - 1;
            }
            catch (Exception e)
            {
                // nothing we can do
                if (_logger.IsOperationsEnabled)
                {
                    _logger.Operations($"Failed to notify about transaction completion for database '{Name}'.", e);
                }
            }
        }

        private void OnClusterTransactionCompletion(ClusterTransactionCommand.SingleClusterDatabaseCommand command, Exception exception)
        {
            try
            {
                var index = command.Index;
                var options = command.Options;

                RachisLogIndexNotifications.NotifyListenersAbout(index, exception);
                ServerStore.Cluster.ClusterTransactionWaiter.TrySetException(options.TaskId, exception);
            }
            catch (Exception e)
            {
                // nothing we can do
                if (_logger.IsOperationsEnabled)
                {
                    _logger.Operations($"Failed to notify about transaction completion for database '{Name}'.", e);
                }
            }
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

                if (_parent.IsShutdownRequested())
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

        public void Dispose()
        {
            _disposeOnce.Dispose();
        }

        internal bool IsDisposed => _disposeOnce.DisposedRequested;

        private unsafe void DisposeInternal()
        {
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Starting dispose");

            _databaseShutdown.Cancel();

            _serverStore.Server.ServerCertificateChanged -= OnCertificateChange;

            ForTestingPurposes?.ActionToCallDuringDocumentDatabaseInternalDispose?.Invoke();

            //before we dispose of the database we take its latest info to be displayed in the studio
            try
            {
                ForTestingPurposes?.DisposeLog?.Invoke(Name, "Generating offline database info");

                var databaseInfo = GenerateOfflineDatabaseInfo();
                if (databaseInfo != null)
                {
                    ForTestingPurposes?.DisposeLog?.Invoke(Name, "Inserting offline database info");
                    DatabaseInfoCache?.InsertDatabaseInfo(databaseInfo, Name);
                }
            }
            catch (Exception e)
            {
                ForTestingPurposes?.DisposeLog?.Invoke(Name, $"Generating offline database info failed: {e}");
                // if we encountered a catastrophic failure we might not be able to retrieve database info

                if (_logger.IsInfoEnabled)
                    _logger.Info("Failed to generate and store database info", e);
            }

            if (ForTestingPurposes == null || ForTestingPurposes.SkipDrainAllRequests == false)
            {
                ForTestingPurposes?.DisposeLog?.Invoke(Name, "Draining all requests");

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

                ForTestingPurposes?.DisposeLog?.Invoke(Name, $"Drained all requests. Took: {sp.Elapsed}");
            }

            var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(DocumentDatabase)} {Name}");

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Acquiring cluster lock");

            var lockTaken = _databaseStateChange.Locker.Wait(TimeSpan.FromSeconds(5));

            ForTestingPurposes?.DisposeLog?.Invoke(Name, $"Acquired the update database record lock. Taken: {lockTaken}");

            if (lockTaken == false && _logger.IsOperationsEnabled)
                _logger.Operations("Failed to acquire lock during database dispose for cluster notifications. Will dispose rudely...");

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Unsubscribing from storage space monitor");
            exceptionAggregator.Execute(() =>
            {
                _serverStore.StorageSpaceMonitor.Unsubscribe(this);
            });
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Unsubscribed from storage space monitor");

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing all running TCP connections");
            foreach (var connection in RunningTcpConnections)
            {
                exceptionAggregator.Execute(() =>
                {
                    connection.Dispose();
                });
            }
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposed all running TCP connections");

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing TxMerger");
            exceptionAggregator.Execute(() =>
            {
                TxMerger?.Dispose();
            });
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposed TxMerger");

            ForTestingPurposes?.AfterTxMergerDispose?.Invoke();

            // must acquire the lock in order to prevent concurrent access to index files
            if (lockTaken == false)
            {
                ForTestingPurposes?.DisposeLog?.Invoke(Name, "Acquiring the update database record lock");
                // ReSharper disable once MethodSupportsCancellation
                _databaseStateChange.Locker.Wait();
                ForTestingPurposes?.DisposeLog?.Invoke(Name, "Acquired the update database record lock");
            }

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing the update database record lock");
            exceptionAggregator.Execute(() => _databaseStateChange.Locker.Dispose());

            // To avoid potential deadlocks, it's vital to maintain a consistent lock acquisition order,
            // especially considering the nested locks involved.
            // Specifically, we need to acquire the 'update database record' lock
            // BEFORE obtaining the 'update values' lock.
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Acquiring the update values lock");
            // ReSharper disable once MethodSupportsCancellation
            _updateValuesLocker.Wait();

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing the update values lock");
            exceptionAggregator.Execute(() => _updateValuesLocker.Dispose());

            var indexStoreTask = _indexStoreTask;
            if (indexStoreTask != null)
            {
                ForTestingPurposes?.DisposeLog?.Invoke(Name, "Waiting for index store task to complete");
                exceptionAggregator.Execute(() =>
                {
                    // we need to wait here for the task to complete
                    // if we will not do that the process will continue
                    // and we will be left with opened files
                    // we are checking cancellation token before each index initialization
                    // so in worst case we will have to wait for 1 index to be opened
                    // if the cancellation is requested during index store initialization
                    indexStoreTask.Wait();
                });
                ForTestingPurposes?.DisposeLog?.Invoke(Name, "Finished waiting for index store task to complete");
            }

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing IndexStore");
            exceptionAggregator.Execute(() =>
            {
                IndexStore?.Dispose();
            });
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposed IndexStore");

            DisposeBackgroundWorkers(exceptionAggregator);

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing ReplicationLoader");
            exceptionAggregator.Execute(() =>
            {
                ReplicationLoader?.Dispose();
            });
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposed ReplicationLoader");

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing EtlLoader");
            exceptionAggregator.Execute(() =>
            {
                EtlLoader?.Dispose();
            });
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposed EtlLoader");

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing QueueSinkLoader");
            exceptionAggregator.Execute(() =>
            {
                QueueSinkLoader?.Dispose();
            });
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposed QueueSinkLoader");

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing Operations");
            exceptionAggregator.Execute(() =>
            {
                Operations?.Dispose(exceptionAggregator);
            });
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposed Operations");

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing HugeDocuments");
            exceptionAggregator.Execute(() =>
            {
                HugeDocuments?.Dispose();
            });
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposed HugeDocuments");

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing NotificationCenter");
            exceptionAggregator.Execute(() =>
            {
                NotificationCenter?.Dispose();
            });
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposed NotificationCenter");

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing SubscriptionStorage");
            exceptionAggregator.Execute(() =>
            {
                SubscriptionStorage?.Dispose();
            });
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposed SubscriptionStorage");

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing ConfigurationStorage");
            exceptionAggregator.Execute(() =>
            {
                ConfigurationStorage?.Dispose();
            });
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposed ConfigurationStorage");

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing DocumentsStorage");
            exceptionAggregator.Execute(() =>
            {
                DocumentsStorage?.Dispose();
            });
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposed DocumentsStorage");

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Waiting for cluster transactions executor task to complete");
            exceptionAggregator.Execute(() =>
            {
                var clusterTransactions = _clusterTransactionsThread;
                _clusterTransactionsThread = null;

                if (clusterTransactions != null && PoolOfThreads.LongRunningWork.Current != clusterTransactions)
                    clusterTransactions.Join(int.MaxValue);
            });
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Finished waiting for cluster transactions executor task to complete");

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing _databaseShutdown");
            exceptionAggregator.Execute(() =>
            {
                _databaseShutdown.Dispose();
            });
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposed _databaseShutdown");

            exceptionAggregator.Execute(() =>
            {
                _serverStore.LicenseManager.LicenseChanged -= LoadTimeSeriesPolicyRunnerConfigurations;
            });

            exceptionAggregator.Execute(() =>
            {
                if (IoChanges == null)
                    return;

                IoChanges.OnIoChange -= CheckWriteRateAndNotifyIfNecessary;
            });

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing MasterKey");
            exceptionAggregator.Execute(() =>
            {
                if (MasterKey == null)
                    return;
                fixed (byte* pKey = MasterKey)
                {
                    Sodium.sodium_memzero(pKey, (UIntPtr)MasterKey.Length);
                }
            });
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposed MasterKey");

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing _fileLocker");
            exceptionAggregator.Execute(() => _fileLocker.Dispose());
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposed _fileLocker");

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing RachisLogIndexNotifications");
            exceptionAggregator.Execute(RachisLogIndexNotifications);
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposed RachisLogIndexNotifications");

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing _hasClusterTransaction");
            exceptionAggregator.Execute(_hasClusterTransaction);
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposed _hasClusterTransaction");

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing _proxyRequestExecutor");
            exceptionAggregator.Execute(() =>
            {
                if (_proxyRequestExecutor?.IsValueCreated == true)
                    _proxyRequestExecutor.Value.Dispose();
            });
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposed _proxyRequestExecutor");

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Finished dispose");

            exceptionAggregator.ThrowIfNeeded();
        }

        private void DisposeBackgroundWorkers(ExceptionAggregator exceptionAggregator)
        {
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing ExpiredDocumentsCleaner");
            exceptionAggregator.Execute(() =>
            {
                ExpiredDocumentsCleaner?.Dispose();
            });
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposed ExpiredDocumentsCleaner");

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing PeriodicBackupRunner");
            exceptionAggregator.Execute(() =>
            {
                PeriodicBackupRunner?.Dispose();
            });
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposed PeriodicBackupRunner");

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposing TombstoneCleaner");
            exceptionAggregator.Execute(() =>
            {
                TombstoneCleaner?.Dispose();
            });
            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Disposed TombstoneCleaner");
        }

        public DynamicJsonValue GenerateOfflineDatabaseInfo()
        {
            var envs = GetAllStoragesEnvironment().ToList();
            if (envs.Count == 0 || envs.Any(x => x.Environment == null))
                return null;

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Generating offline database info: sizeOnDisk.");
            var sizeOnDisk = GetSizeOnDisk();

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Generating offline database info: indexingErrors.");
            var indexingErrors = IndexStore.GetIndexes().Sum(index => index.GetErrorCount());

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Generating offline database info: alertCount.");
            var alertCount = NotificationCenter.GetAlertCount();

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Generating offline database info: performanceHints.");
            var performanceHints = NotificationCenter.GetPerformanceHintCount();

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Generating offline database info: backupInfo.");
            var backupInfo = PeriodicBackupRunner?.GetBackupInfo();

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Generating offline database info: mountPointsUsage.");
            var mountPointsUsage = GetMountPointsUsage(includeTempBuffers: false)
                .Select(x => x.ToJson())
                .ToList();

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Generating offline database info: documentsCount.");
            var documentsCount = DocumentsStorage.GetNumberOfDocuments();

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Generating offline database info: indexesCount.");
            var indexesCount = IndexStore.GetIndexes().Count();

            ForTestingPurposes?.DisposeLog?.Invoke(Name, "Generating offline database info: indexesStatus.");
            var indexesStatus = IndexStore.Status.ToString();

            var databaseInfo = new DynamicJsonValue
            {
                [nameof(ExtendedDatabaseInfo.HasRevisionsConfiguration)] = DocumentsStorage.RevisionsStorage.Configuration != null,
                [nameof(ExtendedDatabaseInfo.HasExpirationConfiguration)] = (ExpiredDocumentsCleaner?.ExpirationConfiguration?.Disabled ?? true) == false,
                [nameof(ExtendedDatabaseInfo.HasRefreshConfiguration)] = (ExpiredDocumentsCleaner?.RefreshConfiguration?.Disabled ?? true) == false,
                [nameof(ExtendedDatabaseInfo.HasDataArchivalConfiguration)] = (DataArchivist?.DataArchivalConfiguration?.Disabled ?? true) == false,
                [nameof(ExtendedDatabaseInfo.IsAdmin)] = true, //TODO: implement me!
                [nameof(ExtendedDatabaseInfo.IsEncrypted)] = DocumentsStorage.Environment.Options.Encryption.IsEnabled,
                [nameof(ExtendedDatabaseInfo.Name)] = Name,
                [nameof(ExtendedDatabaseInfo.Disabled)] = false, //TODO: this value should be overwritten by the studio since it is cached
                [nameof(ExtendedDatabaseInfo.TotalSize)] = new DynamicJsonValue
                {
                    [nameof(Size.HumaneSize)] = sizeOnDisk.Data.HumaneSize,
                    [nameof(Size.SizeInBytes)] = sizeOnDisk.Data.SizeInBytes
                },
                [nameof(ExtendedDatabaseInfo.TempBuffersSize)] = new DynamicJsonValue
                {
                    [nameof(Size.HumaneSize)] = "0 Bytes",
                    [nameof(Size.SizeInBytes)] = 0
                },
                [nameof(ExtendedDatabaseInfo.IndexingErrors)] = indexingErrors,
                [nameof(ExtendedDatabaseInfo.Alerts)] = alertCount,
                [nameof(ExtendedDatabaseInfo.PerformanceHints)] = performanceHints,
                [nameof(ExtendedDatabaseInfo.UpTime)] = null, //it is shutting down
                [nameof(ExtendedDatabaseInfo.BackupInfo)] = backupInfo,
                [nameof(ExtendedDatabaseInfo.MountPointsUsage)] = new DynamicJsonArray(mountPointsUsage),
                [nameof(ExtendedDatabaseInfo.DocumentsCount)] = documentsCount,
                [nameof(ExtendedDatabaseInfo.IndexesCount)] = indexesCount,
                [nameof(ExtendedDatabaseInfo.RejectClients)] = false, //TODO: implement me!
                [nameof(ExtendedDatabaseInfo.IndexingStatus)] = indexesStatus,
                ["CachedDatabaseInfo"] = true
            };
            return databaseInfo;
        }

        public DatabaseSummary GetDatabaseSummary()
        {
            using (DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext transactionContext))
            using (documentsContext.OpenReadTransaction())
            using (transactionContext.OpenReadTransaction())
            {
                return new DatabaseSummary
                {
                    DocumentsCount = DocumentsStorage.GetNumberOfDocuments(documentsContext),
                    AttachmentsCount = DocumentsStorage.AttachmentsStorage.GetNumberOfAttachments(documentsContext).AttachmentCount,
                    RevisionsCount = DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(documentsContext),
                    ConflictsCount = DocumentsStorage.ConflictsStorage.GetNumberOfConflicts(documentsContext),
                    CounterEntriesCount = DocumentsStorage.CountersStorage.GetNumberOfCounterEntries(documentsContext),
                    CompareExchangeCount = ServerStore.Cluster.GetNumberOfCompareExchange(transactionContext, DocumentsStorage.DocumentDatabase.Name),
                    CompareExchangeTombstonesCount = ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(transactionContext, DocumentsStorage.DocumentDatabase.Name),
                    IdentitiesCount = ServerStore.Cluster.GetNumberOfIdentities(transactionContext, DocumentsStorage.DocumentDatabase.Name),
                    TimeSeriesSegmentsCount = DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesSegments(documentsContext)
                };
            }
        }

        public long ReadLastEtag()
        {
            using (DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
            using (var tx = documentsContext.OpenReadTransaction())
            {
                return DocumentsStorage.ReadLastEtag(tx.InnerTransaction);
            }
        }

        public (long Etag, string ChangeVector) ReadLastEtagAndChangeVector()
        {
            using (DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
            using (var tx = documentsContext.OpenReadTransaction())
            {
                return (DocumentsStorage.ReadLastEtag(tx.InnerTransaction), DocumentsStorage.GetDatabaseChangeVector(tx.InnerTransaction));
            }
        }

        public void SetChangeVector(string changeVector)
        {
            using (DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                DocumentsStorage.SetDatabaseChangeVector(context, context.GetChangeVector(changeVector));
                tx.Commit();
            }
        }

        public void RunIdleOperations(DatabaseCleanupMode mode = DatabaseCleanupMode.Regular)
        {
            Debug.Assert(mode != DatabaseCleanupMode.None, "mode != CleanupMode.None");
            if (mode == DatabaseCleanupMode.None)
                return;

            if (Monitor.TryEnter(_idleLocker) == false)
                return;

            try
            {
                var sp = Stopwatch.StartNew();
                var utcNow = DateTime.UtcNow;

                _lastIdleTicks = utcNow.Ticks;
                IndexStore?.RunIdleOperations(mode);
                Operations?.CleanupOperations();
                SubscriptionStorage?.CleanupSubscriptions();

                Scripts?.RunIdleOperations();
                DocumentsStorage.Environment.Cleanup();
                ConfigurationStorage.Environment.Cleanup();

                if (utcNow >= _nextIoMetricsCleanupTime)
                {
                    IoMetricsUtil.CleanIoMetrics(GetAllStoragesEnvironment(), _nextIoMetricsCleanupTime.Ticks);
                    _nextIoMetricsCleanupTime = utcNow.Add(Configuration.Storage.IoMetricsCleanupInterval.AsTimeSpan);
                }

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Ran idle operations for database '{Name}' in {mode} mode, took: {sp.ElapsedMilliseconds}ms");
            }
            finally
            {
                Monitor.Exit(_idleLocker);
            }
        }

        public IEnumerable<StorageEnvironmentWithType> GetAllStoragesEnvironment(List<StorageEnvironmentWithType.StorageEnvironmentType> types = null)
        {
            types ??= DefaultStorageEnvironmentTypes;

            // TODO :: more storage environments ?
            foreach (var type in types)
            {
                switch (type)
                {
                    case StorageEnvironmentWithType.StorageEnvironmentType.Documents:
                        var documentsStorage = DocumentsStorage;
                        if (documentsStorage != null)
                            yield return
                                new StorageEnvironmentWithType(Name, StorageEnvironmentWithType.StorageEnvironmentType.Documents,
                                    documentsStorage.Environment);
                        break;

                    case StorageEnvironmentWithType.StorageEnvironmentType.Configuration:
                        var configurationStorage = ConfigurationStorage;
                        if (configurationStorage != null)
                            yield return
                                new StorageEnvironmentWithType("Configuration",
                                    StorageEnvironmentWithType.StorageEnvironmentType.Configuration, configurationStorage.Environment);
                        break;

                    case StorageEnvironmentWithType.StorageEnvironmentType.Index:
                        //check for null to prevent NRE when disposing the DocumentDatabase
                        foreach (var index in (IndexStore?.GetIndexes()).EmptyIfNull())
                        {
                            var env = index?._indexStorage?.Environment();
                            if (env != null)
                                yield return
                                    new StorageEnvironmentWithType(index.Name,
                                        StorageEnvironmentWithType.StorageEnvironmentType.Index, env)
                                    {
                                        LastIndexQueryTime = index.GetLastQueryingTime()
                                    };
                        }
                        break;
                }
            }
        }

        private IEnumerable<FullBackup.StorageEnvironmentInformation> GetAllStoragesForBackup(bool excludeIndexes)
        {
            foreach (var storageEnvironmentWithType in GetAllStoragesEnvironment(DefaultStorageEnvironmentTypesForBackup))
            {
                switch (storageEnvironmentWithType.Type)
                {
                    case StorageEnvironmentWithType.StorageEnvironmentType.Documents:
                        ForTestingPurposes?.BeforeSnapshotOfDocuments?.Invoke();

                        yield return new FullBackup.StorageEnvironmentInformation
                        {
                            Name = string.Empty,
                            Folder = Constants.Documents.PeriodicBackup.Folders.Documents,
                            Env = storageEnvironmentWithType.Environment
                        };

                        ForTestingPurposes?.AfterSnapshotOfDocuments?.Invoke();

                        break;

                    case StorageEnvironmentWithType.StorageEnvironmentType.Index:
                        if (excludeIndexes)
                            break;

                        yield return new FullBackup.StorageEnvironmentInformation
                        {
                            Name = IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(storageEnvironmentWithType.Name),
                            Folder = Constants.Documents.PeriodicBackup.Folders.Indexes,
                            Env = storageEnvironmentWithType.Environment
                        };
                        break;

                    case StorageEnvironmentWithType.StorageEnvironmentType.Configuration:
                        yield return new FullBackup.StorageEnvironmentInformation
                        {
                            Name = string.Empty,
                            Folder = Constants.Documents.PeriodicBackup.Folders.Configuration,
                            Env = storageEnvironmentWithType.Environment
                        };
                        break;

                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
        }

        public SmugglerResult FullBackupTo(Stream stream, SnapshotBackupCompressionAlgorithm compressionAlgorithm, CompressionLevel compressionLevel = CompressionLevel.Optimal,
            bool excludeIndexes = false, Action<(string Message, int FilesCount)> infoNotify = null, CancellationToken cancellationToken = default)
        {
            SmugglerResult smugglerResult;

            long lastTombstoneEtag = 0;
            using (DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                lastTombstoneEtag = DocumentsStorage.ReadLastTombstoneEtag(ctx.Transaction.InnerTransaction);
            }

            using (TombstoneCleaner.PreventTombstoneCleaningUpToEtag(lastTombstoneEtag))
            using (var zipArchive = new ZipArchive(stream, ZipArchiveMode.Create, leaveOpen: true))
            {
                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
                {
                    // the smuggler output is already compressed
                    var zipArchiveEntry = zipArchive.CreateEntry(RestoreSettings.SmugglerValuesFileName);
                    using (var zipStream = zipArchiveEntry.Open())
                    using (var outputStream = GetOutputStream(zipStream))
                    {
                        var smugglerSource = new DatabaseSource(this, 0, 0, _logger);
                        using (DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                        {
                            var smugglerDestination = new StreamDestination(outputStream, context, smugglerSource, compressionAlgorithm.ToExportCompressionAlgorithm(), compressionLevel);
                            var databaseSmugglerOptionsServerSide = new DatabaseSmugglerOptionsServerSide(AuthorizationStatus.DatabaseAdmin)
                            {
                                OperateOnTypes = DatabaseItemType.CompareExchange | DatabaseItemType.Identities
                            };

                            var smuggler = Smuggler.Create(
                                smugglerSource,
                                smugglerDestination,
                                context,
                                databaseSmugglerOptionsServerSide,
                                result: null,
                                onProgress: null,
                                token: cancellationToken);

                            smugglerResult = smuggler.ExecuteAsync().Result;
                        }

                        outputStream.Flush();
                    }

                    infoNotify?.Invoke(("Backed up Database Record", 1));

                    var package = new BackupZipArchive(zipArchive, compressionAlgorithm, compressionLevel);
                    var settingsEntry = package.CreateEntry(RestoreSettings.SettingsFileName);
                    using (var zipStream = settingsEntry.Open())
                    using (var outputStream = GetOutputStream(zipStream))
                    using (var writer = new BlittableJsonTextWriter(serverContext, outputStream))
                    {
                        writer.WriteStartObject();

                        // read and save the database record
                        writer.WritePropertyName(nameof(RestoreSettings.DatabaseRecord));
                        using (serverContext.OpenReadTransaction())
                        using (var databaseRecord = _serverStore.Cluster.ReadRawDatabaseRecord(serverContext, Name, out _))
                        {
                            serverContext.Write(writer, databaseRecord.Raw);
                        }

                        // save the database values (subscriptions, periodic backups statuses, etl states...)
                        writer.WriteComma();
                        writer.WritePropertyName(nameof(RestoreSettings.DatabaseValues));
                        writer.WriteStartObject();

                        var first = true;
                        var prefix = Helpers.ClusterStateMachineValuesPrefix(Name);

                        using (serverContext.OpenReadTransaction())
                        {
                            foreach (var keyValue in ClusterStateMachine.ReadValuesStartingWith(serverContext, prefix))
                            {
                                cancellationToken.ThrowIfCancellationRequested();

                                if (first == false)
                                    writer.WriteComma();

                                first = false;

                                var key = keyValue.Key.ToString().Substring(prefix.Length);
                                writer.WritePropertyName(key);
                                serverContext.Write(writer, keyValue.Value);
                            }
                        }

                        writer.WriteEndObject();
                        // end of values

                        writer.WriteEndObject();
                        writer.Flush();
                        outputStream.Flush();
                    }
                }

                infoNotify?.Invoke(("Backed up database values", 1));

                BackupMethods.Full.ToFile(GetAllStoragesForBackup(excludeIndexes), zipArchive, compressionAlgorithm, compressionLevel, infoNotify: infoNotify, cancellationToken: cancellationToken);
            }

            return smugglerResult;
        }

        public Stream GetOutputStream(Stream fileStream)
        {
            if (MasterKey == null)
                return fileStream;

            var encryptingStream = new EncryptingXChaCha20Poly1305Stream(fileStream, MasterKey);

            encryptingStream.Initialize();

            return encryptingStream;
        }

        /// <summary>
        /// this event is intended for entities that are not singletons
        /// per database and still need to be informed on changes to the database record.
        /// </summary>
        public event Action<DatabaseRecord> DatabaseRecordChanged;

        public async ValueTask ValueChangedAsync(long index, string type, object changeState)
        {
            try
            {
                if (_databaseShutdown.IsCancellationRequested)
                    ThrowDatabaseShutdown();

                await NotifyFeaturesAboutValueChangeAsync(index, type, changeState);
                RachisLogIndexNotifications.NotifyListenersAbout(index, e: null);
            }
            catch (Exception e)
            {
                RachisLogIndexNotifications.NotifyListenersAbout(index, e);

                if (_databaseShutdown.IsCancellationRequested)
                    ThrowDatabaseShutdown();

                throw;
            }
        }

        public async ValueTask StateChangedAsync(long index)
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

                if (ServerStore.ShouldUpdateTopology(record.Topology.Stamp.Index, _lastTopologyIndex, out string url))
                {
                    _lastTopologyIndex = record.Topology.Stamp.Index;

                    Changes.RaiseNotifications(new TopologyChange
                    {
                        Url = url,
                        Database = Name
                    });

                    if (IsRequestExecutorCreated)
                    {
                        _ = RequestExecutor.UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(new ServerNode()
                        {
                            ClusterTag = _serverStore.NodeTag,
                            Database = Name,
                            Url = ServerStore.GetNodeHttpServerUrl()
                        })
                        {
                            DebugTag = "database-topology-update"
                        });
                    }
                }

                ClientConfiguration = record.Client;
                _localIdentityPartsSeparator = record.Client is { Disabled: false, IdentityPartsSeparator: not null }
                    ? record.Client.IdentityPartsSeparator
                    : null;
                StudioConfiguration = record.Studio;

                ServerStore.DatabasesLandlord.ForTestingPurposes?.DelayNotifyFeaturesAboutStateChange?.Invoke();

                await DatabasesLandlord.NotifyFeaturesAboutStateChangeAsync(record, index, _databaseStateChange);

                RachisLogIndexNotifications.NotifyListenersAbout(index, e: null);
            }
            catch (Exception e)
            {
                DatabaseDisabledException throwShutDown = null;

                if (_databaseShutdown.IsCancellationRequested && e is DatabaseDisabledException == false)
                    e = throwShutDown = CreateDatabaseShutdownException(e);

                RachisLogIndexNotifications.NotifyListenersAbout(index, e);

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Got exception during StateChanged({index}).", e);

                if (throwShutDown != null)
                    throw throwShutDown;

                throw;
            }
        }

        public async Task UpdateOnStateChange(DatabaseRecord record, long index)
        {
            SetIds(record);
            SetUnusedDatabaseIds(record);
            InitializeFromDatabaseRecord(record);
            IndexStore.HandleDatabaseRecordChange(record, index);
            ReplicationLoader?.HandleDatabaseRecordChange(record, index);


            // We've already begun executing the operation and aim to see it through without interruption,
            // hence we won't be passing the cancellation token.
            // ReSharper disable once MethodSupportsCancellation
            await _updateValuesLocker.WaitAsync();

            try
            {
                PeriodicBackupRunner?.UpdateConfigurations(record.PeriodicBackups);
                EtlLoader?.HandleDatabaseRecordChange(record);
                SubscriptionStorage?.HandleDatabaseRecordChange();
            }
            finally
            {
                _updateValuesLocker.Release();
            }

            QueueSinkLoader?.HandleDatabaseRecordChange(record);

            OnDatabaseRecordChanged(record);
        }

        private void SetUnusedDatabaseIds(DatabaseRecord record)
        {
            if (record.UnusedDatabaseIds == null && DocumentsStorage.UnusedDatabaseIds == null)
                return;

            if (record.UnusedDatabaseIds == null || DocumentsStorage.UnusedDatabaseIds == null)
            {
                Interlocked.Exchange(ref DocumentsStorage.UnusedDatabaseIds, record.UnusedDatabaseIds);
                return;
            }

            if (DocumentsStorage.UnusedDatabaseIds.SetEquals(record.UnusedDatabaseIds) == false)
            {
                Interlocked.Exchange(ref DocumentsStorage.UnusedDatabaseIds, record.UnusedDatabaseIds);
            }
        }

        private bool CanSkipValueChange(long index, string type)
        {
            switch (type)
            {
                case nameof(UpdateResponsibleNodeForTasksCommand):
                case nameof(DelayBackupCommand):
                    // both commands cannot be skipped and must be executed
                    return false;
            }

            if (LastValueChangeIndex > index)
            {
                // index and LastDatabaseRecordIndex could have equal values when we transit from/to passive and want to update the tasks.
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Skipping value change for index {index} (current {LastValueChangeIndex}) for {Name} because it was already precessed.");
                return true;
            }

            return false;
        }

        private async ValueTask NotifyFeaturesAboutValueChangeAsync(long index, string type, object changeState)
        {
            if (CanSkipValueChange(index, type))
                return;

            var taken = false;
            while (taken == false)
            {
                taken = await _updateValuesLocker.WaitAsync(TimeSpan.FromSeconds(5), DatabaseShutdown);

                try
                {
                    if (CanSkipValueChange(index, type))
                        return;

                    DatabaseShutdown.ThrowIfCancellationRequested();

                    if (taken == false)
                        continue;

                    SubscriptionStorage?.HandleDatabaseRecordChange();
                    EtlLoader?.HandleDatabaseValueChanged();
                    PeriodicBackupRunner?.HandleDatabaseValueChanged(type, changeState);

                    LastValueChangeIndex = index;
                }
                finally
                {
                    if (taken)
                        _updateValuesLocker.Release();
                }
            }
        }

        public ValueTask RefreshFeaturesAsync()
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

            return DatabasesLandlord.NotifyFeaturesAboutStateChangeAsync(record, index, _databaseStateChange);
        }

        private void InitializeFromDatabaseRecord(DatabaseRecord record)
        {
            if (record == null || DocumentsStorage == null)
                return;

            ClientConfiguration = record.Client;
            StudioConfiguration = record.Studio;
            InitializeCompressionFromDatabaseRecord(record);
            DocumentsStorage.RevisionsStorage.InitializeFromDatabaseRecord(record);
            ExpiredDocumentsCleaner = ExpiredDocumentsCleaner.LoadConfigurations(this, record, ExpiredDocumentsCleaner);
            DataArchivist = DataArchivist.LoadConfiguration(this, record, DataArchivist);
            TimeSeriesPolicyRunner = TimeSeriesPolicyRunner.LoadConfigurations(this, record, TimeSeriesPolicyRunner);
            UpdateCompressionConfigurationFromDatabaseRecord(record);
        }

        public void InitializeCompressionFromDatabaseRecord(DatabaseRecord record)
        {
            if (_documentsCompression.Equals(record.DocumentsCompression))
                return;

            if (record.DocumentsCompression == null) // legacy configurations
            {
                _compressedCollections.Clear();
                _documentsCompression = new DocumentsCompressionConfiguration(false);
                return;
            }

            _documentsCompression = record.DocumentsCompression;
            _compressedCollections = new HashSet<string>(record.DocumentsCompression.Collections, StringComparer.OrdinalIgnoreCase);
        }

        public TableSchema GetDocsSchemaForCollection(CollectionName collection, DocumentFlags flags)
        {
            return flags.HasFlag(DocumentFlags.Archived) ? DocumentsStorage.CompressedDocsSchema : GetDocsSchemaForCollection(collection);
        }

        private void LoadTimeSeriesPolicyRunnerConfigurations()
        {
            LicenseLimitWarning.DismissLicenseLimitNotification(_serverStore.NotificationCenter, LimitType.TimeSeriesRollupsAndRetention);

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var record = _serverStore.Cluster.ReadDatabase(context, Name, out _);
                TimeSeriesPolicyRunner = TimeSeriesPolicyRunner.LoadConfigurations(this, record, TimeSeriesPolicyRunner);
            }
        }

        public IEnumerable<DatabasePerformanceMetrics> GetAllPerformanceMetrics()
        {
            yield return TxMerger.GeneralWaitPerformanceMetrics;
            yield return TxMerger.TransactionPerformanceMetrics;
        }

        protected virtual void OnDatabaseRecordChanged(DatabaseRecord record)
        {
            DatabaseRecordChanged?.Invoke(record);
        }

        public bool HasTopologyChanged(long index)
        {
            // only if have a newer topology index
            return _lastTopologyIndex > index;
        }

        public (Size Data, Size TempBuffers) GetSizeOnDisk()
        {
            var storageEnvironments = GetAllStoragesEnvironment();
            if (storageEnvironments == null)
                return (new Size(0), new Size(0));

            long dataInBytes = 0;
            long tempBuffersInBytes = 0;
            foreach (var environment in storageEnvironments)
            {
                if (environment == null)
                    continue;

                var sizeOnDisk = environment.Environment.GenerateSizeReport(includeTempBuffers: true);
                dataInBytes += sizeOnDisk.DataFileInBytes + sizeOnDisk.JournalsInBytes;
                tempBuffersInBytes += sizeOnDisk.TempBuffersInBytes + sizeOnDisk.TempRecyclableJournalsInBytes;
            }

            return (new Size(dataInBytes), new Size(tempBuffersInBytes));
        }

        public IEnumerable<MountPointUsage> GetMountPointsUsage(bool includeTempBuffers)
        {
            var storageEnvironments = GetAllStoragesEnvironment();
            if (storageEnvironments == null)
                yield break;

            foreach (var environment in storageEnvironments)
            {
                foreach (var mountPoint in ServerStore.GetMountPointUsageDetailsFor(environment, includeTempBuffers: includeTempBuffers))
                {
                    yield return mountPoint;
                }
            }
        }

        public DatabaseRecord ReadDatabaseRecord()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return ServerStore.Cluster.ReadDatabase(context, Name);
            }
        }

        private void OnCertificateChange(object sender, EventArgs e)
        {
            if (_proxyRequestExecutor.IsValueCreated == false)
                return;

            using (_proxyRequestExecutor.Value)
                _proxyRequestExecutor = CreateRequestExecutor();
        }

        public TableSchema GetDocsSchemaForCollection(CollectionName collection) =>
            _documentsCompression.CompressAllCollections || _compressedCollections.Contains(collection.Name)
                ? DocumentsStorage.CompressedDocsSchema
                : DocumentsStorage.DocsSchema;

        private void UpdateCompressionConfigurationFromDatabaseRecord(DatabaseRecord record)
        {
            if (_documentsCompression.Equals(record.DocumentsCompression))
                return;

            if (record.DocumentsCompression == null) // legacy configurations
            {
                _compressedCollections.Clear();
                _documentsCompression = new DocumentsCompressionConfiguration(false);
                return;
            }

            _documentsCompression = record.DocumentsCompression;
            _compressedCollections = new HashSet<string>(record.DocumentsCompression.Collections, StringComparer.OrdinalIgnoreCase);
        }

        private Lazy<RequestExecutor> CreateRequestExecutor() =>
            new(
                () => RequestExecutor.CreateForProxy(new[] { ServerStore.Configuration.Core.GetNodeHttpServerUrl(ServerStore.Server.WebUrl) }, Name,
                    ServerStore.Server.Certificate.Certificate, DocumentConventions.DefaultForServer), LazyThreadSafetyMode.ExecutionAndPublication);

        internal void HandleNonDurableFileSystemError(object sender, NonDurabilitySupportEventArgs e)
        {
            string title = $"Non Durable File System - {Name ?? "Unknown Database"}";

            if (_logger.IsOperationsEnabled)
                _logger.Operations($"{title}. {e.Message}", e.Exception);

            _serverStore?.NotificationCenter.Add(AlertRaised.Create(
                Name,
                title,
                e.Message,
                AlertType.NonDurableFileSystem,
                NotificationSeverity.Warning,
                Name,
                details: new MessageDetails { Message = e.Details }));
        }

        internal void HandleOnDatabaseRecoveryError(object sender, RecoveryErrorEventArgs e)
        {
            HandleOnRecoveryError(StorageEnvironmentWithType.StorageEnvironmentType.Documents, Name, sender, e);
        }

        internal void HandleOnConfigurationRecoveryError(object sender, RecoveryErrorEventArgs e)
        {
            HandleOnRecoveryError(StorageEnvironmentWithType.StorageEnvironmentType.Configuration, Name, sender, e);
        }

        internal void HandleOnIndexRecoveryError(string indexName, object sender, RecoveryErrorEventArgs e)
        {
            HandleOnRecoveryError(StorageEnvironmentWithType.StorageEnvironmentType.Index, indexName, sender, e);
        }

        private void HandleOnRecoveryError(StorageEnvironmentWithType.StorageEnvironmentType type, string resourceName, object environment, RecoveryErrorEventArgs e)
        {
            AbstractNotificationCenter nc;
            string title;

            switch (type)
            {
                case StorageEnvironmentWithType.StorageEnvironmentType.Configuration:
                case StorageEnvironmentWithType.StorageEnvironmentType.Documents:
                    nc = _serverStore?.NotificationCenter;
                    title = $"Database Recovery Error - {resourceName ?? "Unknown Database"}";

                    if (type == StorageEnvironmentWithType.StorageEnvironmentType.Configuration)
                        title += " (configuration storage)";
                    break;

                case StorageEnvironmentWithType.StorageEnvironmentType.Index:
                    nc = NotificationCenter;
                    title = $"Index Recovery Error - {resourceName ?? "Unknown Index"}";
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type.ToString());
            }

            string message = $"{e.Message}{Environment.NewLine}{Environment.NewLine}Environment: {environment}";

            if (_logger.IsOperationsEnabled)
                _logger.Operations($"{title}. {message}", e.Exception);

            nc?.Add(AlertRaised.Create(Name,
                title,
                message,
                AlertType.RecoveryError,
                NotificationSeverity.Error,
                key: $"{resourceName}/{SystemTime.UtcNow.Ticks % 5}")); // if this was called multiple times let's try to not overwrite previous alerts
        }

        internal void HandleOnDatabaseIntegrityErrorOfAlreadySyncedData(object sender, DataIntegrityErrorEventArgs e)
        {
            HandleOnIntegrityErrorOfAlreadySyncedData(StorageEnvironmentWithType.StorageEnvironmentType.Documents, Name, sender, e);
        }

        internal void HandleOnConfigurationIntegrityErrorOfAlreadySyncedData(object sender, DataIntegrityErrorEventArgs e)
        {
            HandleOnIntegrityErrorOfAlreadySyncedData(StorageEnvironmentWithType.StorageEnvironmentType.Configuration, Name, sender, e);
        }

        internal void HandleOnIndexIntegrityErrorOfAlreadySyncedData(string indexName, object sender, DataIntegrityErrorEventArgs e)
        {
            HandleOnIntegrityErrorOfAlreadySyncedData(StorageEnvironmentWithType.StorageEnvironmentType.Index, indexName, sender, e);
        }

        private void HandleOnIntegrityErrorOfAlreadySyncedData(StorageEnvironmentWithType.StorageEnvironmentType type, string resourceName, object environment, DataIntegrityErrorEventArgs e)
        {
            AbstractNotificationCenter nc;
            string title;

            switch (type)
            {
                case StorageEnvironmentWithType.StorageEnvironmentType.Configuration:
                case StorageEnvironmentWithType.StorageEnvironmentType.Documents:
                    nc = _serverStore?.NotificationCenter;
                    title = $"Integrity error of already synced data - {resourceName ?? "Unknown Database"}";

                    if (type == StorageEnvironmentWithType.StorageEnvironmentType.Configuration)
                        title += " (configuration storage)";
                    break;

                case StorageEnvironmentWithType.StorageEnvironmentType.Index:
                    nc = NotificationCenter;
                    title = $"Integrity error of already synced index data - {resourceName ?? "Unknown Index"}";
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type.ToString());
            }

            string message = $"{e.Message}{Environment.NewLine}{Environment.NewLine}Environment: {environment}";

            if (_logger.IsOperationsEnabled)
                _logger.Operations($"{title}. {message}", e.Exception);

            nc?.Add(AlertRaised.Create(Name,
                title,
                message,
                AlertType.IntegrityErrorOfAlreadySyncedData,
                NotificationSeverity.Warning,
                key: $"{resourceName}/{SystemTime.UtcNow.Ticks % 5}")); // if this was called multiple times let's try to not overwrite previous alerts
        }

        internal void HandleRecoverableFailure(object sender, RecoverableFailureEventArgs e)
        {
            var title = $"Recoverable Voron error in '{Name}' database";
            var message = $"Failure {e.FailureMessage} in the following environment: {e.EnvironmentPath}";

            if (_logger.IsOperationsEnabled)
                _logger.Operations($"{title}. {message}", e.Exception);

            try
            {
                _serverStore.NotificationCenter.Add(AlertRaised.Create(
                    Name,
                    title,
                    message,
                    AlertType.RecoverableVoronFailure,
                    NotificationSeverity.Warning,
                    key: e.EnvironmentId.ToString(),
                    details: new ExceptionDetails(e.Exception)));
            }
            catch (Exception)
            {
                // exception in raising an alert can't prevent us from unloading a database
            }
        }

        public void CheckWriteRateAndNotifyIfNecessary(IoChange ioChange)
        {
            switch (ioChange.MeterItem.Type)
            {
                case IoMetrics.MeterType.Compression:
                    return; // In-memory operation, no action required.

                case IoMetrics.MeterType.JournalWrite:
                    if (ioChange.MeterItem.Duration.TotalMilliseconds < 500)
                        return;
                    break;

                case IoMetrics.MeterType.DataFlush:
                case IoMetrics.MeterType.DataSync:
                    if (ioChange.MeterItem.Duration.TotalMilliseconds < 120_000)
                        return;
                    break;
            }

            if (ioChange.MeterItem.RateOfWritesInMbPerSec > 1)
                return;

            NotificationCenter.SlowWrites.Add(ioChange);
        }

        public long GetEnvironmentsHash()
        {
            long hash = 0;
            foreach (var env in GetAllStoragesEnvironment())
            {
                hash = Hashing.Combine(hash, env.Environment.CurrentReadTransactionId);
                if (env.LastIndexQueryTime.HasValue)
                {
                    // 2 ** 27 = 134217728, Ticks is 10 mill per sec, so about 13.4 seconds
                    // are the rounding point here
                    var aboutEvery13Seconds = env.LastIndexQueryTime.Value.Ticks >> 27;
                    hash = Hashing.Combine(hash, aboutEvery13Seconds);
                }
            }

            return hash;
        }

        public bool IsShutdownRequested()
        {
            return _databaseShutdown.IsCancellationRequested;
        }

        public void ThrowIfShutdownRequested()
        {
            if (_databaseShutdown.IsCancellationRequested)
            {
                throw new OperationCanceledException($"Database '{Name}' is shutting down.");
            }
        }

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff();
        }

        internal sealed class TestingStuff
        {
            internal Action ActionToCallDuringDocumentDatabaseInternalDispose;

            internal Action CollectionRunnerBeforeOpenReadTransaction;

            internal Action CompactionAfterDatabaseUnload;

            internal Action AfterTxMergerDispose;

            internal Action BeforeExecutingClusterTransactions;

            internal Action BeforeSnapshotOfDocuments;

            internal Action AfterSnapshotOfDocuments;

            internal Action<DynamicJsonValue, WebSocket> OnNextMessageChangesApi;

            internal bool SkipDrainAllRequests = false;

            internal Action<string, string> DisposeLog;

            internal bool ForceSendTombstones = false;

            internal Action<PathSetting> ActionToCallOnGetTempPath;

            internal AsyncManualResetEvent DelayQueryByPatch;

            internal bool EnableWritesToTheWrongShard = false;

            internal IDisposable CallDuringDocumentDatabaseInternalDispose(Action action)
            {
                ActionToCallDuringDocumentDatabaseInternalDispose = action;

                return new DisposableAction(() => ActionToCallDuringDocumentDatabaseInternalDispose = null);
            }

            internal Action Subscription_ActionToCallDuringWaitForChangedDocuments;
            internal Action<long> Subscription_ActionToCallAfterRegisterSubscriptionConnection;
            internal Action<ConcurrentSet<SubscriptionConnection>> ConcurrentSubscription_ActionToCallDuringWaitForSubscribe;
            internal Action Subscription_ActionToCallDuringWaitForAck;

            internal IDisposable CallDuringWaitForChangedDocuments(Action action)
            {
                Subscription_ActionToCallDuringWaitForChangedDocuments = action;

                return new DisposableAction(() => Subscription_ActionToCallDuringWaitForChangedDocuments = null);
            }
            internal IDisposable CallAfterRegisterSubscriptionConnection(Action<long> action)
            {
                Subscription_ActionToCallAfterRegisterSubscriptionConnection = action;

                return new DisposableAction(() => Subscription_ActionToCallAfterRegisterSubscriptionConnection = null);
            }
            internal IDisposable CallDuringWaitForSubscribe(Action<ConcurrentSet<SubscriptionConnection>> action)
            {
                ConcurrentSubscription_ActionToCallDuringWaitForSubscribe = action;

                return new DisposableAction(() => ConcurrentSubscription_ActionToCallDuringWaitForSubscribe = null);
            }
            internal IDisposable CallDuringWaitForAck(Action action)
            {
                Subscription_ActionToCallDuringWaitForAck = action;

                return new DisposableAction(() => Subscription_ActionToCallDuringWaitForAck = null);
            }

            internal ManualResetEvent DatabaseRecordLoadHold;
            internal ManualResetEvent HealthCheckHold;

            internal int BulkInsertStreamReadTimeout;
        }
    }

    public enum DatabaseCleanupMode
    {
        None,
        Regular,
        Deep
    }
}
