using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.Expiration;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Utils;
using Raven.Server.Utils.IoMetrics;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server;
using Sparrow.Server.Meters;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Sparrow.Utils;
using Voron;
using Voron.Exceptions;
using Voron.Impl.Backup;
using Constants = Raven.Client.Constants;
using DatabaseInfo = Raven.Client.ServerWide.Operations.DatabaseInfo;
using DatabaseSmuggler = Raven.Server.Smuggler.Documents.DatabaseSmuggler;
using Size = Raven.Client.Util.Size;

namespace Raven.Server.Documents
{
    public class DocumentDatabase : IDisposable
    {
        private readonly ServerStore _serverStore;
        private readonly Action<string> _addToInitLog;
        private readonly Logger _logger;
        private readonly DisposeOnce<SingleAttempt> _disposeOnce;
        private TestingStuff _forTestingPurposes;

        private readonly CancellationTokenSource _databaseShutdown = new CancellationTokenSource();

        private readonly object _idleLocker = new object();

        private readonly object _clusterLocker = new object();

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

        public readonly ClusterTransactionWaiter ClusterTransactionWaiter;

        public void ResetIdleTime()
        {
            _lastIdleTicks = DateTime.MinValue.Ticks;
        }

        public DocumentDatabase(string name, RavenConfiguration configuration, ServerStore serverStore, Action<string> addToInitLog)
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

            _disposeOnce = new DisposeOnce<SingleAttempt>(DisposeInternal);
            try
            {
                if (configuration.Initialized == false)
                    throw new InvalidOperationException("Cannot create a new document database instance without initialized configuration");

                if (Configuration.Core.RunInMemory == false)
                {
                    _addToInitLog("Creating db.lock file");
                    _fileLocker = new FileLocker(Configuration.Core.DataDirectory.Combine("db.lock").FullPath);
                    _fileLocker.TryAcquireWriteLock(_logger);
                }


                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    MasterKey = serverStore.GetSecretKey(ctx, Name);

                    using (var rawRecord = _serverStore.Cluster.ReadRawDatabaseRecord(ctx, Name))
                    {
                        if (rawRecord != null)
                        {
                            var isEncrypted = rawRecord.IsEncrypted;
                            // can happen when we are in the process of restoring a database
                            if (isEncrypted && MasterKey == null)
                                throw new InvalidOperationException($"Attempt to create encrypted db {Name} without supplying the secret key");
                            if (isEncrypted == false && MasterKey != null)
                                throw new InvalidOperationException($"Attempt to create a non-encrypted db {Name}, but a secret key exists for this db.");
                        }
                    }
                }

                ClusterTransactionWaiter = new ClusterTransactionWaiter(this);
                QueryMetadataCache = new QueryMetadataCache();
                IoChanges = new IoChangesNotifications
                {
                    DisableIoMetrics = configuration.Storage.EnableIoMetrics == false
                };
                Changes = new DocumentsChanges();
                TombstoneCleaner = new TombstoneCleaner(this);
                DocumentsStorage = new DocumentsStorage(this, addToInitLog);
                IndexStore = new IndexStore(this, serverStore);
                QueryRunner = new QueryRunner(this);
                EtlLoader = new EtlLoader(this, serverStore);
                ReplicationLoader = new ReplicationLoader(this, serverStore);
                SubscriptionStorage = new SubscriptionStorage(this, serverStore);
                Metrics = new MetricCounters();
                MetricCacher = new DatabaseMetricCacher(this);
                TxMerger = new TransactionOperationsMerger(this, DatabaseShutdown);
                ConfigurationStorage = new ConfigurationStorage(this);
                NotificationCenter = new NotificationCenter.NotificationCenter(ConfigurationStorage.NotificationsStorage, Name, DatabaseShutdown, configuration);
                HugeDocuments = new HugeDocuments(NotificationCenter, ConfigurationStorage.NotificationsStorage, Name, configuration.PerformanceHints.HugeDocumentsCollectionSize,
                    configuration.PerformanceHints.HugeDocumentSize.GetValue(SizeUnit.Bytes));
                Operations = new Operations.Operations(Name, ConfigurationStorage.OperationsStorage, NotificationCenter, Changes,
                    Is32Bits ? TimeSpan.FromHours(12) : TimeSpan.FromDays(2));
                DatabaseInfoCache = serverStore.DatabaseInfoCache;
                RachisLogIndexNotifications = new RachisLogIndexNotifications(DatabaseShutdown);
                CatastrophicFailureNotification = new CatastrophicFailureNotification((environmentId, environmentPath, e, stacktrace) =>
                {
                    serverStore.DatabasesLandlord.CatastrophicFailureHandler.Execute(name, e, environmentId, environmentPath, stacktrace);
                });
                _hasClusterTransaction = new AsyncManualResetEvent(DatabaseShutdown);
                IdentityPartsSeparator = '/';
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        public ServerStore ServerStore => _serverStore;

        public DateTime LastIdleTime => new DateTime(_lastIdleTicks);

        public DateTime LastAccessTime;

        public DatabaseInfoCache DatabaseInfoCache { get; set; }

        public readonly SystemTime Time = new SystemTime();

        public ScriptRunnerCache Scripts;
        private string _lockFile;
        public readonly TransactionOperationsMerger TxMerger;

        public SubscriptionStorage SubscriptionStorage { get; }

        public string Name { get; }

        public Guid DbId => DocumentsStorage.Environment?.DbId ?? Guid.Empty;

        public string DbBase64Id => DocumentsStorage.Environment?.Base64Id ?? "";

        public RavenConfiguration Configuration { get; }

        public QueryRunner QueryRunner { get; }

        public CancellationToken DatabaseShutdown => _databaseShutdown.Token;

        public AsyncManualResetEvent DatabaseShutdownCompleted { get; } = new AsyncManualResetEvent();

        public DocumentsStorage DocumentsStorage { get; private set; }

        public ExpiredDocumentsCleaner ExpiredDocumentsCleaner { get; private set; }

        public TimeSeriesPolicyRunner TimeSeriesPolicyRunner { get; private set; }

        public PeriodicBackupRunner PeriodicBackupRunner { get; private set; }

        public TombstoneCleaner TombstoneCleaner { get; private set; }

        public DocumentsChanges Changes { get; }

        public IoChangesNotifications IoChanges { get; }

        public CatastrophicFailureNotification CatastrophicFailureNotification { get; }

        public NotificationCenter.NotificationCenter NotificationCenter { get; private set; }

        public Operations.Operations Operations { get; private set; }

        public HugeDocuments HugeDocuments { get; }

        public MetricCounters Metrics { get; }

        public IndexStore IndexStore { get; private set; }

        public ConfigurationStorage ConfigurationStorage { get; }

        public ReplicationLoader ReplicationLoader { get; private set; }

        public EtlLoader EtlLoader { get; private set; }

        public readonly ConcurrentSet<TcpConnectionOptions> RunningTcpConnections = new ConcurrentSet<TcpConnectionOptions>();

        public readonly DateTime StartTime;

        public readonly RachisLogIndexNotifications RachisLogIndexNotifications;

        public readonly byte[] MasterKey;

        public char IdentityPartsSeparator { get; private set; }

        public ClientConfiguration ClientConfiguration { get; private set; }

        public StudioConfiguration StudioConfiguration { get; private set; }

        public bool Is32Bits { get; }

        private long _lastDatabaseRecordChangeIndex;

        public long LastDatabaseRecordChangeIndex
        {
            get => Volatile.Read(ref _lastDatabaseRecordChangeIndex);
            private set => _lastDatabaseRecordChangeIndex = value; // we write this always under lock
        }

        private long _lastValueChangeIndex;

        public long LastValueChangeIndex
        {
            get => Volatile.Read(ref _lastValueChangeIndex);
            private set => _lastValueChangeIndex = value; // we write this always under lock
        }

        public bool CanUnload => Interlocked.Read(ref _preventUnloadCounter) == 0;

        public readonly QueryMetadataCache QueryMetadataCache;

        public long LastTransactionId => DocumentsStorage.Environment.CurrentReadTransactionId;

        public void Initialize(InitializeOptions options = InitializeOptions.None, DateTime? wakeup = null)
        {
            try
            {
                Configuration.CheckDirectoryPermissions();

                _addToInitLog("Initializing NotificationCenter");
                NotificationCenter.Initialize(this);

                _addToInitLog("Initializing DocumentStorage");
                DocumentsStorage.Initialize((options & InitializeOptions.GenerateNewDatabaseId) == InitializeOptions.GenerateNewDatabaseId);
                _addToInitLog("Starting Transaction Merger");
                TxMerger.Start();
                _addToInitLog("Initializing ConfigurationStorage");
                ConfigurationStorage.Initialize();

                if ((options & InitializeOptions.SkipLoadingDatabaseRecord) == InitializeOptions.SkipLoadingDatabaseRecord)
                    return;

                _addToInitLog("Loading Database");

                MetricCacher.Initialize();

                long index;
                DatabaseRecord record;
                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                    record = _serverStore.Cluster.ReadDatabase(context, Name, out index);

                if (record == null)
                    DatabaseDoesNotExistException.Throw(Name);

                PeriodicBackupRunner = new PeriodicBackupRunner(this, _serverStore, wakeup);

                _addToInitLog("Initializing IndexStore (async)");
                _indexStoreTask = IndexStore.InitializeAsync(record, index, _addToInitLog);
                _addToInitLog("Initializing Replication");
                ReplicationLoader?.Initialize(record);
                _addToInitLog("Initializing ETL");
                EtlLoader.Initialize(record);

                TombstoneCleaner.Start();

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

                SubscriptionStorage.Initialize();
                _addToInitLog("Initializing SubscriptionStorage completed");

                _serverStore.StorageSpaceMonitor.Subscribe(this);

                ThreadPool.QueueUserWorkItem( _ =>
                {
                    try
                    {
                        NotifyFeaturesAboutStateChange(record, index);
                    }
                    catch
                    {
                        // We ignore the exception since it was caught in the function itself
                    }
                }, null);

                Task.Run(async () =>
                {
                    try
                    {
                        await ExecuteClusterTransactionTask();

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
                }, DatabaseShutdown);
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

      

        public IDisposable PreventFromUnloading()
        {
            Interlocked.Increment(ref _preventUnloadCounter);

            return new DisposableAction(() => Interlocked.Decrement(ref _preventUnloadCounter));
        }

        public DatabaseUsage DatabaseInUse(bool skipUsagesCount)
        {
            return new DatabaseUsage(this, skipUsagesCount);
        }

        internal void ThrowDatabaseShutdown(Exception e = null)
        {
            throw CreateDatabaseShutdownException(e);
        }

        internal DatabaseDisabledException CreateDatabaseShutdownException(Exception e = null)
        {
            return new DatabaseDisabledException("The database " + Name + " is shutting down", e);
        }

        private readonly AsyncManualResetEvent _hasClusterTransaction;
        public readonly DatabaseMetricCacher MetricCacher;

        public void NotifyOnPendingClusterTransaction(long index, DatabasesLandlord.ClusterDatabaseChangeType changeType)
        {
            if (changeType == DatabasesLandlord.ClusterDatabaseChangeType.ClusterTransactionCompleted)
            {
                RachisLogIndexNotifications.NotifyListenersAbout(index, null);
                return;
            }

            _hasClusterTransaction.Set();
        }

        private long? _nextClusterCommand;
        private long _lastCompletedClusterTransaction;
        public long LastCompletedClusterTransaction => _lastCompletedClusterTransaction;
        public bool IsEncrypted => MasterKey != null;

        private int _clusterTransactionDelayOnFailure = 1000;
        private FileLocker _fileLocker;

        private async Task ExecuteClusterTransactionTask()
        {
            while (DatabaseShutdown.IsCancellationRequested == false)
            {
                var topology = ServerStore.LoadDatabaseTopology(Name);
                if (topology.Promotables.Contains(ServerStore.NodeTag))
                {
                    await Task.Delay(1000, DatabaseShutdown);
                    continue;
                }

                await _hasClusterTransaction.WaitAsync(DatabaseShutdown);
                if (DatabaseShutdown.IsCancellationRequested)
                    return;

                _hasClusterTransaction.Reset();

                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    try
                    {
                        var batch = new List<ClusterTransactionCommand.SingleClusterDatabaseCommand>(
                            ClusterTransactionCommand.ReadCommandsBatch(context, Name, fromCount: _nextClusterCommand, take: 256));

                        if (batch.Count == 0)
                        {
                            continue;
                        }

                        var mergedCommands = new BatchHandler.ClusterTransactionMergedCommand(this, batch);
                        try
                        {
                            //If we get a database shutdown while we process a cluster tx command this
                            //will cause us to stop running and disposing the context while its memory is still been used by the merger execution
                            await TxMerger.Enqueue(mergedCommands);
                        }
                        catch (Exception e)
                        {
                            if (_logger.IsInfoEnabled)
                            {
                                _logger.Info($"Failed to execute cluster transaction batch (count: {batch.Count}), will retry them one-by-one.", e);
                            }
                            await ExecuteClusterTransactionOneByOne(batch);
                            continue;
                        }
                        foreach (var command in batch)
                        {
                            OnClusterTransactionCompletion(command, mergedCommands);
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
        }

        private async Task ExecuteClusterTransactionOneByOne(List<ClusterTransactionCommand.SingleClusterDatabaseCommand> batch)
        {
            foreach (var command in batch)
            {
                var singleCommand = new List<ClusterTransactionCommand.SingleClusterDatabaseCommand>
                {
                    command
                };
                var mergedCommand = new BatchHandler.ClusterTransactionMergedCommand(this, singleCommand);
                try
                {
                    await TxMerger.Enqueue(mergedCommand);
                    OnClusterTransactionCompletion(command, mergedCommand);

                    _clusterTransactionDelayOnFailure = 1000;
                }
                catch (Exception e)
                {
                    OnClusterTransactionCompletion(command, mergedCommand, exception: e);
                    NotificationCenter.Add(AlertRaised.Create(
                        Name,
                        "Cluster transaction failed to execute",
                        $"Failed to execute cluster transactions with raft index: {command.Index}. {Environment.NewLine}" +
                        $"With the following document ids involved: {string.Join(", ", command.Commands.Select(item => JsonDeserializationServer.ClusterTransactionDataCommand((BlittableJsonReaderObject)item).Id))} {Environment.NewLine}" +
                        "Performing cluster transactions on this database will be stopped until the issue is resolved.",
                        AlertType.ClusterTransactionFailure,
                        NotificationSeverity.Error,
                        $"{Name}/ClusterTransaction",
                        new ExceptionDetails(e)));

                    await Task.Delay(_clusterTransactionDelayOnFailure, DatabaseShutdown);
                    _clusterTransactionDelayOnFailure = Math.Min(_clusterTransactionDelayOnFailure * 2, 15000);

                    return;
                }
            }
        }

        private void OnClusterTransactionCompletion(ClusterTransactionCommand.SingleClusterDatabaseCommand command,
            BatchHandler.ClusterTransactionMergedCommand mergedCommands, Exception exception = null)
        {
            try
            {
                var index = command.Index;
                var options = mergedCommands.Options[index];
                if (exception == null)
                {
                    Task indexTask = null;
                    if (options.WaitForIndexesTimeout != null)
                    {
                        indexTask = BatchHandler.WaitForIndexesAsync(DocumentsStorage.ContextPool, this, options.WaitForIndexesTimeout.Value,
                            options.SpecifiedIndexesQueryString, options.WaitForIndexThrow,
                            mergedCommands.LastChangeVector, mergedCommands.LastTombstoneEtag, mergedCommands.ModifiedCollections);
                    }

                    var result = new BatchHandler.ClusterTransactionCompletionResult
                    {
                        Array = mergedCommands.Replies[index],
                        IndexTask = indexTask,
                    };
                    ClusterTransactionWaiter.SetResult(options.TaskId, index, result);
                    _nextClusterCommand = command.PreviousCount + command.Commands.Length;
                    _lastCompletedClusterTransaction = _nextClusterCommand.Value - 1;
                    return;
                }

                ClusterTransactionWaiter.SetException(options.TaskId, index, exception);
            }
            catch (Exception e)
            {
                // nothing we can do
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info($"Failed to notify about transaction completion for database '{Name}'.", e);
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

        public void Dispose()
        {
            _disposeOnce.Dispose();
        }

        private unsafe void DisposeInternal()
        {
            _databaseShutdown.Cancel();

            _forTestingPurposes?.ActionToCallDuringDocumentDatabaseInternalDispose?.Invoke();

            //before we dispose of the database we take its latest info to be displayed in the studio
            try
            {
                var databaseInfo = GenerateOfflineDatabaseInfo();
                if (databaseInfo != null)
                    DatabaseInfoCache?.InsertDatabaseInfo(databaseInfo, Name);
            }
            catch (Exception e)
            {
                // if we encountered a catastrophic failure we might not be able to retrieve database info

                if (_logger.IsInfoEnabled)
                    _logger.Info("Failed to generate and store database info", e);
            }

            if (_forTestingPurposes == null || _forTestingPurposes.SkipDrainAllRequests == false)
            {
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
            }

            var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(DocumentDatabase)} {Name}");

            var lockTaken = false;
            Monitor.TryEnter(_clusterLocker, TimeSpan.FromSeconds(5), ref lockTaken);

            if (lockTaken == false && _logger.IsOperationsEnabled)
                _logger.Operations("Failed to acquire lock during database dispose for cluster notifications. Will dispose rudely...");

            exceptionAggregator.Execute(() =>
            {
                _serverStore.StorageSpaceMonitor.Unsubscribe(this);
            });

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

            // must acquire the lock in order to prevent concurrent access to index files
            if (lockTaken == false)
                Monitor.Enter(_clusterLocker);

            var indexStoreTask = _indexStoreTask;
            if (indexStoreTask != null)
            {
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
            }

            exceptionAggregator.Execute(() =>
            {
                IndexStore?.Dispose();
            });

            exceptionAggregator.Execute(() =>
            {
                ExpiredDocumentsCleaner?.Dispose();
            });

            exceptionAggregator.Execute(() =>
            {
                PeriodicBackupRunner?.Dispose();
            });

            exceptionAggregator.Execute(() =>
            {
                TombstoneCleaner?.Dispose();
            });

            exceptionAggregator.Execute(() =>
            {
                ReplicationLoader?.Dispose();
            });

            exceptionAggregator.Execute(() =>
            {
                EtlLoader?.Dispose();
            });

            exceptionAggregator.Execute(() =>
            {
                Operations?.Dispose(exceptionAggregator);
            });

            exceptionAggregator.Execute(() =>
            {
                HugeDocuments?.Dispose();
            });

            exceptionAggregator.Execute(() =>
            {
                NotificationCenter?.Dispose();
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
                    Sodium.sodium_memzero(pKey, (UIntPtr)MasterKey.Length);
                }
            });

            exceptionAggregator.Execute(() => _fileLocker.Dispose());

            exceptionAggregator.Execute(RachisLogIndexNotifications);

            exceptionAggregator.Execute(_hasClusterTransaction);

            exceptionAggregator.ThrowIfNeeded();
        }

        public DynamicJsonValue GenerateOfflineDatabaseInfo()
        {
            var envs = GetAllStoragesEnvironment().ToList();
            if (envs.Count == 0 || envs.Any(x => x.Environment == null))
                return null;

            var size = GetSizeOnDisk();
            var databaseInfo = new DynamicJsonValue
            {
                [nameof(DatabaseInfo.HasRevisionsConfiguration)] = DocumentsStorage.RevisionsStorage.Configuration != null,
                [nameof(DatabaseInfo.HasExpirationConfiguration)] = (ExpiredDocumentsCleaner?.ExpirationConfiguration?.Disabled ?? true) == false,
                [nameof(DatabaseInfo.HasRefreshConfiguration)] = (ExpiredDocumentsCleaner?.RefreshConfiguration?.Disabled ?? true) == false,
                [nameof(DatabaseInfo.IsAdmin)] = true, //TODO: implement me!
                [nameof(DatabaseInfo.IsEncrypted)] = DocumentsStorage.Environment.Options.Encryption.IsEnabled,
                [nameof(DatabaseInfo.Name)] = Name,
                [nameof(DatabaseInfo.Disabled)] = false, //TODO: this value should be overwritten by the studio since it is cached
                [nameof(DatabaseInfo.TotalSize)] = new DynamicJsonValue
                {
                    [nameof(Size.HumaneSize)] = size.Data.HumaneSize,
                    [nameof(Size.SizeInBytes)] = size.Data.SizeInBytes
                },
                [nameof(DatabaseInfo.TempBuffersSize)] = new DynamicJsonValue
                {
                    [nameof(Size.HumaneSize)] = "0 Bytes",
                    [nameof(Size.SizeInBytes)] = 0
                },
                [nameof(DatabaseInfo.IndexingErrors)] = IndexStore.GetIndexes().Sum(index => index.GetErrorCount()),
                [nameof(DatabaseInfo.Alerts)] = NotificationCenter.GetAlertCount(),
                [nameof(DatabaseInfo.PerformanceHints)] = NotificationCenter.GetPerformanceHintCount(),
                [nameof(DatabaseInfo.UpTime)] = null, //it is shutting down
                [nameof(DatabaseInfo.BackupInfo)] = PeriodicBackupRunner?.GetBackupInfo(),
                [nameof(DatabaseInfo.MountPointsUsage)] = new DynamicJsonArray(GetMountPointsUsage(includeTempBuffers: false).Select(x => x.ToJson())),
                [nameof(DatabaseInfo.DocumentsCount)] = DocumentsStorage.GetNumberOfDocuments(),
                [nameof(DatabaseInfo.IndexesCount)] = IndexStore.GetIndexes().Count(),
                [nameof(DatabaseInfo.RejectClients)] = false, //TODO: implement me!
                [nameof(DatabaseInfo.IndexingStatus)] = IndexStore.Status.ToString(),
                ["CachedDatabaseInfo"] = true
            };
            return databaseInfo;
        }

        public DatabaseSummary GetDatabaseSummary()
        {
            using (DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionContext))
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
            using (DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
            using (var tx = documentsContext.OpenWriteTransaction())
            {
                DocumentsStorage.SetDatabaseChangeVector(documentsContext, changeVector);
                tx.Commit();
            }
        }

        public void RunIdleOperations()
        {
            if (Monitor.TryEnter(_idleLocker) == false)
                return;

            try
            {
                var sp = Stopwatch.StartNew();
                var utcNow = DateTime.UtcNow;

                _lastIdleTicks = utcNow.Ticks;
                IndexStore?.RunIdleOperations();
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
                    _logger.Info($"Ran idle operations for database '{Name}', took: {sp.ElapsedMilliseconds}ms");
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
                var env = index?._indexStorage?.Environment();
                if (env != null)
                    yield return
                        new StorageEnvironmentWithType(index.Name,
                            StorageEnvironmentWithType.StorageEnvironmentType.Index, env)
                        {
                            LastIndexQueryTime = index.GetLastQueryingTime()
                        };
            }
        }

        private IEnumerable<FullBackup.StorageEnvironmentInformation> GetAllStoragesForBackup()
        {
            foreach (var storageEnvironmentWithType in GetAllStoragesEnvironment())
            {
                switch (storageEnvironmentWithType.Type)
                {
                    case StorageEnvironmentWithType.StorageEnvironmentType.Documents:
                        yield return new FullBackup.StorageEnvironmentInformation
                        {
                            Name = string.Empty,
                            Folder = Constants.Documents.PeriodicBackup.Folders.Documents,
                            Env = storageEnvironmentWithType.Environment
                        };
                        break;
                    case StorageEnvironmentWithType.StorageEnvironmentType.Index:
                        yield return new FullBackup.StorageEnvironmentInformation
                        {
                            Name = IndexDefinitionBase.GetIndexNameSafeForFileSystem(storageEnvironmentWithType.Name),
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

        public SmugglerResult FullBackupTo(string backupPath, CompressionLevel compressionLevel = CompressionLevel.Optimal,
            Action<(string Message, int FilesCount)> infoNotify = null, CancellationToken cancellationToken = default)
        {
            SmugglerResult smugglerResult;

            using (var file = SafeFileStream.Create(backupPath, FileMode.Create))
            using (var package = new ZipArchive(file, ZipArchiveMode.Create, leaveOpen: true))
            {
                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
                {
                    var zipArchiveEntry = package.CreateEntry(RestoreSettings.SmugglerValuesFileName, compressionLevel);
                    using (var zipStream = zipArchiveEntry.Open())
                    using (var outputStream = GetOutputStream(zipStream))
                    {
                        var smugglerSource = new DatabaseSource(this, 0, 0, _logger);
                        using (DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
                        {
                            var smugglerDestination = new StreamDestination(outputStream, documentsContext, smugglerSource);
                            var databaseSmugglerOptionsServerSide = new DatabaseSmugglerOptionsServerSide
                            {
                                AuthorizationStatus = AuthorizationStatus.DatabaseAdmin,
                                OperateOnTypes = DatabaseItemType.CompareExchange | DatabaseItemType.Identities
                            };
                            var smuggler = new DatabaseSmuggler(this,
                                smugglerSource,
                                smugglerDestination,
                                Time,
                                options: databaseSmugglerOptionsServerSide,
                                token: cancellationToken);

                            smugglerResult = smuggler.Execute();
                        }

                        outputStream.Flush();
                    }

                    infoNotify?.Invoke(("Backed up Database Record", 1));

                    zipArchiveEntry = package.CreateEntry(RestoreSettings.SettingsFileName, compressionLevel);
                    using (var zipStream = zipArchiveEntry.Open())
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

                BackupMethods.Full.ToFile(GetAllStoragesForBackup(), package, compressionLevel,
                    infoNotify: infoNotify, cancellationToken: cancellationToken);

                file.Flush(true); // make sure that we fully flushed to disk
            }

            return smugglerResult;
        }

        public Stream GetOutputStream(Stream fileStream)
        {
            return MasterKey == null ? fileStream : new EncryptingXChaCha20Poly1305Stream(fileStream, MasterKey);
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

                NotifyFeaturesAboutValueChange(record, index);
                RachisLogIndexNotifications.NotifyListenersAbout(index, null);

            }
            catch (Exception e)
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
                IdentityPartsSeparator = record.Client?.IdentityPartsSeparator ?? '/';

                StudioConfiguration = record.Studio;

                NotifyFeaturesAboutStateChange(record, index);
                RachisLogIndexNotifications.NotifyListenersAbout(index, null);
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

        private void NotifyFeaturesAboutStateChange(DatabaseRecord record, long index)
        {
            if (CanSkipDatabaseRecordChange(record.DatabaseName, index))
                return;

            var taken = false;
            while (taken == false)
            {
                Monitor.TryEnter(_clusterLocker, TimeSpan.FromSeconds(5), ref taken);

                try
                {
                    if (CanSkipDatabaseRecordChange(record.DatabaseName, index))
                        return;

                    if (DatabaseShutdown.IsCancellationRequested)
                        return;

                    if (taken == false)
                        continue;

                    Debug.Assert(string.Equals(Name, record.DatabaseName, StringComparison.OrdinalIgnoreCase),
                        $"{Name} != {record.DatabaseName}");

                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Starting to process record {index} (current {LastDatabaseRecordChangeIndex}) for {record.DatabaseName}.");

                    try
                    {
                        DatabaseGroupId = record.Topology.DatabaseTopologyIdBase64;

                        SetUnusedDatabaseIds(record);
                        InitializeFromDatabaseRecord(record);
                        IndexStore.HandleDatabaseRecordChange(record, index);
                        ReplicationLoader?.HandleDatabaseRecordChange(record);
                        EtlLoader?.HandleDatabaseRecordChange(record);
                        SubscriptionStorage?.HandleDatabaseRecordChange(record);

                        OnDatabaseRecordChanged(record);

                        LastDatabaseRecordChangeIndex = index;

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
                finally
                {
                    if (taken)
                        Monitor.Exit(_clusterLocker);
                }
            }
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

        private bool CanSkipDatabaseRecordChange(string database, long index)
        {
            if (LastDatabaseRecordChangeIndex > index)
            {
                // index and LastDatabaseRecordIndex could have equal values when we transit from/to passive and want to update the tasks. 
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Skipping record {index} (current {LastDatabaseRecordChangeIndex}) for {database} because it was already precessed.");
                return true;
            }

            return false;
        }

        private bool CanSkipValueChange(string database, long index)
        {
            if (LastValueChangeIndex > index)
            {
                // index and LastDatabaseRecordIndex could have equal values when we transit from/to passive and want to update the tasks. 
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Skipping value change for index {index} (current {LastValueChangeIndex}) for {database} because it was already precessed.");
                return true;
            }

            return false;
        }

        private void NotifyFeaturesAboutValueChange(DatabaseRecord record, long index)
        {
            if (CanSkipValueChange(record.DatabaseName, index))
                return;

            var taken = false;
            while (taken == false)
            {
                Monitor.TryEnter(_clusterLocker, TimeSpan.FromSeconds(5), ref taken);

                try
                {
                    if (CanSkipValueChange(record.DatabaseName, index))
                        return;

                    if (DatabaseShutdown.IsCancellationRequested)
                        return;

                    if (taken == false)
                        continue;

                    DatabaseShutdown.ThrowIfCancellationRequested();
                    SubscriptionStorage?.HandleDatabaseRecordChange(record);
                    EtlLoader?.HandleDatabaseValueChanged(record);

                    LastValueChangeIndex = index;
                }
                finally
                {
                    if (taken)
                        Monitor.Exit(_clusterLocker);
                }
            }
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
            if (record == null || DocumentsStorage == null)
                return;

            ClientConfiguration = record.Client;
            StudioConfiguration = record.Studio;
            DocumentsStorage.RevisionsStorage.InitializeFromDatabaseRecord(record);
            DocumentsStorage.DocumentPut.InitializeFromDatabaseRecord(record);
            ExpiredDocumentsCleaner = ExpiredDocumentsCleaner.LoadConfigurations(this, record, ExpiredDocumentsCleaner);
            TimeSeriesPolicyRunner = TimeSeriesPolicyRunner.LoadConfigurations(this, record, TimeSeriesPolicyRunner);
            PeriodicBackupRunner.UpdateConfigurations(record);
        }

        public string WhoseTaskIsIt(
            DatabaseTopology databaseTopology,
            IDatabaseTask configuration,
            IDatabaseTaskStatus taskStatus,
            bool keepTaskOnOriginalMemberNode = false)
        {
            var whoseTaskIsIt = databaseTopology.WhoseTaskIsIt(
                ServerStore.Engine.CurrentState, configuration,
                getLastResponsibleNode:
                () =>
                {
                    var lastResponsibleNode = taskStatus?.NodeTag;
                    if (lastResponsibleNode == null)
                    {
                        // first time this task is assigned
                        return null;
                    }

                    if (databaseTopology.AllNodes.Contains(lastResponsibleNode) == false)
                    {
                        // the topology doesn't include the last responsible node anymore
                        // we'll choose a different one
                        return null;
                    }

                    if (taskStatus is PeriodicBackupStatus)
                    {
                        if (databaseTopology.Rehabs.Contains(lastResponsibleNode) &&
                            databaseTopology.PromotablesStatus.TryGetValue(lastResponsibleNode, out var status) &&
                            (status == DatabasePromotionStatus.OutOfCpuCredits || 
                             status == DatabasePromotionStatus.EarlyOutOfMemory ||
                             status == DatabasePromotionStatus.HighDirtyMemory))
                        {
                            // avoid moving backup tasks when the machine is out of CPU credit
                            return lastResponsibleNode;
                        }
                    }

                    if (ServerStore.LicenseManager.HasHighlyAvailableTasks() == false)
                    {
                        // can't redistribute, keep it on the original node
                        RaiseAlertIfNecessary(databaseTopology, configuration, lastResponsibleNode);
                        return lastResponsibleNode;
                    }

                    if (keepTaskOnOriginalMemberNode &&
                        databaseTopology.Members.Contains(lastResponsibleNode))
                    {
                        // keep the task on the original node
                        return lastResponsibleNode;
                    }

                    return null;
                });

            if (whoseTaskIsIt == null && taskStatus is PeriodicBackupStatus)
                return taskStatus.NodeTag; // we don't want to stop backup process

            return whoseTaskIsIt;
        }

        private void RaiseAlertIfNecessary(DatabaseTopology databaseTopology, IDatabaseTask configuration, string lastResponsibleNode)
        {
            // raise alert if redistribution is necessary 
            if (databaseTopology.Count > 1 &&
                ServerStore.NodeTag != lastResponsibleNode &&
                databaseTopology.Members.Contains(lastResponsibleNode) == false)
            {
                var alert = LicenseManager.CreateHighlyAvailableTasksAlert(databaseTopology, configuration, lastResponsibleNode);
                NotificationCenter.Add(alert);
            }
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
            // only if have a newer topology index
            return _lastTopologyIndex > index;
        }

        public bool HasClientConfigurationChanged(long index)
        {
            var serverIndex = GetClientConfigurationEtag();
            return index < serverIndex;
        }

        public long GetClientConfigurationEtag()
        {
            return ClientConfiguration == null || ClientConfiguration.Disabled && ServerStore.LastClientConfigurationIndex > ClientConfiguration.Etag
                ? ServerStore.LastClientConfigurationIndex
                : ClientConfiguration.Etag;
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

        internal void HandleNonDurableFileSystemError(object sender, NonDurabilitySupportEventArgs e)
        {
            _serverStore?.NotificationCenter.Add(AlertRaised.Create(
                Name,
                $"Non Durable File System - {Name ?? "Unknown Database"}",
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
            NotificationCenter.NotificationCenter nc;
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

            nc?.Add(AlertRaised.Create(Name,
                title,
                $"{e.Message}{Environment.NewLine}{Environment.NewLine}Environment: {environment}",
                AlertType.RecoveryError,
                NotificationSeverity.Error,
                key: resourceName));
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
            NotificationCenter.NotificationCenter nc;
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

            nc?.Add(AlertRaised.Create(Name,
                title,
                $"{e.Message}{Environment.NewLine}{Environment.NewLine}Environment: {environment}",
                AlertType.IntegrityErrorOfAlreadySyncedData,
                NotificationSeverity.Warning,
                key: resourceName));
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

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (_forTestingPurposes != null)
                return _forTestingPurposes;

            return _forTestingPurposes = new TestingStuff();
        }

        internal class TestingStuff
        {
            internal Action ActionToCallDuringDocumentDatabaseInternalDispose;

            internal bool SkipDrainAllRequests = false;

            internal IDisposable CallDuringDocumentDatabaseInternalDispose(Action action)
            {
                ActionToCallDuringDocumentDatabaseInternalDispose = action;

                return new DisposableAction(() => ActionToCallDuringDocumentDatabaseInternalDispose = null);
            }
        }
    }
}
