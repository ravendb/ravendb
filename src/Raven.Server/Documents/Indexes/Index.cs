using System;
using System.Buffers;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Lucene.Net.Index;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Exceptions;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Explanation;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Exceptions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Memory;
using Raven.Server.Storage.Layout;
using Raven.Server.Storage.Schema;
using Raven.Server.Utils;
using Raven.Server.Utils.Metrics;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Json;
using Voron;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Threading;
using Sparrow.Utils;
using Size = Sparrow.Size;
using Voron.Debugging;
using Voron.Exceptions;
using Voron.Impl;
using Voron.Impl.Compaction;
using FacetQuery = Raven.Server.Documents.Queries.Facets.FacetQuery;

namespace Raven.Server.Documents.Indexes
{
    public abstract class Index<TIndexDefinition, TField> : Index
        where TIndexDefinition : IndexDefinitionBase<TField> where TField : IndexFieldBase
    {
        public new TIndexDefinition Definition => (TIndexDefinition)base.Definition;

        protected Index(IndexType type, TIndexDefinition definition)
            : base(type, definition)
        {
        }
    }

    public abstract class Index : ITombstoneAware, IDisposable, ILowMemoryHandler
    {
        private long _writeErrors;

        private long _unexpectedErrors;

        private long _analyzerErrors;

        private const long WriteErrorsLimit = 10;

        private const long UnexpectedErrorsLimit = 3;

        private const long AnalyzerErrorLimit = 0;

        protected Logger _logger;

        internal LuceneIndexPersistence IndexPersistence;

        private readonly AsyncManualResetEvent _indexingBatchCompleted = new AsyncManualResetEvent();

        /// <summary>
        /// Cancelled if the database is in shutdown process.
        /// </summary>
        private CancellationTokenSource _indexingProcessCancellationTokenSource;
        private bool _indexDisabled;

        private readonly ConcurrentDictionary<string, IndexProgress.CollectionStats> _inMemoryIndexProgress =
            new ConcurrentDictionary<string, IndexProgress.CollectionStats>();

        internal DocumentDatabase DocumentDatabase;

        internal PoolOfThreads.LongRunningWork _indexingThread;

        private bool _initialized;

        protected UnmanagedBuffersPoolWithLowMemoryHandling _unmanagedBuffersPool;

        private StorageEnvironment _environment;

        internal TransactionContextPool _contextPool;

        protected readonly ManualResetEventSlim _mre = new ManualResetEventSlim();
        private readonly object _disablingIndexLock = new object();

        private readonly ManualResetEventSlim _logsAppliedEvent = new ManualResetEventSlim();

        private DateTime? _lastQueryingTime;
        public DateTime? LastIndexingTime { get; private set; }

        public Stopwatch TimeSpentIndexing = new Stopwatch();

        public readonly HashSet<string> Collections;

        internal IndexStorage _indexStorage;

        private IIndexingWork[] _indexWorkers;

        public readonly ConcurrentSet<ExecutingQueryInfo> CurrentlyRunningQueries =
            new ConcurrentSet<ExecutingQueryInfo>();

        private IndexingStatsAggregator _lastStats;

        private readonly ConcurrentQueue<IndexingStatsAggregator> _lastIndexingStats =
            new ConcurrentQueue<IndexingStatsAggregator>();

        private int _numberOfQueries;
        private bool _didWork;
        private bool _isReplacing;

        protected readonly bool HandleAllDocs;

        protected internal MeterMetric MapsPerSec = new MeterMetric();
        protected internal MeterMetric ReducesPerSec = new MeterMetric();

        protected internal IndexingConfiguration Configuration;

        protected PerformanceHintsConfiguration PerformanceHints;

        private bool _allocationCleanupNeeded;

        private readonly MultipleUseFlag _lowMemoryFlag = new MultipleUseFlag();
        private long _lowMemoryPressure;
        private bool _batchStopped;

        private Size _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;
        internal NativeMemory.ThreadStats _threadAllocations;
        private string _errorStateReason;
        private bool _isCompactionInProgress;
        internal TimeSpan? _firstBatchTimeout;
        private Lazy<Size?> _transactionSizeLimit;
        private bool _scratchSpaceLimitExceeded;

        private readonly ReaderWriterLockSlim _currentlyRunningQueriesLock = new ReaderWriterLockSlim();
        private readonly MultipleUseFlag _priorityChanged = new MultipleUseFlag();
        private readonly MultipleUseFlag _hadRealIndexingWorkToDo = new MultipleUseFlag();
        private readonly MultipleUseFlag _definitionChanged = new MultipleUseFlag();
        private Func<bool> _indexValidationStalenessCheck = () => true;

        private readonly ConcurrentDictionary<string, SpatialField> _spatialFields = new ConcurrentDictionary<string, SpatialField>(StringComparer.OrdinalIgnoreCase);

        internal readonly QueryBuilderFactories _queryBuilderFactories;

        private string IndexingThreadName => "Indexing of " + Name + " of " + _indexStorage.DocumentDatabase.Name;

        private readonly WarnIndexOutputsPerDocument _indexOutputsPerDocumentWarning = new WarnIndexOutputsPerDocument
        {
            MaxNumberOutputsPerDocument = int.MinValue,
            Suggestion = "Please verify this index definition and consider a re-design of your entities or index for better indexing performance"
        };

        private static readonly Size DefaultMaximumMemoryAllocation = new Size(32, SizeUnit.Megabytes);

        public long LastTransactionId => _environment.CurrentReadTransactionId;

        protected Index(IndexType type, IndexDefinitionBase definition)
        {
            Type = type;
            Definition = definition;
            Collections = new HashSet<string>(Definition.Collections, StringComparer.OrdinalIgnoreCase);

            if (Collections.Contains(Constants.Documents.Collections.AllDocumentsCollection))
                HandleAllDocs = true;

            _queryBuilderFactories = new QueryBuilderFactories
            {
                GetSpatialFieldFactory = GetOrAddSpatialField,
                GetRegexFactory = GetOrAddRegex
            };

            _disposeOne = new DisposeOnce<SingleAttempt>(() =>
            {
                using (DrainRunningQueries())
                    DisposeIndex();
            });
        }

        protected virtual void DisposeIndex()
        {
            var needToLock = _currentlyRunningQueriesLock.IsWriteLockHeld == false;
            if (needToLock)
                _currentlyRunningQueriesLock.EnterWriteLock();
            try
            {
                _indexingProcessCancellationTokenSource?.Cancel();

                //Does happen for faulty in memory indexes
                if (DocumentDatabase != null)
                {
                    DocumentDatabase.TombstoneCleaner.Unsubscribe(this);

                    DocumentDatabase.Changes.OnIndexChange -= HandleIndexChange;
                }

                _indexValidationStalenessCheck = null;

                var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(Index)} '{Name}'");

                exceptionAggregator.Execute(() =>
                {
                    var indexingThread = _indexingThread;

                    // If we invoke Thread.Join from the indexing thread itself it will cause a deadlock
                    if (indexingThread != null && PoolOfThreads.LongRunningWork.Current != indexingThread)
                        indexingThread.Join(int.MaxValue);
                });

                exceptionAggregator.Execute(() => { IndexPersistence?.Dispose(); });

                exceptionAggregator.Execute(() => { _environment?.Dispose(); });

                exceptionAggregator.Execute(() => { _unmanagedBuffersPool?.Dispose(); });

                exceptionAggregator.Execute(() => { _contextPool?.Dispose(); });

                exceptionAggregator.Execute(() => { _indexingProcessCancellationTokenSource?.Dispose(); });

                exceptionAggregator.ThrowIfNeeded();
            }
            finally
            {
                if (needToLock)
                    _currentlyRunningQueriesLock.ExitWriteLock();
            }
        }

        public static Index Open(string path, DocumentDatabase documentDatabase)
        {
            var logger = LoggingSource.Instance.GetLogger<Index>(documentDatabase.Name);

            StorageEnvironment environment = null;

            var name = new DirectoryInfo(path).Name;
            var indexPath = path;

            var indexTempPath =
                documentDatabase.Configuration.Indexing.TempPath?.Combine(name);

            var options = StorageEnvironmentOptions.ForPath(indexPath, indexTempPath?.FullPath, null,
                documentDatabase.IoChanges, documentDatabase.CatastrophicFailureNotification);
            try
            {
                InitializeOptions(options, documentDatabase, name);

                DirectoryExecUtils.SubscribeToOnDirectoryInitializeExec(options, documentDatabase.Configuration.Storage, documentDatabase.Name, DirectoryExecUtils.EnvironmentType.Index, logger);

                environment = LayoutUpdater.OpenEnvironment(options);

                IndexType type;
                try
                {
                    type = IndexStorage.ReadIndexType(name, environment);
                }
                catch (Exception e)
                {
                    if (environment.NextWriteTransactionId == 2 && TryFindIndexDefinition(name, documentDatabase.ReadDatabaseRecord(), out var staticDef, out var autoDef))
                    {
                        // initial transaction creating the schema hasn't completed
                        // let's try to create it again

                        environment.Dispose();

                        if (staticDef != null)
                        {
                            switch (staticDef.Type)
                            {
                                case IndexType.Map:
                                case IndexType.JavaScriptMap:
                                    return MapIndex.CreateNew(staticDef, documentDatabase);
                                case IndexType.MapReduce:
                                case IndexType.JavaScriptMapReduce:
                                    return MapReduceIndex.CreateNew(staticDef, documentDatabase);
                            }
                        }
                        else
                        {
                            var definition = IndexStore.CreateAutoDefinition(autoDef);

                            if (definition is AutoMapIndexDefinition autoMapDef)
                                return AutoMapIndex.CreateNew(autoMapDef, documentDatabase);
                            if (definition is AutoMapReduceIndexDefinition autoMapReduceDef)
                                return AutoMapReduceIndex.CreateNew(autoMapReduceDef, documentDatabase);
                        }
                    }

                    throw new IndexOpenException(
                        $"Could not read index type from storage in '{path}'. This indicates index data file corruption.",
                        e);
                }

                switch (type)
                {
                    case IndexType.AutoMap:
                        return AutoMapIndex.Open(environment, documentDatabase);
                    case IndexType.AutoMapReduce:
                        return AutoMapReduceIndex.Open(environment, documentDatabase);
                    case IndexType.Map:
                    case IndexType.JavaScriptMap:
                        return MapIndex.Open(environment, documentDatabase);
                    case IndexType.MapReduce:
                    case IndexType.JavaScriptMapReduce:
                        return MapReduceIndex.Open(environment, documentDatabase);
                    default:
                        throw new ArgumentException($"Unknown index type {type} for index {name}");
                }
            }
            catch (Exception e)
            {
                if (environment != null)
                    environment.Dispose();
                else
                    options.Dispose();

                if (e is IndexOpenException)
                    throw;

                throw new IndexOpenException($"Could not open index from '{path}'.", e);
            }
        }

        public IndexType Type { get; }

        public IndexState State { get; protected set; }

        public IndexDefinitionBase Definition { get; private set; }

        public string Name => Definition?.Name;

        public int MaxNumberOfOutputsPerDocument { get; private set; }

        public virtual IndexRunningStatus Status
        {
            get
            {
                if (_indexingThread != null)
                    return IndexRunningStatus.Running;

                if (Configuration.Disabled || State == IndexState.Disabled)
                    return IndexRunningStatus.Disabled;

                return IndexRunningStatus.Paused;
            }
        }

        public virtual bool HasBoostedFields => false;

        public virtual bool IsMultiMap => false;

        public virtual void ResetIsSideBySideAfterReplacement() { }

        public AsyncManualResetEvent.FrozenAwaiter GetIndexingBatchAwaiter()
        {
            if (_disposeOne.Disposed)
                ThrowObjectDisposed();

            return _indexingBatchCompleted.GetFrozenAwaiter();
        }

        internal static void ThrowObjectDisposed()
        {
            throw new ObjectDisposedException("index");
        }

        protected void Initialize(DocumentDatabase documentDatabase, IndexingConfiguration configuration, PerformanceHintsConfiguration performanceHints)
        {
            _logger = LoggingSource.Instance.GetLogger<Index>(documentDatabase.Name);
            using (DrainRunningQueries())
            {
                if (_initialized)
                    throw new InvalidOperationException($"Index '{Name}' was already initialized.");

                var options = CreateStorageEnvironmentOptions(documentDatabase, configuration);

                StorageEnvironment storageEnvironment = null;
                try
                {
                    storageEnvironment = LayoutUpdater.OpenEnvironment(options);
                    Initialize(storageEnvironment, documentDatabase, configuration, performanceHints);
                }
                catch (Exception)
                {
                    storageEnvironment?.Dispose();
                    options.Dispose();
                    throw;
                }
            }
        }

        private StorageEnvironmentOptions CreateStorageEnvironmentOptions(DocumentDatabase documentDatabase, IndexingConfiguration configuration)
        {
            var name = IndexDefinitionBase.GetIndexNameSafeForFileSystem(Name);

            var indexPath = configuration.StoragePath.Combine(name);

            var indexTempPath = configuration.TempPath?.Combine(name);

            var options = configuration.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly(indexPath.FullPath, indexTempPath?.FullPath ?? Path.Combine(indexPath.FullPath, "Temp"),
                    documentDatabase.IoChanges, documentDatabase.CatastrophicFailureNotification)
                : StorageEnvironmentOptions.ForPath(indexPath.FullPath, indexTempPath?.FullPath, null,
                    documentDatabase.IoChanges, documentDatabase.CatastrophicFailureNotification);

            InitializeOptions(options, documentDatabase, name);

            return options;
        }

        private static void InitializeOptions(StorageEnvironmentOptions options, DocumentDatabase documentDatabase, string name, bool schemaUpgrader = true)
        {
            options.OnNonDurableFileSystemError += documentDatabase.HandleNonDurableFileSystemError;
            options.OnRecoveryError += (s, e) => documentDatabase.HandleOnIndexRecoveryError(name, s, e);
            options.CompressTxAboveSizeInBytes = documentDatabase.Configuration.Storage.CompressTxAboveSize.GetValue(SizeUnit.Bytes);
            options.ForceUsing32BitsPager = documentDatabase.Configuration.Storage.ForceUsing32BitsPager;
            options.TimeToSyncAfterFlashInSec = (int)documentDatabase.Configuration.Storage.TimeToSyncAfterFlash.AsTimeSpan.TotalSeconds;
            options.NumOfConcurrentSyncsPerPhysDrive = documentDatabase.Configuration.Storage.NumberOfConcurrentSyncsPerPhysicalDrive;
            options.MasterKey = documentDatabase.MasterKey?.ToArray(); //clone
            options.DoNotConsiderMemoryLockFailureAsCatastrophicError = documentDatabase.Configuration.Security.DoNotConsiderMemoryLockFailureAsCatastrophicError;
            if (documentDatabase.Configuration.Storage.MaxScratchBufferSize.HasValue)
                options.MaxScratchBufferSize = documentDatabase.Configuration.Storage.MaxScratchBufferSize.Value.GetValue(SizeUnit.Bytes);
            options.PrefetchSegmentSize = documentDatabase.Configuration.Storage.PrefetchBatchSize.GetValue(SizeUnit.Bytes);
            options.PrefetchResetThreshold = documentDatabase.Configuration.Storage.PrefetchResetThreshold.GetValue(SizeUnit.Bytes);

            if (documentDatabase.ServerStore.GlobalIndexingScratchSpaceMonitor != null)
                options.ScratchSpaceUsage.AddMonitor(documentDatabase.ServerStore.GlobalIndexingScratchSpaceMonitor);

            if (schemaUpgrader)
            {
                options.SchemaVersion = SchemaUpgrader.CurrentVersion.IndexVersion;
                options.SchemaUpgrader = SchemaUpgrader.Upgrader(SchemaUpgrader.StorageType.Index, null, null);
            }
        }

        internal ExitWriteLock DrainRunningQueries()
        {
            if (_currentlyRunningQueriesLock.IsWriteLockHeld)
                return new ExitWriteLock();

            if (_currentlyRunningQueriesLock.TryEnterWriteLock(TimeSpan.FromSeconds(10)) == false)
            {
                if (_disposeOne.Disposed)
                    ThrowObjectDisposed();

                throw new TimeoutException("After waiting for 10 seconds for all running queries ");
            }

            return new ExitWriteLock(_currentlyRunningQueriesLock);
        }

        protected void Initialize(
            StorageEnvironment environment,
            DocumentDatabase documentDatabase,
            IndexingConfiguration configuration,
            PerformanceHintsConfiguration performanceHints)
        {
            if (_disposeOne.Disposed)
                throw new ObjectDisposedException($"Index '{Name}' was already disposed.");

            using (DrainRunningQueries())
            {
                if (_initialized)
                    throw new InvalidOperationException($"Index '{Name}' was already initialized.");

                InitializeInternal(environment, documentDatabase, configuration, performanceHints);
            }
        }

        private void InitializeInternal(StorageEnvironment environment, DocumentDatabase documentDatabase, IndexingConfiguration configuration,
            PerformanceHintsConfiguration performanceHints)
        {
            try
            {
                Debug.Assert(Definition != null);

                DocumentDatabase = documentDatabase;
                Configuration = configuration;
                PerformanceHints = performanceHints;

                _environment = environment;
                var safeName = IndexDefinitionBase.GetIndexNameSafeForFileSystem(Name);
                _unmanagedBuffersPool = new UnmanagedBuffersPoolWithLowMemoryHandling($"Indexes//{safeName}");
                _contextPool = new TransactionContextPool(_environment);
                _indexStorage = new IndexStorage(this, _contextPool, documentDatabase);
                _logger = LoggingSource.Instance.GetLogger<Index>(documentDatabase.Name);
                _indexStorage.Initialize(_environment);
                IndexPersistence = new LuceneIndexPersistence(this);
                IndexPersistence.Initialize(_environment);

                LoadValues();

                DocumentDatabase.TombstoneCleaner.Subscribe(this);

                DocumentDatabase.Changes.OnIndexChange += HandleIndexChange;

                _indexValidationStalenessCheck = () =>
                {
                    if (_indexingProcessCancellationTokenSource.IsCancellationRequested)
                        return true;

                    using (DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
                    using (documentsContext.OpenReadTransaction())
                    {
                        return IsStale(documentsContext);
                    }
                };

                OnInitialization();

                if (LastIndexingTime != null)
                    _didWork = true;

                _initialized = true;
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        protected virtual void OnInitialization()
        {
            _indexWorkers = CreateIndexWorkExecutors();
        }

        protected virtual void LoadValues()
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                State = _indexStorage.ReadState(tx);
                _lastQueryingTime = DocumentDatabase.Time.GetUtcNow();
                LastIndexingTime = _indexStorage.ReadLastIndexingTime(tx);
                MaxNumberOfOutputsPerDocument = _indexStorage.ReadStats(tx).MaxNumberOfOutputsPerDocument;
            }
        }

        public virtual void Start()
        {
            if (_disposeOne.Disposed)
                throw new ObjectDisposedException($"Index '{Name}' was already disposed.");

            if (_initialized == false)
                throw new InvalidOperationException($"Index '{Name}' was not initialized.");

            if (DocumentDatabase.IndexStore.IsDisposed.IsRaised())
            {
                Dispose();
                return;
            }

            using (DrainRunningQueries())
            {
                StartIndexingThread();
            }
        }

        private void StartIndexingThread()
        {
            if (_indexingThread != null &&
                _indexingThread != PoolOfThreads.LongRunningWork.Current &&
                _indexingThread.Join(0) != true)
                throw new InvalidOperationException($"Index '{Name}' is executing.");

            if (Configuration.Disabled)
                return;

            if (State == IndexState.Disabled || State == IndexState.Error)
                return;

            SetState(State);

            _indexingProcessCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(DocumentDatabase.DatabaseShutdown);
            _indexDisabled = false;

            _indexingThread = PoolOfThreads.GlobalRavenThreadPool.LongRunning(x =>
            {
                try
                {
                    PoolOfThreads.LongRunningWork.CurrentPooledThread.SetThreadAffinity(
                        DocumentDatabase.Configuration.Server.NumberOfUnusedCoresByIndexes,
                        DocumentDatabase.Configuration.Server.IndexingAffinityMask);
                    LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
                    ExecuteIndexing();
                }
                catch (Exception e)
                {
                    if (_logger.IsOperationsEnabled)
                    {
                        _logger.Operations($"Failed to execute indexing in {IndexingThreadName}", e);
                    }
                }
            }, null, IndexingThreadName);
        }

        public virtual void Stop(bool disableIndex = false)
        {
            if (_disposeOne.Disposed)
                throw new ObjectDisposedException($"Index '{Name}' was already disposed.");

            if (_initialized == false)
                throw new InvalidOperationException($"Index '{Name}' was not initialized.");

            PoolOfThreads.LongRunningWork waiter;
            using (DrainRunningQueries())
            {
                waiter = GetWaitForIndexingThreadToExit(disableIndex);
            }
            // outside the DrainRunningQueries loop
            waiter?.Join(Timeout.Infinite);
        }

        private PoolOfThreads.LongRunningWork GetWaitForIndexingThreadToExit(bool disableIndex)
        {
            if (disableIndex)
            {
                lock (_disablingIndexLock)
                {
                    _indexDisabled = true;
                    _mre.Set();
                }
            }
            else
            {
                _indexingProcessCancellationTokenSource?.Cancel();
            }

            if (_indexingThread == null)
                return null;


            var indexingThread = _indexingThread;
            _indexingThread = null;

            if (PoolOfThreads.LongRunningWork.Current != indexingThread)
                return indexingThread;

            // cancellation was requested, the thread will exit the indexing loop and terminate.
            // if we invoke Thread.Join from the indexing thread itself it will cause a deadlock
            return null;
        }

        public virtual void Update(IndexDefinitionBase definition, IndexingConfiguration configuration)
        {
            Debug.Assert(Type.IsStatic());

            using (DrainRunningQueries())
            {
                var status = Status;
                if (status == IndexRunningStatus.Running)
                    Stop();

                _indexStorage.WriteDefinition(definition);

                Definition = definition;
                Configuration = configuration;

                OnInitialization();

                _priorityChanged.Raise();

                if (status == IndexRunningStatus.Running)
                    Start();
            }
        }

        private DisposeOnce<SingleAttempt> _disposeOne;

        public void Dispose()
        {
            _disposeOne.Dispose();
        }

        public bool IsStale(DocumentsOperationContext databaseContext, long? cutoff = null, List<string> stalenessReasons = null)
        {
            using (CurrentlyInUse(out var valid))
            {
                Debug.Assert(databaseContext.Transaction != null);

                if (valid == false)
                {
                    stalenessReasons?.Add("Storage operation is running.");

                    return true;
                }

                if (Type == IndexType.Faulty)
                {
                    stalenessReasons?.Add("Index is faulty.");

                    return true;
                }

                using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
                using (indexContext.OpenReadTransaction())
                {
                    return IsStale(databaseContext, indexContext, cutoff: cutoff, referenceCutoff: cutoff, stalenessReasons: stalenessReasons);
                }
            }
        }

        public enum IndexProgressStatus
        {
            Faulty = -1,
            RunningStorageOperation = -2,
        }

        public virtual (bool IsStale, long LastProcessedEtag) GetIndexStats(DocumentsOperationContext databaseContext)
        {
            Debug.Assert(databaseContext.Transaction != null);

            if (Type == IndexType.Faulty)
                return (true, (long)IndexProgressStatus.Faulty);

            using (CurrentlyInUse(out var valid))
            {
                if (valid == false)
                    return (true, (long)IndexProgressStatus.RunningStorageOperation);

                using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
                using (indexContext.OpenReadTransaction())
                {
                    long lastEtag = 0;
                    foreach (var collection in Collections)
                    {
                        lastEtag = Math.Max(lastEtag, _indexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection));
                    }

                    var isStale = IsStale(databaseContext, indexContext);
                    return (isStale, lastEtag);
                }
            }
        }


        protected virtual bool IsStale(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, long? cutoff = null, long? referenceCutoff = null, List<string> stalenessReasons = null)
        {
            if (Type == IndexType.Faulty)
                return true;

            foreach (var collection in Collections)
            {
                var lastDocEtag = GetLastDocumentEtagInCollection(databaseContext, collection);

                var lastProcessedDocEtag = _indexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection);
                var lastProcessedTombstoneEtag =
                    _indexStorage.ReadLastProcessedTombstoneEtag(indexContext.Transaction, collection);

                _inMemoryIndexProgress.TryGetValue(collection, out var stats);

                if (cutoff == null)
                {
                    if (lastDocEtag > lastProcessedDocEtag)
                    {
                        if (stalenessReasons == null)
                            return true;

                        var lastDoc = DocumentDatabase.DocumentsStorage.GetByEtag(databaseContext, lastDocEtag);

                        var message = $"There are still some documents to process from collection '{collection}'. " +
                                   $"The last document etag in that collection is '{lastDocEtag:#,#;;0}' " +
                                   $"({Constants.Documents.Metadata.Id}: '{lastDoc.Id}', " +
                                   $"{Constants.Documents.Metadata.LastModified}: '{lastDoc.LastModified}'), " +
                                   $"but last committed document etag for that collection is '{lastProcessedDocEtag:#,#;;0}'";
                        if (stats != null)
                            message += $" (last processed etag is: '{stats.LastProcessedDocumentEtag:#,#;;0}')";

                        stalenessReasons.Add(message);
                    }

                    var lastTombstoneEtag = GetLastTombstoneEtagInCollection(databaseContext, collection);

                    if (lastTombstoneEtag > lastProcessedTombstoneEtag)
                    {
                        if (stalenessReasons == null)
                            return true;

                        var lastTombstone = DocumentDatabase.DocumentsStorage.GetTombstoneByEtag(databaseContext, lastTombstoneEtag);

                        var message = $"There are still some tombstones to process from collection '{collection}'. " +
                                   $"The last tombstone etag in that collection is '{lastTombstoneEtag:#,#;;0}' " +
                                   $"({Constants.Documents.Metadata.Id}: '{lastTombstone.LowerId}', " +
                                   $"{Constants.Documents.Metadata.LastModified}: '{lastTombstone.LastModified}'), " +
                                   $"but last committed tombstone etag for that collection is '{lastProcessedTombstoneEtag:#,#;;0}'.";
                        if (stats != null)
                            message += $" (last processed etag is: '{stats.LastProcessedTombstoneEtag:#,#;;0}')";

                        stalenessReasons.Add(message);
                    }
                }
                else
                {
                    var minDocEtag = Math.Min(cutoff.Value, lastDocEtag);
                    if (minDocEtag > lastProcessedDocEtag)
                    {
                        if (stalenessReasons == null)
                            return true;

                        var lastDoc = DocumentDatabase.DocumentsStorage.GetByEtag(databaseContext, lastDocEtag);

                        var message = $"There are still some documents to process from collection '{collection}'. " +
                                   $"The last document etag in that collection is '{lastDocEtag:#,#;;0}' " +
                                   $"({Constants.Documents.Metadata.Id}: '{lastDoc.Id}', " +
                                   $"{Constants.Documents.Metadata.LastModified}: '{lastDoc.LastModified}') " +
                                   $"with cutoff set to '{cutoff.Value}', " +
                                   $"but last committed document etag for that collection is '{lastProcessedDocEtag:#,#;;0}'.";
                        if (stats != null)
                            message += $" (last processed etag is: '{stats.LastProcessedDocumentEtag:#,#;;0}')";

                        stalenessReasons.Add(message);
                    }

                    var hasTombstones = DocumentDatabase.DocumentsStorage.HasTombstonesWithEtagGreaterThanStartAndLowerThanOrEqualToEnd(databaseContext,
                        collection,
                        lastProcessedTombstoneEtag,
                        cutoff.Value);
                    if (hasTombstones)
                    {
                        if (stalenessReasons == null)
                            return true;

                        stalenessReasons.Add($"There are still tombstones to process from collection '{collection}' " +
                                             $"with etag range '{lastProcessedTombstoneEtag} - {cutoff.Value}'.");
                    }
                }
            }

            return stalenessReasons?.Count > 0;
        }

        public long GetLastMappedEtagFor(string collection)
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (var tx = context.OpenReadTransaction())
                {
                    return _indexStorage.ReadLastIndexedEtag(tx, collection);
                }
            }
        }

        /// <summary>
        /// This should only be used for testing purposes.
        /// </summary>
        public Dictionary<string, long> GetLastMappedEtagsForDebug()
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (var tx = context.OpenReadTransaction())
                {
                    var etags = new Dictionary<string, long>();
                    foreach (var collection in Collections)
                    {
                        etags[collection] = _indexStorage.ReadLastIndexedEtag(tx, collection);
                    }

                    return etags;
                }
            }
        }

        protected void ExecuteIndexing()
        {
            _priorityChanged.Raise();
            NativeMemory.EnsureRegistered();
            using (CultureHelper.EnsureInvariantCulture())
            {
                // if we are starting indexing e.g. manually after failure
                // we need to reset errors to give it a chance
                ResetErrors();

                var storageEnvironment = _environment;
                if (storageEnvironment == null)
                    return; // can be null if we disposed immediately
                try
                {
                    _contextPool.SetMostWorkInGoingToHappenOnThisThread();

                    DocumentDatabase.Changes.OnDocumentChange += HandleDocumentChange;
                    storageEnvironment.OnLogsApplied += HandleLogsApplied;

                    while (true)
                    {
                        lock (_disablingIndexLock)
                        {
                            if (_indexDisabled)
                                return;

                            _mre.Reset();
                        }

                        _scratchSpaceLimitExceeded = false;

                        if (_priorityChanged)
                            ChangeIndexThreadPriority();

                        if (_definitionChanged)
                            PersistIndexDefinition();

                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Starting indexing for '{Name}'.");

                        var stats = _lastStats = new IndexingStatsAggregator(DocumentDatabase.IndexStore.Identities.GetNextIndexingStatsId(), _lastStats);
                        LastIndexingTime = stats.StartTime;

                        AddIndexingPerformance(stats);

                        var batchCompleted = false;

                        bool didWork = false;

                        try
                        {
                            using (var scope = stats.CreateScope())
                            {
                                try
                                {
                                    _indexingProcessCancellationTokenSource.Token.ThrowIfCancellationRequested();

                                    try
                                    {
                                        TimeSpentIndexing.Start();
                                        var lastAllocatedBytes = GC.GetAllocatedBytesForCurrentThread();

                                        didWork = DoIndexingWork(scope, _indexingProcessCancellationTokenSource.Token);
                                        batchCompleted = true;
                                        lastAllocatedBytes = GC.GetAllocatedBytesForCurrentThread() - lastAllocatedBytes;
                                        scope.AddAllocatedBytes(lastAllocatedBytes);
                                    }
                                    finally
                                    {
                                        if (_batchStopped)
                                        {
                                            _batchStopped = false;
                                            DocumentDatabase.IndexStore.StoppedConcurrentIndexBatches.Release();
                                        }

                                        _threadAllocations.CurrentlyAllocatedForProcessing = 0;
                                        _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;

                                        _inMemoryIndexProgress.Clear();
                                        TimeSpentIndexing.Stop();
                                    }

                                    _indexingBatchCompleted.SetAndResetAtomically();

                                    if (didWork)
                                    {
                                        ResetErrors();
                                        _hadRealIndexingWorkToDo.Raise();
                                    }

                                    if (_logger.IsInfoEnabled)
                                        _logger.Info($"Finished indexing for '{Name}'.'");
                                }
                                catch (OutOfMemoryException oome)
                                {
                                    HandleOutOfMemoryException(oome, scope);
                                }
                                catch (VoronUnrecoverableErrorException ide)
                                {
                                    HandleIndexCorruption(scope, ide);
                                }
                                catch (IndexCorruptionException ice)
                                {
                                    HandleIndexCorruption(scope, ice);
                                }
                                catch (IndexWriteException iwe)
                                {
                                    HandleWriteErrors(scope, iwe);
                                }
                                catch (IndexAnalyzerException iae)
                                {
                                    HandleAnalyzerErrors(scope, iae);
                                }
                                catch (CriticalIndexingException cie)
                                {
                                    HandleCriticalErrors(scope, cie);
                                }
                                catch (ExcessiveNumberOfReduceErrorsException enre)
                                {
                                    HandleExcessiveNumberOfReduceErrors(scope, enre);
                                }
                                catch (OperationCanceledException)
                                {
                                    // We are here only in the case of indexing process cancellation.
                                    scope.RecordMapCompletedReason("Operation canceled.");
                                    return;
                                }
                                catch (Exception e)
                                {
                                    HandleUnexpectedErrors(scope, e);
                                }

                                try
                                {
                                    using (_environment.Options.SkipCatastrophicFailureAssertion()) // we really want to store errors
                                    {
                                        var failureInformation = _indexStorage.UpdateStats(stats.StartTime, stats.ToIndexingBatchStats());
                                        HandleIndexFailureInformation(failureInformation);
                                    }
                                }
                                catch (VoronUnrecoverableErrorException vuee)
                                {
                                    HandleIndexCorruption(scope, vuee);
                                }
                                catch (Exception e)
                                {
                                    if (_logger.IsInfoEnabled)
                                        _logger.Info($"Could not update stats for '{Name}'.", e);
                                }

                                try
                                {
                                    if (ShouldReplace())
                                    {
                                        var originalName = Name.Replace(Constants.Documents.Indexing.SideBySideIndexNamePrefix, string.Empty, StringComparison.InvariantCultureIgnoreCase);
                                        _isReplacing = true;

                                        try
                                        {
                                            // this can fail if the indexes lock is currently held, so we'll retry
                                            // however, we might be requested to shutdown, so we want to skip replacing
                                            // in this case, worst case scenario we'll handle this in the next batch
                                            while (_indexingProcessCancellationTokenSource.IsCancellationRequested == false)
                                            {
                                                if (DocumentDatabase.IndexStore.TryReplaceIndexes(originalName, Definition.Name))
                                                    break;
                                            }
                                        }
                                        finally
                                        {
                                            _isReplacing = false;
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    if (_logger.IsInfoEnabled)
                                        _logger.Info($"Could not replace index '{Name}'.", e);
                                }
                            }
                        }
                        finally
                        {
                            stats.Complete();
                        }

                        if (batchCompleted)
                        {
                            DocumentDatabase.Changes.RaiseNotifications(new IndexChange
                            {
                                Name = Name,
                                Type = IndexChangeTypes.BatchCompleted
                            });

                            if (didWork)
                            {
                                _didWork = true;
                                _firstBatchTimeout = null;
                            }

                            var batchCompletedAction = DocumentDatabase.IndexStore.IndexBatchCompleted;
                            batchCompletedAction?.Invoke((Name, didWork));
                        }

                        try
                        {
                            if (_lowMemoryFlag.IsRaised())
                            {
                                // we can reduce the sizes of the mapped temp files
                                storageEnvironment.Cleanup();
                            }

                            // the logic here is that unless we hit the memory limit on the system, we want to retain our
                            // allocated memory as long as we still have work to do (since we will reuse it on the next batch)
                            // and it is probably better to avoid alloc/free jitter.
                            // This is because faster indexes will tend to allocate the memory faster, and we want to give them
                            // all the available resources so they can complete faster.
                            var timeToWaitForMemoryCleanup = 5000;
                            if (_allocationCleanupNeeded)
                            {
                                timeToWaitForMemoryCleanup = 0; // if there is nothing to do, immediately cleanup everything

                                // at any rate, we'll reduce the budget for this index to what it currently has allocated to avoid
                                // the case where we freed memory at the end of the batch, but didn't adjust the budget accordingly
                                // so it will think that it can allocate more than it actually should
                                _currentMaximumAllowedMemory = Size.Min(_currentMaximumAllowedMemory,
                                    new Size(NativeMemory.CurrentThreadStats.TotalAllocated, SizeUnit.Bytes));
                            }

                            if (_scratchSpaceLimitExceeded)
                            {
                                if (_logger.IsInfoEnabled)
                                    _logger.Info($"Indexing exceeded global limit for scratch space usage. Going to flush environment of '{Name}' index and forcing sync of data file");

                                GlobalFlushingBehavior.GlobalFlusher.Value.MaybeFlushEnvironment(storageEnvironment);
                                GlobalFlushingBehavior.GlobalFlusher.Value.ForceSyncEnvironment(storageEnvironment);

                                if (_logsAppliedEvent.Wait(Configuration.MaxTimeToWaitAfterFlushAndSyncWhenExceedingScratchSpaceLimit.AsTimeSpan))
                                {
                                    // we've just flushed let's try to cleanup scratch space immediately
                                    storageEnvironment.Cleanup();
                                }
                            }

                            if (_mre.Wait(timeToWaitForMemoryCleanup, _indexingProcessCancellationTokenSource.Token) == false)
                            {
                                _allocationCleanupNeeded = false;

                                // there is no work to be done, and hasn't been for a while,
                                // so this is a good time to release resources we won't need 
                                // anytime soon
                                ReduceMemoryUsage();

                                WaitHandle.WaitAny(new[]
                                    {_mre.WaitHandle, _logsAppliedEvent.WaitHandle, _indexingProcessCancellationTokenSource.Token.WaitHandle});

                                if (_logsAppliedEvent.IsSet && _mre.IsSet == false && _indexingProcessCancellationTokenSource.IsCancellationRequested == false)
                                {
                                    _hadRealIndexingWorkToDo.Lower();
                                    storageEnvironment.Cleanup();
                                    _logsAppliedEvent.Reset();
                                }
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            return;
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // expected
                }
                catch (Exception e)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Unexpected error in '{Name}' index. This should never happen.", e);

                    try
                    {
                        DocumentDatabase.NotificationCenter.Add(AlertRaised.Create(DocumentDatabase.Name, $"Unexpected error in '{Name}' index",
                            "Unexpected error in indexing thread. See details.", AlertType.Indexing_UnexpectedIndexingThreadError, NotificationSeverity.Error,
                            details: new ExceptionDetails(e)));
                    }
                    catch (Exception)
                    {
                        // ignore if we can't create notification
                    }

                    State = IndexState.Error;
                }
                finally
                {
                    storageEnvironment.OnLogsApplied -= HandleLogsApplied;
                    if (DocumentDatabase != null)
                        DocumentDatabase.Changes.OnDocumentChange -= HandleDocumentChange;
                }
            }
        }

        public void Cleanup()
        {
            _environment?.Cleanup();
        }

        protected virtual bool ShouldReplace()
        {
            return false;
        }

        private void ChangeIndexThreadPriority()
        {
            _priorityChanged.Lower();

            ThreadPriority newPriority;
            var priority = Definition.Priority;
            switch (priority)
            {
                case IndexPriority.Low:
                    newPriority = ThreadPriority.Lowest;
                    break;
                case IndexPriority.Normal:
                    newPriority = ThreadPriority.BelowNormal;
                    break;
                case IndexPriority.High:
                    newPriority = ThreadPriority.Normal;
                    break;
                default:
                    throw new NotSupportedException($"Unknown priority: {priority}");
            }

            var currentPriority = Thread.CurrentThread.Priority;
            if (currentPriority == newPriority)
                return;

            Thread.CurrentThread.Priority = newPriority;
        }

        private void PersistIndexDefinition()
        {
            try
            {
                _indexStorage.WriteDefinition(Definition);

                _definitionChanged.Lower();
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Failed to persist definition of '{Name}' index", e);
            }
        }

        private void HandleLogsApplied()
        {
            if (_hadRealIndexingWorkToDo)
                _logsAppliedEvent.Set();
        }

        private void ReduceMemoryUsage()
        {
            var beforeFree = NativeMemory.CurrentThreadStats.TotalAllocated;
            if (_logger.IsInfoEnabled)
                _logger.Info(
                    $"{new Size(beforeFree, SizeUnit.Bytes)} is used by '{Name}', reducing memory utilization.");

            DocumentDatabase.DocumentsStorage.ContextPool.Clean();
            _contextPool.Clean();
            ByteStringMemoryCache.CleanForCurrentThread();
            IndexPersistence.Clean();
            _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;

            var afterFree = NativeMemory.CurrentThreadStats.TotalAllocated;
            if (_logger.IsInfoEnabled)
                _logger.Info($"After cleanup, using {new Size(afterFree, SizeUnit.Bytes)} by '{Name}'.");
        }

        internal void ResetErrors()
        {
            Interlocked.Exchange(ref _writeErrors, 0);
            Interlocked.Exchange(ref _unexpectedErrors, 0);
            Interlocked.Exchange(ref _analyzerErrors, 0);
        }

        internal void HandleAnalyzerErrors(IndexingStatsScope stats, IndexAnalyzerException iae)
        {
            if (_logger.IsOperationsEnabled)
                _logger.Operations($"Analyzer error occurred for '{Name}'.", iae);

            stats.AddAnalyzerError(iae);

            var analyzerErrors = Interlocked.Increment(ref _analyzerErrors);

            if (State == IndexState.Error || analyzerErrors < AnalyzerErrorLimit)
                return;

            _errorStateReason = $"State was changed due to excessive number of analyzer errors ({analyzerErrors}).";
            SetState(IndexState.Error);
        }

        internal void HandleUnexpectedErrors(IndexingStatsScope stats, Exception e)
        {
            if (_logger.IsOperationsEnabled)
                _logger.Operations($"Unexpected exception occurred for '{Name}'.", e);

            stats.AddUnexpectedError(e);

            var unexpectedErrors = Interlocked.Increment(ref _unexpectedErrors);

            if (State == IndexState.Error || unexpectedErrors < UnexpectedErrorsLimit)
                return;

            _errorStateReason = $"State was changed due to excessive number of unexpected errors ({unexpectedErrors}).";
            SetState(IndexState.Error);
        }

        internal void HandleCriticalErrors(IndexingStatsScope stats, CriticalIndexingException e)
        {
            if (_logger.IsOperationsEnabled)
                _logger.Operations($"Critical exception occurred for '{Name}'.", e);

            if (State == IndexState.Error)
                return;

            _errorStateReason = $"State was changed due to a critical error. Error message: {e.Message}";
            SetState(IndexState.Error);
        }

        internal void HandleWriteErrors(IndexingStatsScope stats, IndexWriteException iwe)
        {
            if (_logger.IsOperationsEnabled)
                _logger.Operations($"Write exception occurred for '{Name}'.", iwe);

            stats.AddWriteError(iwe);

            if (iwe.InnerException is SystemException) // Don't count transient errors
                return;

            var writeErrors = Interlocked.Increment(ref _writeErrors);

            if (State == IndexState.Error || writeErrors < WriteErrorsLimit)
                return;

            _errorStateReason = $"State was changed due to excessive number of write errors ({writeErrors}).";
            SetState(IndexState.Error);
        }

        internal void HandleExcessiveNumberOfReduceErrors(IndexingStatsScope stats, ExcessiveNumberOfReduceErrorsException e)
        {
            if (_logger.IsOperationsEnabled)
                _logger.Operations($"Erroring index due to excessive number of reduce errors '{Name}'.", e);

            stats.AddExcessiveNumberOfReduceErrors(e);

            if (State == IndexState.Error)
                return;

            _errorStateReason = e.Message;
            SetState(IndexState.Error);
        }

        private void HandleOutOfMemoryException(OutOfMemoryException oome, IndexingStatsScope scope)
        {
            try
            {
                scope.AddMemoryError(oome);
                Interlocked.Add(ref _lowMemoryPressure, 10);
                _lowMemoryFlag.Raise();

                var title = $"Out of memory occurred for '{Name}'";
                if (_logger.IsInfoEnabled)
                    _logger.Info(title, oome);

                var message = $"Error message: {oome.Message}";
                var alert = AlertRaised.Create(
                    null,
                    title,
                    message,
                    AlertType.OutOfMemoryException,
                    NotificationSeverity.Error,
                    key: message,
                    details: new MessageDetails
                    {
                        Message = OutOfMemoryDetails(oome)
                    });

                DocumentDatabase.NotificationCenter.Add(alert);
            }
            catch (OutOfMemoryException)
            {
                // nothing to do here
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Failed out of memory exception handling for index '{Name}'", e);
            }
        }

        private static string OutOfMemoryDetails(OutOfMemoryException oome)
        {
            var memoryInfo = MemoryInformation.GetMemInfoUsingOneTimeSmapsReader();

            return $"Managed memory: {new Size(MemoryInformation.GetManagedMemoryInBytes(), SizeUnit.Bytes)}, " +
                   $"Unmanaged allocations: {new Size(MemoryInformation.GetUnManagedAllocationsInBytes(), SizeUnit.Bytes)}, " +
                   $"Shared clean: {memoryInfo.SharedCleanMemory}, " +
                   $"Working set: {memoryInfo.WorkingSet}, " +
                   $"Available memory: {memoryInfo.AvailableMemory}, " +
                   $"Calculated Available memory: {memoryInfo.AvailableWithoutTotalCleanMemory}, " +
                   $"Total memory: {memoryInfo.TotalPhysicalMemory} {Environment.NewLine}" +
                   $"Error: {oome}";
        }

        private void HandleIndexCorruption(IndexingStatsScope stats, Exception e)
        {
            stats.AddCorruptionError(e);

            if (_logger.IsOperationsEnabled)
                _logger.Operations($"Data corruption occurred for '{Name}'.", e);

            if (DocumentDatabase.ServerStore.DatabasesLandlord.CatastrophicFailureHandler.TryGetStats(_environment.DbId, out var corruptionStats) &&
                corruptionStats.WillUnloadDatabase)
            {
                // it can be a transient error, we are going to unload the database and do not error the index yet
                // let's stop the indexing thread

                lock (_disablingIndexLock)
                {
                    _indexDisabled = true;
                    _mre.Set();
                }

                return;
            }

            // we exceeded the number of db unloads due to corruption error, let's error the index

            _errorStateReason = $"State was changed due to data corruption with message '{e.Message}'";

            try
            {
                using (_environment.Options.SkipCatastrophicFailureAssertion()) // we really want to store Error state
                {
                    SetState(IndexState.Error);
                }
            }
            catch (Exception exception)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Unable to set the index {Name} to error state", exception);
                State = IndexState.Error; // just in case it didn't took from the SetState call
            }
        }

        private void HandleIndexFailureInformation(IndexFailureInformation failureInformation)
        {
            if (failureInformation.IsInvalidIndex(_indexValidationStalenessCheck) == false)
                return;

            var message = failureInformation.GetErrorMessage();

            if (_logger.IsOperationsEnabled)
                _logger.Operations(message);

            _errorStateReason = message;
            SetState(IndexState.Error);
        }

        public void ErrorIndexIfCriticalException(Exception e)
        {
            if (e is VoronUnrecoverableErrorException || e is PageCompressedException || e is UnexpectedReduceTreePageException)
                throw new IndexCorruptionException(e);

            if (e is InvalidProgramException ipe)
                throw new JitHitInternalLimitsOnIndexingFunction(ipe);
        }

        protected abstract IIndexingWork[] CreateIndexWorkExecutors();

        public virtual IDisposable InitializeIndexingWork(TransactionOperationContext indexContext)
        {
            return null;
        }

        public bool DoIndexingWork(IndexingStatsScope stats, CancellationToken cancellationToken)
        {
            _threadAllocations = NativeMemory.CurrentThreadStats;

            bool mightBeMore = false;

            using (DocumentDatabase.PreventFromUnloading())
            using (CultureHelper.EnsureInvariantCulture())
            using (DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext databaseContext))
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
            {
                indexContext.PersistentContext.LongLivedTransactions = true;
                databaseContext.PersistentContext.LongLivedTransactions = true;

                using (var tx = indexContext.OpenWriteTransaction())
                using (CurrentIndexingScope.Current =
                    new CurrentIndexingScope(DocumentDatabase.DocumentsStorage, databaseContext, Definition, indexContext, GetOrAddSpatialField, _unmanagedBuffersPool))
                {
                    var writeOperation = new Lazy<IndexWriteOperation>(() => IndexPersistence.OpenIndexWriter(indexContext.Transaction.InnerTransaction, indexContext));

                    try
                    {
                        using (InitializeIndexingWork(indexContext))
                        {
                            foreach (var work in _indexWorkers)
                            {
                                using (var scope = stats.For(work.Name))
                                {
                                    mightBeMore |= work.Execute(databaseContext, indexContext, writeOperation, scope,
                                        cancellationToken);

                                    if (mightBeMore)
                                        _mre.Set();
                                }
                            }

                            if (writeOperation.IsValueCreated)
                            {
                                using (var indexWriteOperation = writeOperation.Value)
                                {
                                    indexWriteOperation.Commit(stats);

                                    UpdateThreadAllocations(indexContext, indexWriteOperation, null, false);
                                }
                            }

                            _indexStorage.WriteReferences(CurrentIndexingScope.Current, tx);
                        }

                        using (stats.For(IndexingOperation.Storage.Commit))
                        {
                            tx.InnerTransaction.LowLevelTransaction.RetrieveCommitStats(out CommitStats commitStats);

                            tx.InnerTransaction.LowLevelTransaction.AfterCommitWhenNewReadTransactionsPrevented += () =>
                            {
                                if (writeOperation.IsValueCreated == false)
                                    return;

                                using (stats.For(IndexingOperation.Lucene.RecreateSearcher))
                                {
                                    // we need to recreate it after transaction commit to prevent it from seeing uncommitted changes
                                    // also we need this to be called when new read transaction are prevented in order to ensure
                                    // that queries won't get the searcher having 'old' state but see 'new' changes committed here
                                    // e.g. the old searcher could have a segment file in its in-memory state which has been removed in this tx
                                    IndexPersistence.RecreateSearcher(tx.InnerTransaction);
                                }
                            };

                            tx.Commit();
                            SlowWriteNotification.Notify(commitStats, DocumentDatabase);

                            stats.RecordCommitStats(commitStats.NumberOfModifiedPages, commitStats.NumberOf4KbsWrittenToDisk);
                        }
                    }
                    catch
                    {
                        IndexPersistence.DisposeWriters();
                        throw;
                    }
                    finally
                    {
                        if (writeOperation.IsValueCreated)
                            writeOperation.Value.Dispose();
                    }

                    return mightBeMore;
                }
            }
        }

        public abstract IIndexedDocumentsEnumerator GetMapEnumerator(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext,
            IndexingStatsScope stats, IndexType type);

        public abstract void HandleDelete(Tombstone tombstone, string collection, IndexWriteOperation writer,
            TransactionOperationContext indexContext, IndexingStatsScope stats);

        public abstract int HandleMap(LazyStringValue lowerId, LazyStringValue id, IEnumerable mapResults, IndexWriteOperation writer,
            TransactionOperationContext indexContext, IndexingStatsScope stats);

        private void HandleIndexChange(IndexChange change)
        {
            if (string.Equals(change.Name, Name, StringComparison.OrdinalIgnoreCase) == false)
                return;

            if (change.Type == IndexChangeTypes.IndexMarkedAsErrored)
                Stop();
        }

        protected virtual void HandleDocumentChange(DocumentChange change)
        {
            if (HandleAllDocs == false && Collections.Contains(change.CollectionName) == false)
                return;
            _mre.Set();
        }

        public virtual List<IndexingError> GetErrors()
        {
            using (CurrentlyInUse(out var valid))
            {
                if (valid == false)
                    return new List<IndexingError>();

                return _indexStorage.ReadErrors();
            }
        }

        public long GetErrorCount()
        {
            using (CurrentlyInUse(out var valid))
            {
                if (valid == false)
                    return 0;

                if (Type == IndexType.Faulty)
                    return 1;

                return _indexStorage.ReadErrorsCount();
            }
        }

        public DateTime? GetLastIndexingErrorTime()
        {
            using (CurrentlyInUse(out var valid))
            {
                if (valid == false || Type == IndexType.Faulty)
                    return DateTime.MinValue;

                return _indexStorage.ReadLastIndexingErrorTime();
            }
        }

        public virtual void SetPriority(IndexPriority priority)
        {
            if (Definition.Priority == priority)
                return;

            using (DrainRunningQueries())
            {
                AssertIndexState(assertState: false);

                if (Definition.Priority == priority)
                    return;

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Changing priority for '{Name}' from '{Definition.Priority}' to '{priority}'.");

                var oldPriority = Definition.Priority;

                Definition.Priority = priority;

                try
                {
                    _indexStorage.WriteDefinition(Definition, timeout: TimeSpan.FromSeconds(5));
                }
                catch (TimeoutException)
                {
                    _definitionChanged.Raise();
                    _mre.Set();
                }
                catch (Exception)
                {
                    Definition.Priority = oldPriority;
                    throw;
                }

                _priorityChanged.Raise();

                DocumentDatabase.Changes.RaiseNotifications(new IndexChange
                {
                    Name = Name,
                    Type = IndexChangeTypes.PriorityChanged
                });
            }
        }

        public virtual void SetState(IndexState state, bool inMemoryOnly = false)
        {
            if (State == state)
                return;

            using (DrainRunningQueries())
            {
                AssertIndexState(assertState: false);

                if (State == state)
                    return;

                if (state != IndexState.Error)
                    _errorStateReason = null;

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Changing state for '{Name})' from '{State}' to '{state}'.");

                var oldState = State;
                State = state;

                if (inMemoryOnly)
                    return;

                try
                {
                    // this might fail if we can't write, so we first update the in memory state
                    _indexStorage.WriteState(state);
                }
                finally
                {
                    // even if there is a failure, update it
                    var changeType = GetIndexChangeType(state, oldState);
                    if (changeType != IndexChangeTypes.None)
                    {
                        // HandleIndexChange is going to be called here
                        DocumentDatabase.Changes.RaiseNotifications(new IndexChange
                        {
                            Name = Name,
                            Type = changeType
                        });
                    }
                }
            }
        }

        private static IndexChangeTypes GetIndexChangeType(IndexState state, IndexState oldState)
        {
            var notificationType = IndexChangeTypes.None;

            if (state == IndexState.Disabled)
                notificationType = IndexChangeTypes.IndexDemotedToDisabled;
            else if (state == IndexState.Error)
                notificationType = IndexChangeTypes.IndexMarkedAsErrored;
            else if (state == IndexState.Idle)
                notificationType = IndexChangeTypes.IndexDemotedToIdle;
            else if (state == IndexState.Normal && oldState == IndexState.Idle)
                notificationType = IndexChangeTypes.IndexPromotedFromIdle;
            return notificationType;
        }

        public virtual void SetLock(IndexLockMode mode)
        {
            if (Definition.LockMode == mode)
                return;

            if (Type.IsAuto())
            {
                throw new NotSupportedException($"'Lock Mode' can't be set for the Auto-Index '{Name}'.");
            }

            using (DrainRunningQueries())
            {
                AssertIndexState(assertState: false);

                if (Definition.LockMode == mode)
                    return;

                if (_logger.IsInfoEnabled)
                    _logger.Info(
                        $"Changing lock mode for '{Name}' from '{Definition.LockMode}' to '{mode}'.");

                var oldLockMode = Definition.LockMode;

                Definition.LockMode = mode;

                try
                {
                    _indexStorage.WriteDefinition(Definition, timeout: TimeSpan.FromSeconds(5));
                }
                catch (TimeoutException)
                {
                    _definitionChanged.Raise();
                    _mre.Set();
                }
                catch (Exception)
                {
                    Definition.LockMode = oldLockMode;
                    throw;
                }

                DocumentDatabase.Changes.RaiseNotifications(new IndexChange
                {
                    Name = Name,
                    Type = IndexChangeTypes.LockModeChanged
                });
            }
        }

        public virtual void Enable()
        {
            if (State != IndexState.Disabled && State != IndexState.Error)
                return;

            using (DrainRunningQueries())
            {
                if (State != IndexState.Disabled && State != IndexState.Error)
                    return;

                SetState(IndexState.Normal);
                Start();
            }
        }

        public virtual void Disable()
        {
            if (State == IndexState.Disabled)
                return;

            using (DrainRunningQueries())
            {
                if (State == IndexState.Disabled)
                    return;

                Stop(disableIndex: true);
                SetState(IndexState.Disabled);
                Cleanup();
            }
        }

        public void Rename(string name)
        {
            _indexStorage.Rename(name);
        }

        public virtual IndexProgress GetProgress(DocumentsOperationContext documentsContext, bool? isStale = null)
        {
            using (CurrentlyInUse(out var valid))
            {
                if (valid == false || DocumentDatabase.DatabaseShutdown.IsCancellationRequested || _disposeOne.Disposed)
                {
                    return new IndexProgress
                    {
                        Name = Name,
                        Type = Type
                    };
                }

                if (_contextPool == null)
                    throw new ObjectDisposedException("Index " + Name);

                if (documentsContext.Transaction == null)
                    throw new InvalidOperationException("Cannot calculate index progress without valid transaction.");

                using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var progress = new IndexProgress
                    {
                        Name = Name,
                        Type = Type,
                        IsStale = isStale ?? IsStale(documentsContext, context)
                    };

                    var stats = _indexStorage.ReadStats(tx);

                    progress.Collections = new Dictionary<string, IndexProgress.CollectionStats>(StringComparer.OrdinalIgnoreCase);
                    progress.IndexRunningStatus = Status;

                    var indexingPerformance = _lastStats?.ToIndexingPerformanceLiveStats();
                    if (indexingPerformance?.DurationInMs > 0)
                    {
                        progress.ProcessedPerSecond = indexingPerformance.InputCount / (indexingPerformance.DurationInMs / 1000);
                    }

                    foreach (var collection in GetCollections(documentsContext, out var isAllDocs))
                    {
                        var collectionNameForStats = isAllDocs == false ? collection : Constants.Documents.Collections.AllDocumentsCollection;
                        var collectionStats = stats.Collections[collectionNameForStats];

                        var lastEtags = GetLastEtags(collectionNameForStats,
                            collectionStats.LastProcessedDocumentEtag,
                            collectionStats.LastProcessedTombstoneEtag);

                        if (progress.Collections.TryGetValue(collectionNameForStats, out var progressStats) == false)
                        {
                            progressStats = progress.Collections[collectionNameForStats] = new IndexProgress.CollectionStats
                            {
                                LastProcessedDocumentEtag = lastEtags.LastProcessedDocumentEtag,
                                LastProcessedTombstoneEtag = lastEtags.LastProcessedTombstoneEtag
                            };
                        }

                        progressStats.NumberOfDocumentsToProcess +=
                            DocumentDatabase.DocumentsStorage.GetNumberOfDocumentsToProcess(
                                documentsContext, collection, progressStats.LastProcessedDocumentEtag, out var totalCount);
                        progressStats.TotalNumberOfDocuments += totalCount;

                        progressStats.NumberOfTombstonesToProcess +=
                            DocumentDatabase.DocumentsStorage.GetNumberOfTombstonesToProcess(
                                documentsContext, collection, progressStats.LastProcessedTombstoneEtag, out totalCount);
                        progressStats.TotalNumberOfTombstones += totalCount;
                    }

                    return progress;
                }
            }
        }

        private IEnumerable<string> GetCollections(DocumentsOperationContext documentsContext, out bool isAllDocs)
        {
            if (Collections.Count == 1 && Collections.Contains(Constants.Documents.Collections.AllDocumentsCollection))
            {
                isAllDocs = true;
                return DocumentDatabase.DocumentsStorage.GetCollections(documentsContext).Select(x => x.Name);
            }

            isAllDocs = false;
            return Collections;
        }

        public IndexProgress.CollectionStats GetStats(string collection)
        {
            return _inMemoryIndexProgress.GetOrAdd(collection, _ => new IndexProgress.CollectionStats());
        }

        private (long LastProcessedDocumentEtag, long LastProcessedTombstoneEtag) GetLastEtags(
            string collection, long lastProcessedDocumentEtag, long lastProcessedTombstoneEtag)
        {
            if (_inMemoryIndexProgress.TryGetValue(collection, out var stats) == false)
                return (lastProcessedDocumentEtag, lastProcessedTombstoneEtag);

            var lastDocumentEtag = Math.Max(lastProcessedDocumentEtag, stats.LastProcessedDocumentEtag);
            var lastTombstoneEtag = Math.Max(lastProcessedTombstoneEtag, stats.LastProcessedTombstoneEtag);
            return (lastDocumentEtag, lastTombstoneEtag);
        }

        public virtual IndexStats GetStats(bool calculateLag = false, bool calculateStaleness = false,
            DocumentsOperationContext documentsContext = null)
        {
            using (CurrentlyInUse(out var valid))
            {
                if (valid == false)
                {
                    return new IndexStats()
                    {
                        Name = Name,
                        Type = Type
                    };
                }

                if (_contextPool == null)
                    throw new ObjectDisposedException("Index " + Name);

                using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                using (var reader = IndexPersistence.OpenIndexReader(tx.InnerTransaction))
                {
                    var stats = _indexStorage.ReadStats(tx);

                    stats.Name = Name;
                    stats.Type = Type;
                    stats.EntriesCount = reader.EntriesCount();
                    stats.LockMode = Definition.LockMode;
                    stats.Priority = Definition.Priority;
                    stats.State = State;
                    stats.Status = Status;

                    stats.MappedPerSecondRate = MapsPerSec.OneMinuteRate;
                    stats.ReducedPerSecondRate = ReducesPerSec.OneMinuteRate;

                    stats.LastBatchStats = _lastStats?.ToIndexingPerformanceLiveStats();
                    stats.LastQueryingTime = _lastQueryingTime;

                    if (calculateStaleness || calculateLag)
                    {
                        if (documentsContext == null)
                            throw new InvalidOperationException("Cannot calculate staleness or lag without valid context.");

                        if (documentsContext.Transaction == null)
                            throw new InvalidOperationException(
                                "Cannot calculate staleness or lag without valid transaction.");

                        if (calculateStaleness)
                            stats.IsStale = IsStale(documentsContext, context);

                        if (calculateLag)
                        {
                            foreach (var collection in Collections)
                            {
                                var collectionStats = stats.Collections[collection];

                                var lastDocumentEtag =
                                    DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(documentsContext, collection);
                                var lastTombstoneEtag =
                                    DocumentDatabase.DocumentsStorage.GetLastTombstoneEtag(documentsContext, collection);

                                collectionStats.DocumentLag = Math.Max(0,
                                    lastDocumentEtag - collectionStats.LastProcessedDocumentEtag);
                                collectionStats.TombstoneLag = Math.Max(0,
                                    lastTombstoneEtag - collectionStats.LastProcessedTombstoneEtag);
                            }
                        }
                    }

                    stats.Memory = GetMemoryStats();

                    return stats;
                }
            }
        }

        private IndexStats.MemoryStats GetMemoryStats()
        {
            var stats = new IndexStats.MemoryStats();

            var name = IndexDefinitionBase.GetIndexNameSafeForFileSystem(Name);

            var indexPath = Configuration.StoragePath.Combine(name);

            var indexTempPath = Configuration.TempPath?.Combine(name);

            var totalSize = 0L;
            foreach (var mapping in NativeMemory.FileMapping)
            {
                var directory = Path.GetDirectoryName(mapping.Key);

                var isIndexPath = string.Equals(indexPath.FullPath, directory, StringComparison.OrdinalIgnoreCase);
                var isTempPath = indexTempPath != null && string.Equals(indexTempPath.FullPath, directory, StringComparison.OrdinalIgnoreCase);

                if (isIndexPath || isTempPath)
                {
                    foreach (var singleMapping in mapping.Value.Value.Info)
                        totalSize += singleMapping.Value;
                }
            }

            stats.DiskSize.SizeInBytes = totalSize;

            var indexingThread = _indexingThread;
            if (indexingThread != null)
            {
                foreach (var threadAllocationsValue in NativeMemory.AllThreadStats)
                {
                    if (indexingThread.ManagedThreadId == threadAllocationsValue.Id)
                    {
                        stats.ThreadAllocations.SizeInBytes = threadAllocationsValue.TotalAllocated;
                        if (stats.ThreadAllocations.SizeInBytes < 0)
                            stats.ThreadAllocations.SizeInBytes = 0;
                        stats.MemoryBudget.SizeInBytes = _currentMaximumAllowedMemory.GetValue(SizeUnit.Bytes);
                        break;
                    }
                }
            }

            return stats;
        }

        public DateTime? GetLastQueryingTime()
        {
            return _lastQueryingTime;
        }

        private void MarkQueried(DateTime time)
        {
            if (_lastQueryingTime != null &&
                _lastQueryingTime.Value >= time)
                return;

            _lastQueryingTime = time;
        }

        public IndexDefinition GetIndexDefinition()
        {
            return Definition.GetOrCreateIndexDefinitionInternal();
        }

        public virtual async Task StreamQuery(HttpResponse response, IStreamDocumentQueryResultWriter writer,
            IndexQueryServerSide query, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            var result = new StreamDocumentQueryResult(response, writer, token);
            await QueryInternal(result, query, documentsContext, token);
            result.Flush();

            DocumentDatabase.QueryMetadataCache.MaybeAddToCache(query.Metadata, Name);
        }

        public virtual async Task<DocumentQueryResult> Query(IndexQueryServerSide query,
            DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            var result = new DocumentQueryResult();
            await QueryInternal(result, query, documentsContext, token);
            return result;
        }

        private async Task QueryInternal<TQueryResult>(TQueryResult resultToFill, IndexQueryServerSide query,
            DocumentsOperationContext documentsContext, OperationCancelToken token)
            where TQueryResult : QueryResultServerSide
        {
            AssertIndexState();

            if (State == IndexState.Idle)
            {
                try
                {
                    SetState(IndexState.Normal);
                }
                catch (Exception e)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Failed to change state of '{Name}' index from {IndexState.Idle} to {IndexState.Normal}. Proceeding with running the query.",
                            e);
                }
            }

            MarkQueried(DocumentDatabase.Time.GetUtcNow());

            AssertQueryDoesNotContainFieldsThatAreNotIndexed(query.Metadata);

            if (resultToFill.SupportsInclude == false
                && (query.Metadata.Includes != null && query.Metadata.Includes.Length > 0))
                throw new NotSupportedException("Includes are not supported by this type of query.");

            if (resultToFill.SupportsHighlighting == false && query.Metadata.HasHighlightings)
                throw new NotSupportedException("Highlighting is not supported by this type of query.");

            if (query.Metadata.HasHighlightings && (query.Metadata.HasIntersect || query.Metadata.HasMoreLikeThis))
                throw new NotSupportedException("Highlighting is not supported by this type of query.");

            if (resultToFill.SupportsExplanations == false && query.Metadata.HasExplanations)
                throw new NotSupportedException("Explanations are not supported by this type of query.");

            if (query.Metadata.HasExplanations && (query.Metadata.HasIntersect || query.Metadata.HasMoreLikeThis))
                throw new NotSupportedException("Explanations are not supported by this type of query.");

            using (var marker = MarkQueryAsRunning(query, token))
            {
                var queryDuration = Stopwatch.StartNew();
                AsyncWaitForIndexing wait = null;
                (long? DocEtag, long? ReferenceEtag)? cutoffEtag = null;

                var stalenessScope = query.Timings?.For(nameof(QueryTimingsScope.Names.Staleness), start: false);

                while (true)
                {
                    AssertIndexState();
                    marker.HoldLock();

                    // we take the awaiter _before_ the indexing transaction happens, 
                    // so if there are any changes, it will already happen to it, and we'll 
                    // query the index again. This is important because of: 
                    // http://issues.hibernatingrhinos.com/issue/RavenDB-5576
                    var frozenAwaiter = GetIndexingBatchAwaiter();
                    using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
                    using (var indexTx = indexContext.OpenReadTransaction())
                    {
                        documentsContext.OpenReadTransaction();
                        // we have to open read tx for mapResults _after_ we open index tx

                        bool isStale;
                        using (stalenessScope?.Start())
                        {
                            if (query.WaitForNonStaleResults && cutoffEtag == null)
                                cutoffEtag = GetCutoffEtag(documentsContext);

                            isStale = IsStale(documentsContext, indexContext, cutoffEtag?.DocEtag, cutoffEtag?.ReferenceEtag);
                            if (WillResultBeAcceptable(isStale, query, wait) == false)
                            {
                                documentsContext.CloseTransaction();

                                Debug.Assert(query.WaitForNonStaleResultsTimeout != null);

                                if (wait == null)
                                    wait = new AsyncWaitForIndexing(queryDuration, query.WaitForNonStaleResultsTimeout.Value, this);

                                marker.ReleaseLock();

                                await wait.WaitForIndexingAsync(frozenAwaiter).ConfigureAwait(false);
                                continue;
                            }
                        }

                        FillQueryResult(resultToFill, isStale, query.Metadata, documentsContext, indexContext);

                        using (var reader = IndexPersistence.OpenIndexReader(indexTx.InnerTransaction))
                        {
                            using (var queryScope = query.Timings?.For(nameof(QueryTimingsScope.Names.Query)))
                            {
                                QueryTimingsScope gatherScope = null;
                                QueryTimingsScope fillScope = null;

                                if (queryScope != null && query.Metadata.Includes?.Length > 0)
                                {
                                    var includesScope = queryScope.For(nameof(QueryTimingsScope.Names.Includes), start: false);
                                    gatherScope = includesScope.For(nameof(QueryTimingsScope.Names.Gather), start: false);
                                    fillScope = includesScope.For(nameof(QueryTimingsScope.Names.Fill), start: false);
                                }

                                var totalResults = new Reference<int>();
                                var skippedResults = new Reference<int>();
                                IncludeCountersCommand includeCountersCommand = null;

                                var fieldsToFetch = new FieldsToFetch(query, Definition);

                                var includeDocumentsCommand = new IncludeDocumentsCommand(
                                    DocumentDatabase.DocumentsStorage, documentsContext,
                                    query.Metadata.Includes);
                                if (query.Metadata.HasCounters)
                                {
                                    includeCountersCommand = new IncludeCountersCommand(
                                        DocumentDatabase,
                                        documentsContext,
                                        query.Metadata.CounterIncludes.Counters);
                                }

                                var retriever = GetQueryResultRetriever(query, queryScope, documentsContext, fieldsToFetch, includeDocumentsCommand);

                                IEnumerable<(Document Result, Dictionary<string, Dictionary<string, string[]>> Highlightings, ExplanationResult Explanation)> documents;

                                if (query.Metadata.HasMoreLikeThis)
                                {
                                    documents = reader.MoreLikeThis(
                                        query,
                                        retriever,
                                        documentsContext,
                                        token.Token);
                                }
                                else if (query.Metadata.HasIntersect)
                                {
                                    documents = reader.IntersectQuery(
                                        query,
                                        fieldsToFetch,
                                        totalResults,
                                        skippedResults,
                                        retriever,
                                        documentsContext,
                                        GetOrAddSpatialField,
                                        token.Token);
                                }
                                else
                                {
                                    documents = reader.Query(
                                        query,
                                        queryScope,
                                        fieldsToFetch,
                                        totalResults,
                                        skippedResults,
                                        retriever,
                                        documentsContext,
                                        GetOrAddSpatialField,
                                        token.Token);
                                }

                                try
                                {
                                    foreach (var document in documents)
                                    {
                                        resultToFill.TotalResults = totalResults.Value;
                                        resultToFill.AddResult(document.Result);

                                        if (document.Highlightings != null)
                                            resultToFill.AddHighlightings(document.Highlightings);

                                        if (document.Explanation != null)
                                            resultToFill.AddExplanation(document.Explanation);

                                        using (gatherScope?.Start())
                                            includeDocumentsCommand.Gather(document.Result);

                                        includeCountersCommand?.Fill(document.Result);
                                    }
                                }
                                catch (Exception e)
                                {
                                    if (resultToFill.SupportsExceptionHandling == false)
                                        throw;

                                    resultToFill.HandleException(e);
                                }

                                using (fillScope?.Start())
                                    includeDocumentsCommand.Fill(resultToFill.Includes);

                                if (includeCountersCommand != null)
                                    resultToFill.AddCounterIncludes(includeCountersCommand);

                                resultToFill.TotalResults = Math.Max(totalResults.Value, resultToFill.Results.Count);
                                resultToFill.SkippedResults = skippedResults.Value;
                                resultToFill.IncludedPaths = query.Metadata.Includes;
                            }
                        }

                        return;
                    }
                }
            }
        }

        public virtual async Task<FacetedQueryResult> FacetedQuery(FacetQuery facetQuery, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            AssertIndexState();

            if (State == IndexState.Idle)
                SetState(IndexState.Normal);

            var query = facetQuery.Query;

            MarkQueried(DocumentDatabase.Time.GetUtcNow());
            AssertQueryDoesNotContainFieldsThatAreNotIndexed(query.Metadata);

            using (var marker = MarkQueryAsRunning(query, token))
            {
                var result = new FacetedQueryResult();

                var queryDuration = Stopwatch.StartNew();
                AsyncWaitForIndexing wait = null;
                (long? DocEtag, long? ReferenceEtag)? cutoffEtag = null;

                while (true)
                {
                    AssertIndexState();
                    marker.HoldLock();

                    using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
                    {
                        // we take the awaiter _before_ the indexing transaction happens, 
                        // so if there are any changes, it will already happen to it, and we'll 
                        // query the index again. This is important because of: 
                        // http://issues.hibernatingrhinos.com/issue/RavenDB-5576
                        var frozenAwaiter = GetIndexingBatchAwaiter();
                        using (var indexTx = indexContext.OpenReadTransaction())
                        {
                            documentsContext.OpenReadTransaction();
                            // we have to open read tx for mapResults _after_ we open index tx

                            if (query.WaitForNonStaleResults && cutoffEtag == null)
                                cutoffEtag = GetCutoffEtag(documentsContext);

                            var isStale = IsStale(documentsContext, indexContext, cutoffEtag?.DocEtag, cutoffEtag?.ReferenceEtag);

                            if (WillResultBeAcceptable(isStale, query, wait) == false)
                            {
                                documentsContext.CloseTransaction();
                                Debug.Assert(query.WaitForNonStaleResultsTimeout != null);

                                if (wait == null)
                                    wait = new AsyncWaitForIndexing(queryDuration,
                                        query.WaitForNonStaleResultsTimeout.Value, this);

                                marker.ReleaseLock();

                                await wait.WaitForIndexingAsync(frozenAwaiter).ConfigureAwait(false);
                                continue;
                            }

                            FillFacetedQueryResult(result, isStale,
                                facetQuery.FacetsEtag, facetQuery.Query.Metadata,
                                documentsContext, indexContext);

                            documentsContext.CloseTransaction();

                            using (var reader = IndexPersistence.OpenFacetedIndexReader(indexTx.InnerTransaction))
                            {
                                result.Results = reader.FacetedQuery(facetQuery, documentsContext, GetOrAddSpatialField, token.Token);
                                result.TotalResults = result.Results.Count;
                                return result;
                            }
                        }
                    }

                }
            }
        }

        public virtual TermsQueryResultServerSide GetTerms(string field, string fromValue, int pageSize,
            DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            AssertIndexState();

            using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
            using (var tx = indexContext.OpenReadTransaction())
            {
                var result = new TermsQueryResultServerSide
                {
                    IndexName = Name,
                    ResultEtag =
                        CalculateIndexEtag(documentsContext, indexContext, null, IsStale(documentsContext, indexContext))
                };

                using (var reader = IndexPersistence.OpenIndexReader(tx.InnerTransaction))
                {
                    result.Terms = reader.Terms(field, fromValue, pageSize, token.Token);
                }

                return result;
            }
        }

        public virtual async Task<SuggestionQueryResult> SuggestionQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            AssertIndexState();

            if (State == IndexState.Idle)
                SetState(IndexState.Normal);

            MarkQueried(DocumentDatabase.Time.GetUtcNow());
            AssertQueryDoesNotContainFieldsThatAreNotIndexed(query.Metadata);

            using (var marker = MarkQueryAsRunning(query, token))
            {
                var result = new SuggestionQueryResult();

                var queryDuration = Stopwatch.StartNew();
                AsyncWaitForIndexing wait = null;
                (long? DocEtag, long? ReferenceEtag)? cutoffEtag = null;

                while (true)
                {
                    AssertIndexState();
                    marker.HoldLock();

                    using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
                    {
                        // we take the awaiter _before_ the indexing transaction happens, 
                        // so if there are any changes, it will already happen to it, and we'll 
                        // query the index again. This is important because of: 
                        // http://issues.hibernatingrhinos.com/issue/RavenDB-5576
                        var frozenAwaiter = GetIndexingBatchAwaiter();
                        using (var indexTx = indexContext.OpenReadTransaction())
                        {
                            documentsContext.OpenReadTransaction();
                            // we have to open read tx for mapResults _after_ we open index tx

                            if (query.WaitForNonStaleResults && cutoffEtag == null)
                                cutoffEtag = GetCutoffEtag(documentsContext);

                            var isStale = IsStale(documentsContext, indexContext, cutoffEtag?.DocEtag, cutoffEtag?.ReferenceEtag);

                            if (WillResultBeAcceptable(isStale, query, wait) == false)
                            {
                                documentsContext.CloseTransaction();
                                Debug.Assert(query.WaitForNonStaleResultsTimeout != null);

                                if (wait == null)
                                    wait = new AsyncWaitForIndexing(queryDuration,
                                        query.WaitForNonStaleResultsTimeout.Value, this);

                                marker.ReleaseLock();

                                await wait.WaitForIndexingAsync(frozenAwaiter).ConfigureAwait(false);
                                continue;
                            }

                            FillSuggestionQueryResult(result, isStale, query.Metadata, documentsContext, indexContext);

                            documentsContext.CloseTransaction();

                            var suggestField = (SuggestionField)query.Metadata.SelectFields[0];
                            using (var reader = IndexPersistence.OpenSuggestionIndexReader(indexTx.InnerTransaction, suggestField.Name))
                            {
                                result.Results.Add(reader.Suggestions(query, suggestField, documentsContext, token.Token));
                                result.TotalResults = result.Results.Count;
                                return result;
                            }
                        }
                    }
                }
            }
        }
        public IndexEntriesQueryResult IndexEntries(IndexQueryServerSide query, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            AssertIndexState();

            if (State == IndexState.Idle)
                SetState(IndexState.Normal);

            MarkQueried(DocumentDatabase.Time.GetUtcNow());
            AssertQueryDoesNotContainFieldsThatAreNotIndexed(query.Metadata);

            using (var marker = MarkQueryAsRunning(query, token))
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
            using (var indexTx = indexContext.OpenReadTransaction())
            using (var reader = IndexPersistence.OpenIndexReader(indexTx.InnerTransaction))
            {
                AssertIndexState();
                marker.HoldLock();

                var result = new IndexEntriesQueryResult();

                using (documentsContext.OpenReadTransaction())
                {
                    var isStale = IsStale(documentsContext, indexContext);
                    FillQueryResult(result, isStale, query.Metadata, documentsContext, indexContext);
                }

                var totalResults = new Reference<int>();
                foreach (var indexEntry in reader.IndexEntries(documentsContext, query, totalResults, documentsContext, GetOrAddSpatialField, token.Token))
                {
                    result.AddResult(indexEntry);
                }

                result.TotalResults = totalResults.Value;

                return result;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AssertIndexState(bool assertState = true)
        {
            DocumentDatabase.DatabaseShutdown.ThrowIfCancellationRequested();

            if (assertState && _isCompactionInProgress)
                ThrowCompactionInProgress();

            if (_initialized == false)
                ThrowNotInitialized();

            if (_disposeOne.Disposed)
                ThrowWasDisposed();

            if (assertState && State == IndexState.Error)
            {
                var errorStateReason = _errorStateReason;
                if (string.IsNullOrWhiteSpace(errorStateReason) == false)
                    ThrowMarkedAsError(errorStateReason);

                ThrowErrored();
            }
        }

        private (long DocumentCutoff, long? ReferenceCutoff) GetCutoffEtag(DocumentsOperationContext context)
        {
            long cutoffEtag = 0;

            foreach (var collection in Collections)
            {
                var etag = GetLastEtagInCollection(context, collection);

                if (etag > cutoffEtag)
                    cutoffEtag = etag;
            }

            long? referenceCutoffEtag = null;

            var referencedCollections = GetReferencedCollections();

            if (referencedCollections != null)
            {
                foreach (var referencedCollection in GetReferencedCollections())
                foreach (var refCollection in referencedCollection.Value)
                {
                    var etag = GetLastEtagInCollection(context, refCollection.Name);

                    if (referenceCutoffEtag == null || etag > referenceCutoffEtag)
                        referenceCutoffEtag = etag;
                }
            }

            return (cutoffEtag, referenceCutoffEtag);
        }

        private void ThrowErrored()
        {
            throw new InvalidOperationException(
                $"Index '{Name}' is marked as errored. Please check index errors available at '/databases/{DocumentDatabase.Name}/indexes/errors?name={Name}'.");
        }

        private void ThrowMarkedAsError(string errorStateReason)
        {
            throw new InvalidOperationException($"Index '{Name}' is marked as errored. {errorStateReason}");
        }

        private void ThrowWasDisposed()
        {
            throw new ObjectDisposedException($"Index '{Name}' was already disposed.");
        }

        private void ThrowNotInitialized()
        {
            throw new InvalidOperationException($"Index '{Name}' was not initialized.");
        }

        private void ThrowCompactionInProgress()
        {
            throw new InvalidOperationException($"Index '{Name}' is currently being compacted.");
        }

        private void AssertQueryDoesNotContainFieldsThatAreNotIndexed(QueryMetadata metadata)
        {
            foreach (var field in metadata.IndexFieldNames)
            {
                AssertKnownField(field);
            }

            if (metadata.OrderBy != null)
            {
                foreach (var sortedField in metadata.OrderBy)
                {
                    if (sortedField.OrderingType == OrderByFieldType.Random)
                        continue;

                    if (sortedField.OrderingType == OrderByFieldType.Score)
                        continue;

                    var f = sortedField.Name;

#if FEATURE_CUSTOM_SORTING
                    if (f.Value.StartsWith(Constants.Documents.Indexing.Fields.CustomSortFieldName))
                        continue;
#endif

                    AssertKnownField(f);
                }
            }
        }

        private void AssertKnownField(string f)
        {
            // the catch all field name means that we have dynamic fields names

            if (Definition.HasDynamicFields || IndexPersistence.ContainsField(f))
                return;

            ThrowInvalidField(f);
        }

        private static void ThrowInvalidField(string f)
        {
            throw new ArgumentException($"The field '{f}' is not indexed, cannot query/sort on fields that are not indexed");
        }

        private void FillFacetedQueryResult(FacetedQueryResult result, bool isStale, long facetSetupEtag, QueryMetadata q,
            DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {
            result.IndexName = Name;
            result.IsStale = isStale;
            result.IndexTimestamp = LastIndexingTime ?? DateTime.MinValue;
            result.LastQueryTime = _lastQueryingTime ?? DateTime.MinValue;
            result.ResultEtag = CalculateIndexEtag(documentsContext, indexContext, q, result.IsStale) ^ facetSetupEtag;
            result.NodeTag = DocumentDatabase.ServerStore.NodeTag;
        }

        private void FillSuggestionQueryResult(SuggestionQueryResult result, bool isStale, QueryMetadata q,
            DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {
            result.IndexName = Name;
            result.IsStale = isStale;
            result.IndexTimestamp = LastIndexingTime ?? DateTime.MinValue;
            result.LastQueryTime = _lastQueryingTime ?? DateTime.MinValue;
            result.ResultEtag = CalculateIndexEtag(documentsContext, indexContext, q, result.IsStale);
            result.NodeTag = DocumentDatabase.ServerStore.NodeTag;
        }

        private void FillQueryResult<TResult, TInclude>(QueryResultBase<TResult, TInclude> result, bool isStale, QueryMetadata q,
            DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {
            result.IndexName = Name;
            result.IsStale = isStale;
            result.IndexTimestamp = LastIndexingTime ?? DateTime.MinValue;
            result.LastQueryTime = _lastQueryingTime ?? DateTime.MinValue;
            result.ResultEtag = CalculateIndexEtag(documentsContext, indexContext, q, result.IsStale);
            result.NodeTag = DocumentDatabase.ServerStore.NodeTag;
        }

        private QueryDoneRunning MarkQueryAsRunning(IIndexQuery query, OperationCancelToken token)
        {
            var queryStartTime = DateTime.UtcNow;
            var queryId = Interlocked.Increment(ref _numberOfQueries);

            if (queryId == 1 && _didWork == false)
                _firstBatchTimeout = query.WaitForNonStaleResultsTimeout / 2 ?? DefaultWaitForNonStaleResultsTimeout / 2;

            var executingQueryInfo = new ExecutingQueryInfo(queryStartTime, query, queryId, token);
            CurrentlyRunningQueries.Add(executingQueryInfo);

            return new QueryDoneRunning(this, executingQueryInfo);
        }

        protected QueryDoneRunning CurrentlyInUse(out bool available)
        {
            var queryDoneRunning = new QueryDoneRunning(this, null);
            available = queryDoneRunning.TryHoldLock();
            return queryDoneRunning;
        }

        protected QueryDoneRunning CurrentlyInUse()
        {
            var queryDoneRunning = new QueryDoneRunning(this, null);
            queryDoneRunning.HoldLock();
            return queryDoneRunning;
        }

        private static readonly TimeSpan DefaultWaitForNonStaleResultsTimeout = TimeSpan.FromSeconds(15); // this matches default timeout from client

        private readonly ConcurrentLruRegexCache _regexCache = new ConcurrentLruRegexCache(1024);

        private static bool WillResultBeAcceptable(bool isStale, IndexQueryBase<BlittableJsonReaderObject> query, AsyncWaitForIndexing wait)
        {
            if (isStale == false)
                return true;

            if (query.WaitForNonStaleResults && query.WaitForNonStaleResultsTimeout == null)
            {
                query.WaitForNonStaleResultsTimeout = DefaultWaitForNonStaleResultsTimeout;
                return false;
            }

            if (query.WaitForNonStaleResultsTimeout == null)
                return true;

            if (wait != null && wait.TimeoutExceeded)
                return true;

            return false;
        }

        protected virtual unsafe long CalculateIndexEtag(DocumentsOperationContext documentsContext,
            TransactionOperationContext indexContext, QueryMetadata q, bool isStale)
        {
            var length = MinimumSizeForCalculateIndexEtagLength();

            var indexEtagBytes = stackalloc byte[length];

            CalculateIndexEtagInternal(indexEtagBytes, isStale, State, documentsContext, indexContext);

            UseAllDocumentsEtag(documentsContext, q, length, indexEtagBytes);

            unchecked
            {
                return (long)Hashing.XXHash64.Calculate(indexEtagBytes, (ulong)length);
            }
        }

        protected static unsafe void UseAllDocumentsEtag(DocumentsOperationContext documentsContext, QueryMetadata q, int length, byte* indexEtagBytes)
        {
            if (q?.HasIncludeOrLoad == true)
            {
                Debug.Assert(length > sizeof(long) * 4);

                long* buffer = (long*)indexEtagBytes;
                buffer[0] = DocumentsStorage.ReadLastDocumentEtag(documentsContext.Transaction.InnerTransaction);
                buffer[1] = DocumentsStorage.ReadLastTombstoneEtag(documentsContext.Transaction.InnerTransaction);
                //buffer[2] - last processed doc etag
                //buffer[3] - last process tombstone etag
            }
        }

        protected int MinimumSizeForCalculateIndexEtagLength()
        {
            var length = sizeof(long) * 4 * Collections.Count + // last document etag, last tombstone etag and last mapped etags per collection
                         sizeof(int) + // definition hash
                         1 + // isStale
                         1; // index state
            return length;
        }

        protected unsafe void CalculateIndexEtagInternal(byte* indexEtagBytes, bool isStale, IndexState indexState,
            DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {

            foreach (var collection in Collections)
            {
                var lastDocEtag = DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(documentsContext, collection);
                var lastTombstoneEtag = DocumentDatabase.DocumentsStorage.GetLastTombstoneEtag(documentsContext, collection);
                var lastMappedEtag = _indexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection);
                var lastProcessedTombstoneEtag = _indexStorage.ReadLastProcessedTombstoneEtag(indexContext.Transaction, collection);

                *(long*)indexEtagBytes = lastDocEtag;
                indexEtagBytes += sizeof(long);
                *(long*)indexEtagBytes = lastTombstoneEtag;
                indexEtagBytes += sizeof(long);
                *(long*)indexEtagBytes = lastMappedEtag;
                indexEtagBytes += sizeof(long);
                *(long*)indexEtagBytes = lastProcessedTombstoneEtag;
                indexEtagBytes += sizeof(long);
            }

            *(int*)indexEtagBytes = Definition.GetHashCode();
            indexEtagBytes += sizeof(int);
            *indexEtagBytes = isStale ? (byte)0 : (byte)1;
            indexEtagBytes += sizeof(byte);
            *indexEtagBytes = (byte)indexState;
        }

        public long GetIndexEtag(QueryMetadata q)
        {
            using (CurrentlyInUse(out var valid))
            {
                if (valid == false)
                    return DateTime.UtcNow.Ticks; // must be always different

                using (DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
                using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
                {
                    using (indexContext.OpenReadTransaction())
                    using (documentsContext.OpenReadTransaction())
                    {
                        return CalculateIndexEtag(documentsContext, indexContext, q, IsStale(documentsContext, indexContext));
                    }
                }
            }
        }

        public virtual Dictionary<string, long> GetLastProcessedTombstonesPerCollection()
        {
            using (CurrentlyInUse())
            {
                using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    using (var tx = context.OpenReadTransaction())
                    {
                        return GetLastProcessedDocumentTombstonesPerCollection(tx);
                    }
                }
            }
        }

        protected Dictionary<string, long> GetLastProcessedDocumentTombstonesPerCollection(RavenTransaction tx)
        {
            var etags = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
            foreach (var collection in Collections)
            {
                etags[collection] = _indexStorage.ReadLastProcessedTombstoneEtag(tx, collection);
            }

            return etags;
        }


        private void AddIndexingPerformance(IndexingStatsAggregator stats)
        {
            _lastIndexingStats.Enqueue(stats);

            while (_lastIndexingStats.Count > 25)
                _lastIndexingStats.TryDequeue(out stats);
        }

        public IndexingPerformanceStats[] GetIndexingPerformance()
        {
            var lastStats = _lastStats;

            return _lastIndexingStats
                .Select(x => x == lastStats ? x.ToIndexingPerformanceLiveStatsWithDetails() : x.ToIndexingPerformanceStats())
                .ToArray();
        }

        public IndexingStatsAggregator GetLatestIndexingStat()
        {
            return _lastStats;
        }

        public abstract IQueryResultRetriever GetQueryResultRetriever(IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsOperationContext documentsContext, FieldsToFetch fieldsToFetch, IncludeDocumentsCommand includeDocumentsCommand);

        protected void HandleIndexOutputsPerDocument(LazyStringValue documentId, int numberOfOutputs, IndexingStatsScope stats)
        {
            stats.RecordNumberOfProducedOutputs(numberOfOutputs);

            if (numberOfOutputs > MaxNumberOfOutputsPerDocument)
                MaxNumberOfOutputsPerDocument = numberOfOutputs;

            if (PerformanceHints.MaxWarnIndexOutputsPerDocument <= 0 || numberOfOutputs <= PerformanceHints.MaxWarnIndexOutputsPerDocument)
                return;

            _indexOutputsPerDocumentWarning.NumberOfExceedingDocuments++;

            if (_indexOutputsPerDocumentWarning.MaxNumberOutputsPerDocument < numberOfOutputs)
            {
                _indexOutputsPerDocumentWarning.MaxNumberOutputsPerDocument = numberOfOutputs;
                _indexOutputsPerDocumentWarning.SampleDocumentId = documentId;
            }

            if (_indexOutputsPerDocumentWarning.LastWarnedAt != null &&
                (SystemTime.UtcNow - _indexOutputsPerDocumentWarning.LastWarnedAt.Value).Minutes <= 5)
            {
                // save the hint every 5 minutes (at worst case)
                return;
            }

            _indexOutputsPerDocumentWarning.LastWarnedAt = SystemTime.UtcNow;

            var hint = PerformanceHint.Create(
                DocumentDatabase.Name,
                "High indexing fanout ratio",
                $"Index '{Name}' has produced more than {PerformanceHints.MaxWarnIndexOutputsPerDocument:#,#} map results from a single document",
                PerformanceHintType.Indexing,
                NotificationSeverity.Warning,
                source: Name,
                details: _indexOutputsPerDocumentWarning);

            DocumentDatabase.NotificationCenter.Add(hint);
        }

        public virtual Dictionary<string, HashSet<CollectionName>> GetReferencedCollections()
        {
            return null;
        }

        private int? _minBatchSize;

        private const int MinMapBatchSize = 128;
        internal const int MinMapReduceBatchSize = 64;

        private int MinBatchSize
        {
            get
            {
                if (_minBatchSize != null)
                    return _minBatchSize.Value;

                switch (Type)
                {
                    case IndexType.Map:
                    case IndexType.AutoMap:
                    case IndexType.JavaScriptMap:
                        _minBatchSize = MinMapBatchSize;
                        break;
                    case IndexType.MapReduce:
                    case IndexType.AutoMapReduce:
                    case IndexType.JavaScriptMapReduce:
                        _minBatchSize = MinMapReduceBatchSize;
                        break;
                    default:
                        throw new ArgumentException($"Unknown index type {Type}");
                }

                return _minBatchSize.Value;
            }
        }

        private Size? TransactionSizeLimit
        {
            get
            {
                if (_transactionSizeLimit != null)
                    return _transactionSizeLimit.Value;

                if (Configuration.TransactionSizeLimit == null)
                {
                    _transactionSizeLimit = new Lazy<Size?>(() => null);
                }
                else
                {
                    var limit = Configuration.TransactionSizeLimit;

                    if (Type.IsMapReduce())
                        limit = limit * 0.75; // let's decrease the limit to take into account additional work in reduce phase

                    _transactionSizeLimit = new Lazy<Size?>(() => limit);
                }

                return _transactionSizeLimit.Value;
            }
        }

        public bool CanContinueBatch(
            IndexingStatsScope stats,
            DocumentsOperationContext documentsOperationContext,
            TransactionOperationContext indexingContext,
            IndexWriteOperation indexWriteOperation,
            int count)
        {
            var txAllocationsInBytes = UpdateThreadAllocations(indexingContext, indexWriteOperation, stats, updateReduceStats: false);

            if (_indexDisabled)
            {
                stats.RecordMapCompletedReason("Index was disabled");
                return false;
            }

            if (_lowMemoryFlag.IsRaised() && count > MinBatchSize)
            {
                HandleStoppedBatchesConcurrently(stats, count,
                    canContinue: () => _lowMemoryFlag.IsRaised() == false, 
                    reason: "low memory");

                stats.RecordMapCompletedReason($"The batch was stopped after processing {count:#,#;;0} documents because of low memory");
                return false;
            }

            if (_firstBatchTimeout.HasValue && stats.Duration > _firstBatchTimeout)
            {
                stats.RecordMapCompletedReason(
                    $"Stopping the first batch after {_firstBatchTimeout} to ensure just created index has some results");

                _firstBatchTimeout = null;

                return false;
            }

            if (stats.ErrorsCount >= IndexStorage.MaxNumberOfKeptErrors)
            {
                stats.RecordMapCompletedReason(
                    $"Number of errors ({stats.ErrorsCount}) reached maximum number of allowed errors per batch ({IndexStorage.MaxNumberOfKeptErrors})");
                return false;
            }

            if (sizeof(int) == IntPtr.Size || DocumentDatabase.Configuration.Storage.ForceUsing32BitsPager)
            {
                IPagerLevelTransactionState pagerLevelTransactionState = documentsOperationContext.Transaction?.InnerTransaction?.LowLevelTransaction;
                var total32BitsMappedSize = pagerLevelTransactionState?.GetTotal32BitsMappedSize();
                if (total32BitsMappedSize > 8 * Voron.Global.Constants.Size.Megabyte)
                {
                    stats.RecordMapCompletedReason($"Running in 32 bits and have {total32BitsMappedSize / 1024:#,#0} kb mapped in docs ctx");
                    return false;
                }

                pagerLevelTransactionState = indexingContext.Transaction?.InnerTransaction?.LowLevelTransaction;
                total32BitsMappedSize = pagerLevelTransactionState?.GetTotal32BitsMappedSize();
                if (total32BitsMappedSize > 8 * Voron.Global.Constants.Size.Megabyte)
                {
                    stats.RecordMapCompletedReason($"Running in 32 bits and have {total32BitsMappedSize / 1024:#,#0} kb mapped in index ctx");
                    return false;
                }
            }

            if (TransactionSizeLimit != null)
            {
                var txAllocations = new Size(txAllocationsInBytes, SizeUnit.Bytes);
                if (txAllocations > TransactionSizeLimit.Value)
                {
                    stats.RecordMapCompletedReason($"Reached transaction size limit ({TransactionSizeLimit.Value}). Allocated {new Size(txAllocationsInBytes, SizeUnit.Bytes)} in current transaction");
                    return false;
                }
            }

            if (Configuration.ScratchSpaceLimit != null &&
                _environment.Options.ScratchSpaceUsage.ScratchSpaceInBytes > Configuration.ScratchSpaceLimit.Value.GetValue(SizeUnit.Bytes) && count > MinBatchSize)
            {
                _scratchSpaceLimitExceeded = true;

                stats.RecordMapCompletedReason(
                    $"Reached scratch space limit ({Configuration.ScratchSpaceLimit.Value}). Current scratch space is {new Size(_environment.Options.ScratchSpaceUsage.ScratchSpaceInBytes, SizeUnit.Bytes)}");

                return false;
            }

            var globalIndexingScratchSpaceUsage = DocumentDatabase.ServerStore.GlobalIndexingScratchSpaceMonitor;

            if (globalIndexingScratchSpaceUsage?.IsLimitExceeded == true && count > MinBatchSize)
            {
                _scratchSpaceLimitExceeded = true;

                stats.RecordMapCompletedReason(
                    $"Reached global scratch space limit for indexing ({globalIndexingScratchSpaceUsage.LimitAsSize}). Current scratch space is {globalIndexingScratchSpaceUsage.ScratchSpaceAsSize}");
                
                return false;
            }

            var allocated = new Size(_threadAllocations.CurrentlyAllocatedForProcessing, SizeUnit.Bytes);
            if (allocated > _currentMaximumAllowedMemory)
            {
                var canContinue = true;

                if (MemoryUsageGuard.TryIncreasingMemoryUsageForThread(
                    _threadAllocations,
                    ref _currentMaximumAllowedMemory,
                    allocated,
                    _environment.Options.RunningOn32Bits,
                    _logger,
                    out var memoryUsage) == false)
                {
                    _allocationCleanupNeeded = true;

                    documentsOperationContext.DoNotReuse = true;
                    indexingContext.DoNotReuse = true;

                    if (stats.MapAttempts >= Configuration.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)
                    {
                        if (_logger.IsInfoEnabled)
                        {
                            var message = $"Stopping the batch because cannot budget additional memory. " +
                                          $"Current budget: {allocated}.";

                            if (memoryUsage != null)
                            {
                                message += " Current memory usage: " +
                                           $"{nameof(memoryUsage.WorkingSet)} = {memoryUsage.WorkingSet}," +
                                           $"{nameof(memoryUsage.PrivateMemory)} = {memoryUsage.PrivateMemory}";
                            }

                            _logger.Info(message);
                        }

                        HandleStoppedBatchesConcurrently(stats, count, 
                            canContinue: MemoryUsageGuard.CanIncreaseMemoryUsageForThread, 
                            reason: "cannot budget additional memory");

                        stats.RecordMapCompletedReason("Cannot budget additional memory for batch");
                        canContinue = false;
                    }
                }

                if (memoryUsage != null)
                    stats.RecordMapMemoryStats(memoryUsage.WorkingSet, memoryUsage.PrivateMemory, _currentMaximumAllowedMemory.GetValue(SizeUnit.Bytes));

                return canContinue;
            }

            return true;
        }

        public long UpdateThreadAllocations(
            TransactionOperationContext indexingContext,
            IndexWriteOperation indexWriteOperation,
            IndexingStatsScope stats,
            bool updateReduceStats)
        {
            var threadAllocations = _threadAllocations.TotalAllocated;
            var txAllocations = indexingContext.Transaction.InnerTransaction.LowLevelTransaction.NumberOfModifiedPages
                                * Voron.Global.Constants.Storage.PageSize;
            var indexWriterAllocations = indexWriteOperation?.GetUsedMemory() ?? 0;

            if (stats != null)
            {
                var allocatedForStats = threadAllocations + txAllocations + indexWriterAllocations;
                if (updateReduceStats)
                {
                    stats.RecordReduceAllocations(allocatedForStats);
                }
                else
                {
                    stats.RecordMapAllocations(allocatedForStats);
                }
            }

            var allocatedForProcessing = threadAllocations +
                                   // we multiple it by two to take into account additional work
                                   // that will need to be done during the commit phase of the index
                                   2 * (indexWriterAllocations + txAllocations);

            _threadAllocations.CurrentlyAllocatedForProcessing = allocatedForProcessing;

            return txAllocations;
        }

        private void HandleStoppedBatchesConcurrently(
            IndexingStatsScope stats, int count, 
            Func<bool> canContinue, string reason)
        {
            if (_batchStopped)
            {
                // already stopped by MapDocuments, HandleReferences or CleanupDeletedDocuments
                return;
            }

            _batchStopped = DocumentDatabase.IndexStore.StoppedConcurrentIndexBatches.Wait(0);
            if (_batchStopped)
                return;

            var message = $"Halting processing of batch after {count:#,#;;0} and waiting because of {reason}, " +
                          $"other indexes are currently completing and index {Name} will wait for them to complete";
            stats.RecordMapCompletedReason(message);
            if (_logger.IsInfoEnabled)
                _logger.Info(message);
            var timeout = _indexStorage.DocumentDatabase.Configuration.Indexing.MaxTimeForDocumentTransactionToRemainOpen.AsTimeSpan;

            while (true)
            {
                _batchStopped = DocumentDatabase.IndexStore.StoppedConcurrentIndexBatches.Wait(
                    timeout,
                    _indexingProcessCancellationTokenSource.Token);
                if (_batchStopped)
                    break;

                if (canContinue())
                    break;

                if (_logger.IsInfoEnabled)
                    _logger.Info($"{Name} is still waiting for other indexes to complete their batches because there is a low memory condition in action...");
            }
        }

        public void Compact(Action<IOperationProgress> onProgress, CompactionResult result, CancellationToken token)
        {
            if (_isCompactionInProgress)
                throw new InvalidOperationException($"Index '{Name}' cannot be compacted because compaction is already in progress.");

            result.SizeBeforeCompactionInMb = CalculateIndexStorageSize().GetValue(SizeUnit.Megabytes);

            if (Type.IsMapReduce())
            {
                result.AddMessage($"Skipping compaction of '{Name}' index because compaction of map-reduce indexes isn't supported");
                onProgress?.Invoke(result.Progress);
                result.TreeName = null;
                result.SizeAfterCompactionInMb = result.SizeBeforeCompactionInMb;

                return;
            }

            result.AddMessage($"Starting compaction of index '{Name}'.");
            result.AddMessage($"Draining queries for {Name}.");
            onProgress?.Invoke(result.Progress);

            using (DrainRunningQueries())
            {
                if (_environment.Options.IncrementalBackupEnabled)
                    throw new InvalidOperationException(
                        $"Index '{Name}' cannot be compacted because incremental backup is enabled.");

                if (Configuration.RunInMemory)
                    throw new InvalidOperationException(
                        $"Index '{Name}' cannot be compacted because it runs in memory.");

                _isCompactionInProgress = true;
                PathSetting compactPath = null;

                try
                {
                    var storageEnvironmentOptions = _environment.Options;

                    using (RestartEnvironment())
                    {
                        var environmentOptions =
                                                (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)storageEnvironmentOptions;
                        var srcOptions = StorageEnvironmentOptions.ForPath(environmentOptions.BasePath.FullPath, null, null, DocumentDatabase.IoChanges,
                            DocumentDatabase.CatastrophicFailureNotification);

                        InitializeOptions(srcOptions, DocumentDatabase, Name, schemaUpgrader: false);

                        compactPath = Configuration.StoragePath.Combine(IndexDefinitionBase.GetIndexNameSafeForFileSystem(Name) + "_Compact");

                        using (var compactOptions = (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)
                            StorageEnvironmentOptions.ForPath(compactPath.FullPath, null, null, DocumentDatabase.IoChanges,
                                DocumentDatabase.CatastrophicFailureNotification))
                        {
                            InitializeOptions(compactOptions, DocumentDatabase, Name, schemaUpgrader: false);

                            StorageCompaction.Execute(srcOptions, compactOptions, progressReport =>
                            {
                                result.Progress.TreeProgress = progressReport.TreeProgress;
                                result.Progress.TreeTotal = progressReport.TreeTotal;
                                result.Progress.TreeName = progressReport.TreeName;
                                result.Progress.GlobalProgress = progressReport.GlobalProgress;
                                result.Progress.GlobalTotal = progressReport.GlobalTotal;
                                result.AddMessage(progressReport.Message);
                                onProgress?.Invoke(result.Progress);
                            }, token);
                        }

                        // reset tree name back to null after processing
                        result.TreeName = null;

                        IOExtensions.DeleteDirectory(environmentOptions.BasePath.FullPath);
                        IOExtensions.MoveDirectory(compactPath.FullPath, environmentOptions.BasePath.FullPath);
                    }

                    result.SizeAfterCompactionInMb = CalculateIndexStorageSize().GetValue(SizeUnit.Megabytes);
                }
                catch (Exception e)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations("Unable to complete compaction, index is not usable and may require reset of the index to recover", e);

                    throw;
                }
                finally
                {
                    if (compactPath != null)
                        IOExtensions.DeleteDirectory(compactPath.FullPath);

                    _isCompactionInProgress = false;
                }
            }
        }

        public IDisposable RestartEnvironment()
        {
            // shutdown environment
            if (_currentlyRunningQueriesLock.IsWriteLockHeld == false)
                throw new InvalidOperationException("Expected to be called only via DrainRunningQueries");

            // here we ensure that we aren't currently running any indexing,
            // because we'll shut down the environment for this index, reads
            // are handled using the DrainRunningQueries portion
            GetWaitForIndexingThreadToExit(disableIndex: false)?.Join(Timeout.Infinite);
            _environment.Dispose();

            return new DisposableAction(() =>
            {
                // restart environment
                if (_currentlyRunningQueriesLock.IsWriteLockHeld == false)
                    throw new InvalidOperationException("Expected to be called only via DrainRunningQueries");

                var options = CreateStorageEnvironmentOptions(DocumentDatabase, Configuration);

                DirectoryExecUtils.SubscribeToOnDirectoryInitializeExec(options, DocumentDatabase.Configuration.Storage, DocumentDatabase.Name, DirectoryExecUtils.EnvironmentType.Index, _logger);

                try
                {
                    _environment = LayoutUpdater.OpenEnvironment(options);
                    InitializeInternal(_environment, DocumentDatabase, Configuration, PerformanceHints);
                }
                catch
                {
                    Dispose();
                    options.Dispose();
                    throw;
                }
                StartIndexingThread();
            });
        }

        public Size CalculateIndexStorageSize()
        {
            long sizeOnDiskInBytes = 0;

            using (var tx = _environment.ReadTransaction())
            {
                var storageReport = _environment.GenerateReport(tx);
                if (storageReport == null)
                    return new Size(0, SizeUnit.Bytes);

                var journalSize = storageReport.Journals.Sum(j => j.AllocatedSpaceInBytes);
                sizeOnDiskInBytes += storageReport.DataFile.AllocatedSpaceInBytes + journalSize;
            }

            return new Size(sizeOnDiskInBytes, SizeUnit.Bytes);
        }

        public long GetLastEtagInCollection(DocumentsOperationContext databaseContext, string collection)
        {
            long lastDocEtag;
            long lastTombstoneEtag;
            if (collection == Constants.Documents.Collections.AllDocumentsCollection)
            {
                lastDocEtag = DocumentsStorage.ReadLastDocumentEtag(databaseContext.Transaction.InnerTransaction);
                lastTombstoneEtag = DocumentsStorage.ReadLastTombstoneEtag(databaseContext.Transaction.InnerTransaction);
            }
            else
            {
                lastDocEtag = DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(databaseContext, collection);
                lastTombstoneEtag = DocumentDatabase.DocumentsStorage.GetLastTombstoneEtag(databaseContext, collection);
            }
            return Math.Max(lastDocEtag, lastTombstoneEtag);
        }


        public long GetLastDocumentEtagInCollection(DocumentsOperationContext databaseContext, string collection)
        {
            return collection == Constants.Documents.Collections.AllDocumentsCollection
                ? DocumentsStorage.ReadLastDocumentEtag(databaseContext.Transaction.InnerTransaction)
                : DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(databaseContext, collection);
        }

        public long GetLastTombstoneEtagInCollection(DocumentsOperationContext databaseContext, string collection)
        {
            return collection == Constants.Documents.Collections.AllDocumentsCollection
                ? DocumentsStorage.ReadLastTombstoneEtag(databaseContext.Transaction.InnerTransaction)
                : DocumentDatabase.DocumentsStorage.GetLastTombstoneEtag(databaseContext, collection);
        }

        public virtual DetailedStorageReport GenerateStorageReport(bool calculateExactSizes)
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                return _environment.GenerateDetailedReport(tx.InnerTransaction, calculateExactSizes);
            }
        }

        public override string ToString()
        {
            return Name;
        }

        public void LowMemory()
        {
            _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;
            _allocationCleanupNeeded = true;
            _lowMemoryFlag.Raise();
        }

        public void LowMemoryOver()
        {
            var oldValue = _lowMemoryPressure;
            var newValue = Math.Max(0, oldValue - 1);
            if (Interlocked.CompareExchange(ref _lowMemoryPressure, newValue, oldValue) == 0)
            {
                _lowMemoryFlag.Lower();
            }
        }

        internal void SimulateLowMemory()
        {
            _lowMemoryPressure = long.MaxValue;
            LowMemory();
        }

        private Regex GetOrAddRegex(string arg)
        {
            return _regexCache.Get(arg);
        }

        private SpatialField GetOrAddSpatialField(string name)
        {
            return _spatialFields.GetOrAdd(name, n =>
            {
                if (Definition.MapFields.TryGetValue(name, out var field) == false)
                    return new SpatialField(name, new SpatialOptions());

                if (field is AutoIndexField autoField)
                    return new SpatialField(name, autoField.Spatial ?? new SpatialOptions());

                if (field is IndexField staticField)
                    return new SpatialField(name, staticField.Spatial ?? new SpatialOptions());

                return new SpatialField(name, new SpatialOptions());
            });
        }

        private static bool TryFindIndexDefinition(string directoryName, DatabaseRecord record, out IndexDefinition staticDef, out AutoIndexDefinition autoDef)
        {
            foreach (var index in record.Indexes)
            {
                if (directoryName == IndexDefinitionBase.GetIndexNameSafeForFileSystem(index.Key))
                {
                    staticDef = index.Value;
                    autoDef = null;
                    return true;
                }
            }

            foreach (var index in record.AutoIndexes)
            {
                if (directoryName == IndexDefinitionBase.GetIndexNameSafeForFileSystem(index.Key))
                {
                    autoDef = index.Value;
                    staticDef = null;
                    return true;
                }
            }

            staticDef = null;
            autoDef = null;

            return false;
        }

        protected struct QueryDoneRunning : IDisposable
        {
            private static readonly TimeSpan DefaultLockTimeout = TimeSpan.FromSeconds(3);

            private static readonly TimeSpan ExtendedLockTimeout = TimeSpan.FromSeconds(30);

            readonly Index _parent;
            private readonly ExecutingQueryInfo _queryInfo;
            private bool _hasLock;

            public QueryDoneRunning(Index parent, ExecutingQueryInfo queryInfo)
            {
                _parent = parent;
                _queryInfo = queryInfo;
                _hasLock = false;
            }

            public void HoldLock()
            {
                var timeout = _parent._isReplacing
                    ? ExtendedLockTimeout
                    : DefaultLockTimeout;

                if (_parent._currentlyRunningQueriesLock.TryEnterReadLock(timeout) == false)
                    ThrowLockTimeoutException();

                _hasLock = true;
            }

            public bool TryHoldLock()
            {
                if (_parent._currentlyRunningQueriesLock.TryEnterReadLock(0) == false)
                    return false;

                _hasLock = true;

                return true;
            }

            private void ThrowLockTimeoutException()
            {
                throw new TimeoutException(
                    $"Could not get the index read lock in a reasonable time, {_parent.Name} is probably undergoing maintenance now, try again later");
            }

            public void ReleaseLock()
            {
                _hasLock = false;
                _parent._currentlyRunningQueriesLock.ExitReadLock();
            }

            public void Dispose()
            {
                if (_hasLock)
                    _parent._currentlyRunningQueriesLock.ExitReadLock();
                if (_queryInfo != null)
                    _parent.CurrentlyRunningQueries.TryRemove(_queryInfo);
            }
        }

        internal struct ExitWriteLock : IDisposable
        {
            readonly ReaderWriterLockSlim _rwls;

            public ExitWriteLock(ReaderWriterLockSlim rwls)
            {
                _rwls = rwls;
            }

            public void Dispose()
            {
                _rwls?.ExitWriteLock();
            }
        }

        public void AssertNotDisposed()
        {
            if (_disposeOne.Disposed)
                ThrowObjectDisposed();
        }
    }
}
