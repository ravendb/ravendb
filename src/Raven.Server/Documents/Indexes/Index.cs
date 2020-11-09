using System;
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
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Exceptions;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Memory;
using Raven.Server.Storage.Layout;
using Raven.Server.Storage.Schema;
using Raven.Server.Utils;
using Raven.Server.Utils.Enumerators;
using Raven.Server.Utils.Metrics;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Server;
using Sparrow.Server.Exceptions;
using Sparrow.Threading;
using Sparrow.Utils;
using Voron;
using Voron.Debugging;
using Voron.Exceptions;
using Voron.Impl;
using Voron.Impl.Compaction;
using Constants = Raven.Client.Constants;
using FacetQuery = Raven.Server.Documents.Queries.Facets.FacetQuery;
using Size = Sparrow.Size;

namespace Raven.Server.Documents.Indexes
{
    public abstract class Index<TIndexDefinition, TField> : Index
        where TIndexDefinition : IndexDefinitionBase<TField> where TField : IndexFieldBase
    {
        public new TIndexDefinition Definition => (TIndexDefinition)base.Definition;

        protected Index(IndexType type, IndexSourceType sourceType, TIndexDefinition definition)
            : base(type, sourceType, definition)
        {
        }
    }

    public abstract class Index : ITombstoneAware, IDisposable, ILowMemoryHandler
    {
        private int _writeErrors;

        private int _unexpectedErrors;

        private int _analyzerErrors;

        private int _diskFullErrors;

        private const int WriteErrorsLimit = 10;

        private const int UnexpectedErrorsLimit = 3;

        private const int AnalyzerErrorLimit = 0;

        private const int DiskFullErrorLimit = 10;

        internal const int LowMemoryPressure = 10;

        private const int AllocationCleanupRequestsLimit = 10;

        private readonly Size MappedSizeLimitOn32Bits = new Size(8, SizeUnit.Megabytes);

        protected Logger _logger;

        internal LuceneIndexPersistence IndexPersistence;

        internal IndexFieldsPersistence IndexFieldsPersistence;

        private readonly AsyncManualResetEvent _indexingBatchCompleted = new AsyncManualResetEvent();

        private readonly SemaphoreSlim _indexingInProgress = new SemaphoreSlim(1, 1);

        private long _allocatedAfterPreviousCleanup = 0;

        /// <summary>
        /// Cancelled if the database is in shutdown process.
        /// </summary>
        private CancellationTokenSource _indexingProcessCancellationTokenSource;

        private bool _indexDisabled;

        private readonly ConcurrentDictionary<string, IndexProgress.CollectionStats> _inMemoryIndexProgress =
            new ConcurrentDictionary<string, IndexProgress.CollectionStats>();

        private readonly ConcurrentDictionary<string, IndexProgress.CollectionStats> _inMemoryReferencesIndexProgress =
            new ConcurrentDictionary<string, IndexProgress.CollectionStats>();

        internal DocumentDatabase DocumentDatabase;

        internal PoolOfThreads.LongRunningWork _indexingThread;

        private bool _initialized;

        protected UnmanagedBuffersPoolWithLowMemoryHandling _unmanagedBuffersPool;

        internal StorageEnvironment _environment;

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

        private IndexingStatsAggregator _lastStats;

        private readonly ConcurrentQueue<IndexingStatsAggregator> _lastIndexingStats =
            new ConcurrentQueue<IndexingStatsAggregator>();

        private bool _didWork;
        private bool _isReplacing;

        protected readonly bool HandleAllDocs;

        protected internal MeterMetric MapsPerSec;
        protected internal MeterMetric ReducesPerSec;

        protected internal IndexingConfiguration Configuration;

        protected PerformanceHintsConfiguration PerformanceHints;

        private int _allocationCleanupNeeded;

        private readonly MultipleUseFlag _lowMemoryFlag = new MultipleUseFlag();
        private long _lowMemoryPressure;
        private bool _batchStopped;

        private Size _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;
        internal NativeMemory.ThreadStats _threadAllocations;
        private string _errorStateReason;
        private bool _isCompactionInProgress;
        public bool _firstQuery = true;
        internal TimeSpan? _firstBatchTimeout;
        private Lazy<Size?> _transactionSizeLimit;
        private bool _scratchSpaceLimitExceeded;

        private readonly ReaderWriterLockSlim _currentlyRunningQueriesLock = new ReaderWriterLockSlim();
        private readonly MultipleUseFlag _priorityChanged = new MultipleUseFlag();
        private readonly MultipleUseFlag _hadRealIndexingWorkToDo = new MultipleUseFlag();
        private readonly MultipleUseFlag _definitionChanged = new MultipleUseFlag();
        private Size _initialManagedAllocations;

        private readonly ConcurrentDictionary<string, SpatialField> _spatialFields = new ConcurrentDictionary<string, SpatialField>(StringComparer.OrdinalIgnoreCase);

        internal readonly QueryBuilderFactories _queryBuilderFactories;

        private string IndexingThreadName => "Indexing of " + Name + " of " + _indexStorage.DocumentDatabase.Name;

        private readonly WarnIndexOutputsPerDocument _indexOutputsPerDocumentWarning = new WarnIndexOutputsPerDocument
        {
            MaxNumberOutputsPerDocument = int.MinValue,
            Suggestion = "Please verify this index definition and consider a re-design of your entities or index for better indexing performance"
        };

        private static readonly Size DefaultMaximumMemoryAllocation = new Size(32, SizeUnit.Megabytes);

        public long? LastTransactionId => _environment?.CurrentReadTransactionId;

        internal bool IsLowMemory => _lowMemoryFlag.IsRaised();

        private readonly double _txAllocationsRatio;

        private readonly string _itemType;

        protected Index(IndexType type, IndexSourceType sourceType, IndexDefinitionBase definition)
        {
            Type = type;
            SourceType = sourceType;
            Definition = definition;
            Collections = new HashSet<string>(Definition.Collections, StringComparer.OrdinalIgnoreCase);

            if (Collections.Contains(Constants.Documents.Collections.AllDocumentsCollection))
                HandleAllDocs = true;

            _queryBuilderFactories = new QueryBuilderFactories
            {
                GetSpatialFieldFactory = GetOrAddSpatialField,
                GetRegexFactory = GetOrAddRegex
            };

            if (type.IsMapReduce())
            {
                _txAllocationsRatio = (definition is MapReduceIndexDefinition mpd && mpd.OutputReduceToCollection != null) ? 4 : 3;
            }
            else
            {
                _txAllocationsRatio = 2;
            }

            switch (sourceType)
            {
                case IndexSourceType.None:
                    _itemType = "item";
                    break;
                case IndexSourceType.Documents:
                    _itemType = "document";
                    break;
                case IndexSourceType.TimeSeries:
                    _itemType = "time series item";
                    break;
                case IndexSourceType.Counters:
                    _itemType = "counter";
                    break;
                default:
                    throw new ArgumentException($"Unknown index source type: {sourceType}");
            }

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

                environment = StorageLoader.OpenEnvironment(options, StorageEnvironmentWithType.StorageEnvironmentType.Index);

                IndexType type;
                IndexSourceType sourceType;
                try
                {
                    type = IndexStorage.ReadIndexType(name, environment);
                    sourceType = IndexStorage.ReadIndexSourceType(name, environment);
                }
                catch (Exception e)
                {
                    bool tryFindIndexDefinition;
                    AutoIndexDefinition autoDef;
                    IndexDefinition staticDef;

                    using (documentDatabase.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    using (var rawRecord = documentDatabase.ServerStore.Cluster.ReadRawDatabaseRecord(ctx, documentDatabase.Name))
                    {
                        tryFindIndexDefinition = TryFindIndexDefinition(name, rawRecord, out staticDef, out autoDef);
                    }

                    if (environment.NextWriteTransactionId == 2 && tryFindIndexDefinition)
                    {
                        // initial transaction creating the schema hasn't completed
                        // let's try to create it again

                        environment.Dispose();

                        if (staticDef != null)
                        {
                            switch (staticDef.SourceType)
                            {
                                case IndexSourceType.Documents:
                                    switch (staticDef.Type)
                                    {
                                        case IndexType.Map:
                                        case IndexType.JavaScriptMap:
                                            return MapIndex.CreateNew(staticDef, documentDatabase);
                                        case IndexType.MapReduce:
                                        case IndexType.JavaScriptMapReduce:
                                            return MapReduceIndex.CreateNew<MapReduceIndex>(staticDef, documentDatabase);
                                    }
                                    break;
                                case IndexSourceType.Counters:
                                    switch (staticDef.Type)
                                    {
                                        case IndexType.Map:
                                        case IndexType.JavaScriptMap:
                                            return MapCountersIndex.CreateNew(staticDef, documentDatabase);
                                        case IndexType.MapReduce:
                                        case IndexType.JavaScriptMapReduce:
                                            return MapReduceIndex.CreateNew<MapReduceCountersIndex>(staticDef, documentDatabase);
                                    }
                                    break;
                                case IndexSourceType.TimeSeries:
                                    switch (staticDef.Type)
                                    {
                                        case IndexType.Map:
                                        case IndexType.JavaScriptMap:
                                            return MapTimeSeriesIndex.CreateNew(staticDef, documentDatabase);
                                        case IndexType.MapReduce:
                                        case IndexType.JavaScriptMapReduce:
                                            return MapReduceIndex.CreateNew<MapReduceTimeSeriesIndex>(staticDef, documentDatabase);
                                    }
                                    break;
                                default:
                                    throw new ArgumentException($"Unknown index source type {staticDef.SourceType} for index {name}");
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

                switch (sourceType)
                {
                    case IndexSourceType.Documents:
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
                                return MapReduceIndex.Open<MapReduceIndex>(environment, documentDatabase);
                            default:
                                throw new ArgumentException($"Unknown index type {type} for index {name}");
                        }
                    case IndexSourceType.Counters:
                        switch (type)
                        {
                            case IndexType.Map:
                            case IndexType.JavaScriptMap:
                                return MapCountersIndex.Open(environment, documentDatabase);
                            case IndexType.MapReduce:
                            case IndexType.JavaScriptMapReduce:
                                return MapReduceIndex.Open<MapReduceCountersIndex>(environment, documentDatabase);
                            default:
                                throw new ArgumentException($"Unknown index type {type} for index {name}");
                        }
                    case IndexSourceType.TimeSeries:
                        switch (type)
                        {
                            case IndexType.Map:
                            case IndexType.JavaScriptMap:
                                return MapTimeSeriesIndex.Open(environment, documentDatabase);
                            case IndexType.MapReduce:
                            case IndexType.JavaScriptMapReduce:
                                return MapReduceIndex.Open<MapReduceTimeSeriesIndex>(environment, documentDatabase);
                            default:
                                throw new ArgumentException($"Unknown index type {type} for index {name}");
                        }
                    default:
                        throw new ArgumentException($"Unknown index source type {sourceType} for index {name}");
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

        public IndexSourceType SourceType { get; }

        public IndexState State { get; protected set; }

        public IndexDefinitionBase Definition { get; private set; }

        public string Name => Definition?.Name;

        public int MaxNumberOfOutputsPerDocument { get; private set; }

        public bool IsInvalidIndex()
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                return _indexStorage.IsIndexInvalid(tx);
            }
        }

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

        public virtual void ResetIsSideBySideAfterReplacement()
        {
        }

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
            if (configuration.EnableMetrics)
            {
                ReducesPerSec = new MeterMetric();
                MapsPerSec = new MeterMetric();
            }

            _logger = LoggingSource.Instance.GetLogger<Index>(documentDatabase.Name);
            using (DrainRunningQueries())
            {
                if (_initialized)
                    throw new InvalidOperationException($"Index '{Name}' was already initialized.");

                var options = CreateStorageEnvironmentOptions(documentDatabase, configuration);

                DirectoryExecUtils.SubscribeToOnDirectoryInitializeExec(options, documentDatabase.Configuration.Storage, documentDatabase.Name, DirectoryExecUtils.EnvironmentType.Index, _logger);

                StorageEnvironment storageEnvironment = null;
                try
                {
                    storageEnvironment = StorageLoader.OpenEnvironment(options, StorageEnvironmentWithType.StorageEnvironmentType.Index);
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
            options.OnIntegrityErrorOfAlreadySyncedData += (s, e) => documentDatabase.HandleOnIndexIntegrityErrorOfAlreadySyncedData(name, s, e);
            options.CompressTxAboveSizeInBytes = documentDatabase.Configuration.Storage.CompressTxAboveSize.GetValue(SizeUnit.Bytes);
            options.ForceUsing32BitsPager = documentDatabase.Configuration.Storage.ForceUsing32BitsPager;
            options.EnablePrefetching = documentDatabase.Configuration.Storage.EnablePrefetching;
            options.TimeToSyncAfterFlushInSec = (int)documentDatabase.Configuration.Storage.TimeToSyncAfterFlush.AsTimeSpan.TotalSeconds;
            options.NumOfConcurrentSyncsPerPhysDrive = documentDatabase.Configuration.Storage.NumberOfConcurrentSyncsPerPhysicalDrive;
            options.Encryption.MasterKey = documentDatabase.MasterKey?.ToArray(); //clone
            options.Encryption.RegisterForJournalCompressionHandler();
            options.DoNotConsiderMemoryLockFailureAsCatastrophicError = documentDatabase.Configuration.Security.DoNotConsiderMemoryLockFailureAsCatastrophicError;
            if (documentDatabase.Configuration.Storage.MaxScratchBufferSize.HasValue)
                options.MaxScratchBufferSize = documentDatabase.Configuration.Storage.MaxScratchBufferSize.Value.GetValue(SizeUnit.Bytes);
            options.PrefetchSegmentSize = documentDatabase.Configuration.Storage.PrefetchBatchSize.GetValue(SizeUnit.Bytes);
            options.PrefetchResetThreshold = documentDatabase.Configuration.Storage.PrefetchResetThreshold.GetValue(SizeUnit.Bytes);
            options.SyncJournalsCountThreshold = documentDatabase.Configuration.Storage.SyncJournalsCountThreshold;
            options.IgnoreInvalidJournalErrors = documentDatabase.Configuration.Storage.IgnoreInvalidJournalErrors;
            options.SkipChecksumValidationOnDatabaseLoading = documentDatabase.Configuration.Storage.SkipChecksumValidationOnDatabaseLoading;
            options.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions = documentDatabase.Configuration.Storage.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions;

            if (documentDatabase.ServerStore.GlobalIndexingScratchSpaceMonitor != null)
                options.ScratchSpaceUsage.AddMonitor(documentDatabase.ServerStore.GlobalIndexingScratchSpaceMonitor);

            if (schemaUpgrader)
            {
                options.SchemaVersion = SchemaUpgrader.CurrentVersion.IndexVersion;
                options.SchemaUpgrader = SchemaUpgrader.Upgrader(SchemaUpgrader.StorageType.Index, null, null, null);
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

                _logger = LoggingSource.Instance.GetLogger<Index>(documentDatabase.Name);
                _environment = environment;
                var safeName = IndexDefinitionBase.GetIndexNameSafeForFileSystem(Name);
                _unmanagedBuffersPool = new UnmanagedBuffersPoolWithLowMemoryHandling($"Indexes//{safeName}");

                InitializeComponentsUsingEnvironment(documentDatabase, _environment);

                LoadValues();

                DocumentDatabase.TombstoneCleaner.Subscribe(this);

                DocumentDatabase.Changes.OnIndexChange += HandleIndexChange;

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

        private bool IsStaleInternal()
        {
            if (_indexingProcessCancellationTokenSource.IsCancellationRequested)
                return true;

            using (var context = QueryOperationContext.Allocate(DocumentDatabase, this))
            using (context.OpenReadTransaction())
            {
                return IsStale(context);
            }
        }

        private void InitializeComponentsUsingEnvironment(DocumentDatabase documentDatabase, StorageEnvironment environment)
        {
            _contextPool?.Dispose();
            _contextPool = new TransactionContextPool(environment, documentDatabase.Configuration.Memory.MaxContextSizeToKeep);

            _indexStorage = new IndexStorage(this, _contextPool, documentDatabase);
            _indexStorage.Initialize(environment);

            IndexPersistence?.Dispose();

            IndexPersistence = new LuceneIndexPersistence(this);
            IndexPersistence.Initialize(environment);

            IndexFieldsPersistence = new IndexFieldsPersistence(this);
            IndexFieldsPersistence.Initialize();
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
                catch (ObjectDisposedException ode)
                {
                    if (_disposeOne.Disposed == false)
                    {
                        ReportUnexpectedIndexingError(ode);
                    }
                    // else we are been disposed of and we can ignore this error.
                }
                catch (Exception e)
                {
                    ReportUnexpectedIndexingError(e);
                }
            }, null, IndexingThreadName);
        }

        private void ReportUnexpectedIndexingError(Exception e)
        {
            try
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Unexpected error in '{Name}' index. This should never happen.", e);

                DocumentDatabase.NotificationCenter.Add(AlertRaised.Create(DocumentDatabase.Name, $"Unexpected error in '{Name}' index",
                    "Unexpected error in indexing thread. See details.", AlertType.Indexing_UnexpectedIndexingThreadError, NotificationSeverity.Error,
                    key: Name,
                    details: new ExceptionDetails(e)));
            }
            catch (Exception)
            {
                // ignore if we can't create notification
            }

            State = IndexState.Error;
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

            var indexingThread = _indexingThread;

            if (indexingThread == null)
                return null;

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

        public bool IsStale(QueryOperationContext queryContext, long? cutoff = null, List<string> stalenessReasons = null)
        {
            using (CurrentlyInUse(out var valid))
            {
                Debug.Assert(queryContext.Documents.Transaction != null);

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
                    return IsStale(queryContext, indexContext, cutoff: cutoff, referenceCutoff: cutoff, stalenessReasons: stalenessReasons);
                }
            }
        }

        public enum IndexProgressStatus
        {
            Faulty = -1,
            RunningStorageOperation = -2,
        }

        public IndexingState GetIndexingState(QueryOperationContext queryContext)
        {
            queryContext.AssertOpenedTransactions();

            if (Type == IndexType.Faulty)
                return new IndexingState(isStale: true, lastProcessedEtag: (long)IndexProgressStatus.Faulty, lastProcessedCompareExchangeReferenceEtag: null, lastProcessedCompareExchangeReferenceTombstoneEtag: null);

            using (CurrentlyInUse(out var valid))
            {
                long? lastProcessedCompareExchangeReferenceEtag = null;
                long? lastProcessedCompareExchangeReferenceTombstoneEtag = null;
                if (Definition.HasCompareExchange)
                {
                    lastProcessedCompareExchangeReferenceEtag = 0;
                    lastProcessedCompareExchangeReferenceTombstoneEtag = 0;
                }

                if (valid == false)
                    return new IndexingState(isStale: true, lastProcessedEtag: (long)IndexProgressStatus.RunningStorageOperation, lastProcessedCompareExchangeReferenceEtag: lastProcessedCompareExchangeReferenceEtag, lastProcessedCompareExchangeReferenceTombstoneEtag: lastProcessedCompareExchangeReferenceTombstoneEtag);

                using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
                using (indexContext.OpenReadTransaction())
                    return GetIndexingStateInternal(queryContext, indexContext);
            }
        }

        protected virtual IndexingState GetIndexingStateInternal(QueryOperationContext queryContext, TransactionOperationContext indexContext)
        {
            long lastProcessedEtag = 0;
            foreach (var collection in Collections)
                lastProcessedEtag = Math.Max(lastProcessedEtag, _indexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection));

            var isStale = IsStale(queryContext, indexContext);
            return new IndexingState(isStale, lastProcessedEtag, lastProcessedCompareExchangeReferenceEtag: null, lastProcessedCompareExchangeReferenceTombstoneEtag: null);
        }

        protected virtual IndexItem GetItemByEtag(QueryOperationContext queryContext, long etag)
        {
            var document = DocumentDatabase.DocumentsStorage.GetByEtag(queryContext.Documents, etag);
            if (document == null)
                return default;

            return new DocumentIndexItem(document.Id, document.LowerId, document.Etag, document.LastModified, document.Data.Size, document);
        }

        protected virtual IndexItem GetTombstoneByEtag(QueryOperationContext queryContext, long etag)
        {
            var tombstone = DocumentDatabase.DocumentsStorage.GetTombstoneByEtag(queryContext.Documents, etag);
            if (tombstone == null)
                return default;

            return new DocumentIndexItem(tombstone.LowerId, tombstone.LowerId, tombstone.Etag, tombstone.LastModified, 0, tombstone);
        }

        protected virtual bool HasTombstonesWithEtagGreaterThanStartAndLowerThanOrEqualToEnd(QueryOperationContext context, string collection, long start, long end)
        {
            return DocumentDatabase.DocumentsStorage.HasTombstonesWithEtagGreaterThanStartAndLowerThanOrEqualToEnd(context.Documents,
                        collection,
                        start,
                        end);
        }

        internal virtual bool IsStale(QueryOperationContext queryContext, TransactionOperationContext indexContext, long? cutoff = null, long? referenceCutoff = null, long? compareExchangeReferenceCutoff = null, List<string> stalenessReasons = null)
        {
            if (Type == IndexType.Faulty)
                return true;

            foreach (var collection in Collections)
            {
                var lastItemEtag = GetLastItemEtagInCollection(queryContext, collection);

                var lastProcessedItemEtag = _indexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection);
                var lastProcessedTombstoneEtag = _indexStorage.ReadLastProcessedTombstoneEtag(indexContext.Transaction, collection);

                _inMemoryIndexProgress.TryGetValue(collection, out var stats);

                if (cutoff == null)
                {
                    if (lastItemEtag > lastProcessedItemEtag)
                    {
                        if (stalenessReasons == null)
                            return true;

                        var lastDoc = GetItemByEtag(queryContext, lastItemEtag);

                        var message = $"There are still some {_itemType}s to process from collection '{collection}'. " +
                                   $"The last {_itemType} etag in that collection is '{lastItemEtag:#,#;;0}' " +
                                   $"({lastDoc}), " +
                                   $"but last committed {_itemType} etag for that collection is '{lastProcessedItemEtag:#,#;;0}'";
                        if (stats != null)
                            message += $" (last processed etag is: '{stats.LastProcessedItemEtag:#,#;;0}')";

                        stalenessReasons.Add(message);
                    }

                    var lastTombstoneEtag = GetLastTombstoneEtagInCollection(queryContext, collection);

                    if (lastTombstoneEtag > lastProcessedTombstoneEtag)
                    {
                        if (stalenessReasons == null)
                            return true;

                        var lastTombstone = GetTombstoneByEtag(queryContext, lastTombstoneEtag);

                        var message = $"There are still some tombstones to process from collection '{collection}'. " +
                                   $"The last tombstone etag in that collection is '{lastTombstoneEtag:#,#;;0}' " +
                                   $"({lastTombstone}), " +
                                   $"but last committed tombstone etag for that collection is '{lastProcessedTombstoneEtag:#,#;;0}'.";
                        if (stats != null)
                            message += $" (last processed etag is: '{stats.LastProcessedTombstoneEtag:#,#;;0}')";

                        stalenessReasons.Add(message);
                    }
                }
                else
                {
                    var minDocEtag = Math.Min(cutoff.Value, lastItemEtag);
                    if (minDocEtag > lastProcessedItemEtag)
                    {
                        if (stalenessReasons == null)
                            return true;

                        var lastDoc = GetItemByEtag(queryContext, lastItemEtag);

                        var message = $"There are still some {_itemType}s to process from collection '{collection}'. " +
                                   $"The last {_itemType} etag in that collection is '{lastItemEtag:#,#;;0}' " +
                                   $"({lastDoc}) " +
                                   $"with cutoff set to '{cutoff.Value}', " +
                                   $"but last committed {_itemType} etag for that collection is '{lastProcessedItemEtag:#,#;;0}'.";
                        if (stats != null)
                            message += $" (last processed etag is: '{stats.LastProcessedItemEtag:#,#;;0}')";

                        stalenessReasons.Add(message);
                    }

                    var hasTombstones = HasTombstonesWithEtagGreaterThanStartAndLowerThanOrEqualToEnd(queryContext,
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
                    if (storageEnvironment != null)
                        storageEnvironment.OnLogsApplied += HandleLogsApplied;

                    SubscribeToChanges(DocumentDatabase);

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

                        PauseIfCpuCreditsBalanceIsTooLow();

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
                                        _indexingInProgress.Wait(_indexingProcessCancellationTokenSource.Token);

                                        TimeSpentIndexing.Start();

                                        didWork = DoIndexingWork(scope, _indexingProcessCancellationTokenSource.Token);

                                        if (_lowMemoryPressure > 0)
                                            LowMemoryOver();

                                        batchCompleted = true;

                                        
                                    }
                                    catch
                                    {
                                        // need to call it here to the let the index continue running
                                        // we'll stop when we reach the index error threshold
                                        _mre.Set();
                                        throw;
                                    }
                                    finally
                                    {
                                        _indexingInProgress.Release();

                                        if (_batchStopped)
                                        {
                                            _batchStopped = false;
                                            DocumentDatabase.IndexStore.StoppedConcurrentIndexBatches.Release();
                                        }

                                        _threadAllocations.CurrentlyAllocatedForProcessing = 0;
                                        _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;

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
                                catch (TimeoutException te)
                                {
                                    if (_logger.IsOperationsEnabled)
                                        _logger.Operations($"Failed to open write transaction, indexing will be retried", te);
                                }
                                catch (OutOfMemoryException oome)
                                {
                                    HandleOutOfMemoryException(oome, scope);
                                }
                                catch (EarlyOutOfMemoryException eoome)
                                {
                                    HandleOutOfMemoryException(eoome, scope);
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
                                catch (DiskFullException dfe)
                                {
                                    HandleDiskFullErrors(scope, storageEnvironment, dfe);
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
                                        var originalName = Name.Replace(Constants.Documents.Indexing.SideBySideIndexNamePrefix, string.Empty, StringComparison.OrdinalIgnoreCase);
                                        _isReplacing = true;

                                        if (batchCompleted)
                                        {
                                            // this side-by-side index will be replaced in a second, notify about indexing success
                                            // so we know that indexing batch is no longer in progress
                                            NotifyAboutCompletedBatch(didWork);
                                        }

                                        try
                                        {
                                            // this can fail if the indexes lock is currently held, so we'll retry
                                            // however, we might be requested to shutdown, so we want to skip replacing
                                            // in this case, worst case scenario we'll handle this in the next batch
                                            while (_indexingProcessCancellationTokenSource.IsCancellationRequested == false)
                                            {
                                                if (DocumentDatabase.IndexStore.TryReplaceIndexes(originalName, Definition.Name, _indexingProcessCancellationTokenSource.Token))
                                                {
                                                    StartIndexingThread();
                                                    return;
                                                }
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
                                    _mre.Set(); // try again

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
                            NotifyAboutCompletedBatch(didWork);

                        try
                        {
                            // the logic here is that unless we hit the memory limit on the system, we want to retain our
                            // allocated memory as long as we still have work to do (since we will reuse it on the next batch)
                            // and it is probably better to avoid alloc/free jitter.
                            // This is because faster indexes will tend to allocate the memory faster, and we want to give them
                            // all the available resources so they can complete faster.
                            var timeToWaitForMemoryCleanup = 5000;
                            var forceMemoryCleanup = false;

                            if (_lowMemoryFlag.IsRaised())
                            {
                                ReduceMemoryUsage(storageEnvironment);
                            }
                            else if (_allocationCleanupNeeded > 0)
                            {
                                timeToWaitForMemoryCleanup = 0; // if there is nothing to do, immediately cleanup everything

                                // at any rate, we'll reduce the budget for this index to what it currently has allocated to avoid
                                // the case where we freed memory at the end of the batch, but didn't adjust the budget accordingly
                                // so it will think that it can allocate more than it actually should
                                _currentMaximumAllowedMemory = Size.Min(_currentMaximumAllowedMemory,
                                    new Size(NativeMemory.CurrentThreadStats.TotalAllocated, SizeUnit.Bytes));

                                if (_allocationCleanupNeeded > AllocationCleanupRequestsLimit)
                                    forceMemoryCleanup = true;
                            }

                            if (_scratchSpaceLimitExceeded)
                            {
                                if (_logger.IsInfoEnabled)
                                    _logger.Info(
                                        $"Indexing exceeded global limit for scratch space usage. Going to flush environment of '{Name}' index and forcing sync of data file");

                                GlobalFlushingBehavior.GlobalFlusher.Value.MaybeFlushEnvironment(storageEnvironment);

                                if (_logsAppliedEvent.Wait(Configuration.MaxTimeToWaitAfterFlushAndSyncWhenExceedingScratchSpaceLimit.AsTimeSpan))
                                {
                                    // we've just flushed let's cleanup scratch space immediately
                                    storageEnvironment.CleanupMappedMemory();
                                }
                            }

                            if (forceMemoryCleanup || _mre.Wait(timeToWaitForMemoryCleanup, _indexingProcessCancellationTokenSource.Token) == false)
                            {
                                Interlocked.Exchange(ref _allocationCleanupNeeded, 0);

                                if (_environment.Options.Encryption.IsEnabled)
                                {
                                    using (var tx = _environment.WriteTransaction())
                                    {
                                        _environment.Options.Encryption.JournalCompressionBufferHandler.ZeroCompressionBuffer(tx.LowLevelTransaction);
                                    }
                                }

                                // allocation cleanup has been requested multiple times or
                                // there is no work to be done, and hasn't been for a while,
                                // so this is a good time to release resources we won't need
                                // anytime soon
                                ReduceMemoryUsage(storageEnvironment);

                                if (forceMemoryCleanup)
                                    continue;

                                WaitHandle.WaitAny(new[] { _mre.WaitHandle, _logsAppliedEvent.WaitHandle, _indexingProcessCancellationTokenSource.Token.WaitHandle });

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
                finally
                {
                    _inMemoryIndexProgress.Clear();

                    if (storageEnvironment != null)
                        storageEnvironment.OnLogsApplied -= HandleLogsApplied;

                    UnsubscribeFromChanges(DocumentDatabase);
                }
            }
        }

        private void PauseIfCpuCreditsBalanceIsTooLow()
        {
            AlertRaised alert = null;
            int numberOfTimesSlept = 0;

            while (DocumentDatabase.ServerStore.Server.CpuCreditsBalance.BackgroundTasksAlertRaised.IsRaised() && _indexDisabled == false)
            {
                _indexingProcessCancellationTokenSource.Token.ThrowIfCancellationRequested();

                // give us a bit more than a measuring cycle to gain more CPU credits
                Thread.Sleep(1250);

                if (alert == null && numberOfTimesSlept++ > 5)
                {
                    alert = AlertRaised.Create(
                       DocumentDatabase.Name,
                       Name,
                       "Indexing has been paused because the CPU credits balance is almost completely used, will be resumed when there are enough CPU credits to use.",
                       AlertType.Throttling_CpuCreditsBalance,
                       NotificationSeverity.Warning,
                       key: Name);
                    DocumentDatabase.NotificationCenter.Add(alert);
                }
            }

            if (alert != null)
            {
                DocumentDatabase.NotificationCenter.Dismiss(alert.Id);
            }
        }

        private void NotifyAboutCompletedBatch(bool didWork)
        {
            DocumentDatabase.Changes.RaiseNotifications(new IndexChange { Name = Name, Type = IndexChangeTypes.BatchCompleted });

            if (didWork)
            {
                _didWork = true;
                _firstBatchTimeout = null;
            }

            var batchCompletedAction = DocumentDatabase.IndexStore.IndexBatchCompleted;
            batchCompletedAction?.Invoke((Name, didWork));
        }

        public void Cleanup()
        {
            if (_initialized == false)
                return;

            ReduceMemoryUsage(_environment);
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

        private void ReduceMemoryUsage(StorageEnvironment environment)
        {
            if (_indexingInProgress.Wait(0) == false)
                return;

            try
            {
                var allocatedBeforeCleanup = NativeMemory.CurrentThreadStats.TotalAllocated;
                if (allocatedBeforeCleanup == _allocatedAfterPreviousCleanup)
                    return;

                DocumentDatabase.DocumentsStorage.ContextPool.Clean();
                _contextPool.Clean();
                ByteStringMemoryCache.CleanForCurrentThread();
                IndexPersistence.Clean();
                environment?.Cleanup();

                _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;

                _allocatedAfterPreviousCleanup = NativeMemory.CurrentThreadStats.TotalAllocated;
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info($"Reduced the memory usage of index '{Name}'. " +
                                 $"Before: {new Size(allocatedBeforeCleanup, SizeUnit.Bytes)}, " +
                                 $"after: {new Size(_allocatedAfterPreviousCleanup, SizeUnit.Bytes)}");
                }
            }
            finally
            {
                _indexingInProgress.Release();
            }
        }

        internal void ResetErrors()
        {
            Interlocked.Exchange(ref _writeErrors, 0);
            Interlocked.Exchange(ref _unexpectedErrors, 0);
            Interlocked.Exchange(ref _analyzerErrors, 0);
            Interlocked.Exchange(ref _diskFullErrors, 0);
        }

        internal void HandleAnalyzerErrors(IndexingStatsScope stats, IndexAnalyzerException iae)
        {
            if (_logger.IsOperationsEnabled)
                _logger.Operations($"Analyzer error occurred for '{Name}'.", iae);

            stats.AddAnalyzerError(iae);

            var analyzerErrors = Interlocked.Increment(ref _analyzerErrors);

            if (State == IndexState.Error || analyzerErrors < AnalyzerErrorLimit)
                return;

            SetErrorState($"State was changed due to excessive number of analyzer errors ({analyzerErrors}).");
        }

        internal void HandleUnexpectedErrors(IndexingStatsScope stats, Exception e)
        {
            if (_logger.IsOperationsEnabled)
                _logger.Operations($"Unexpected exception occurred for '{Name}'.", e);

            stats.AddUnexpectedError(e);

            var unexpectedErrors = Interlocked.Increment(ref _unexpectedErrors);

            if (State == IndexState.Error || unexpectedErrors < UnexpectedErrorsLimit)
                return;

            SetErrorState($"State was changed due to excessive number of unexpected errors ({unexpectedErrors}).");
        }

        internal void HandleCriticalErrors(IndexingStatsScope stats, CriticalIndexingException e)
        {
            if (_logger.IsOperationsEnabled)
                _logger.Operations($"Critical exception occurred for '{Name}'.", e);

            if (State == IndexState.Error)
                return;

            SetErrorState($"State was changed due to a critical error. Error message: {e.Message}");
        }

        internal void HandleWriteErrors(IndexingStatsScope stats, IndexWriteException iwe)
        {
            if (_logger.IsOperationsEnabled)
                _logger.Operations($"Write exception occurred for '{Name}'.", iwe);

            stats.AddWriteError(iwe);

            var writeErrors = Interlocked.Increment(ref _writeErrors);

            if (State == IndexState.Error || writeErrors < WriteErrorsLimit)
                return;

            SetErrorState($"State was changed due to excessive number of write errors ({writeErrors}).");
        }

        internal void HandleExcessiveNumberOfReduceErrors(IndexingStatsScope stats, ExcessiveNumberOfReduceErrorsException e)
        {
            if (_logger.IsOperationsEnabled)
                _logger.Operations($"Erroring index due to excessive number of reduce errors '{Name}'.", e);

            stats.AddExcessiveNumberOfReduceErrors(e);

            if (State == IndexState.Error)
                return;

            SetErrorState(e.Message);
        }

        internal void HandleDiskFullErrors(IndexingStatsScope stats, StorageEnvironment storageEnvironment, DiskFullException dfe)
        {
            stats.AddDiskFullError(dfe);

            var diskFullErrors = Interlocked.Increment(ref _diskFullErrors);
            if (diskFullErrors < DiskFullErrorLimit)
            {
                var timeToWaitInMilliseconds = (int)Math.Min(Math.Pow(2, diskFullErrors), 30) * 1000;

                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"After disk full error in index : '{Name}', " +
                                       $"going to try flushing and syncing the environment to cleanup the storage. " +
                                       $"Will wait for flush for: {timeToWaitInMilliseconds}ms", dfe);

                // force flush and sync
                var sp = Stopwatch.StartNew();
                GlobalFlushingBehavior.GlobalFlusher.Value.MaybeFlushEnvironment(storageEnvironment);
                if (_logsAppliedEvent.Wait(timeToWaitInMilliseconds, _indexingProcessCancellationTokenSource.Token))
                {
                    storageEnvironment.ForceSyncDataFile();
                }

                var timeLeft = timeToWaitInMilliseconds - sp.ElapsedMilliseconds;
                // wait for sync
                if (timeLeft > 0)
                    Task.Delay((int)timeLeft, _indexingProcessCancellationTokenSource.Token).Wait();

                storageEnvironment.Cleanup(tryCleanupRecycledJournals: true);
                return;
            }

            if (_logger.IsOperationsEnabled)
                _logger.Operations($"Disk full error occurred for '{Name}'. Setting index to errored state", dfe);

            if (State == IndexState.Error)
                return;

            storageEnvironment.Options.TryCleanupRecycledJournals();
            SetErrorState($"State was changed due to excessive number of disk full errors ({diskFullErrors}).");
        }

        private void SetErrorState(string reason)
        {
            _errorStateReason = reason;
            SetState(IndexState.Error, ignoreWriteError: true);
        }

        private void HandleOutOfMemoryException(Exception exception, IndexingStatsScope scope)
        {
            try
            {
                scope.AddMemoryError(exception);
                Interlocked.Add(ref _lowMemoryPressure, LowMemoryPressure);
                _lowMemoryFlag.Raise();

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Out of memory occurred for '{Name}'", exception);

                DocumentDatabase.NotificationCenter.OutOfMemory.Add(_environment, exception);
            }
            catch (Exception e) when (e.IsOutOfMemory())
            {
                // nothing to do here
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Failed out of memory exception handling for index '{Name}'", e);
            }
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

            try
            {
                using (_environment.Options.SkipCatastrophicFailureAssertion()) // we really want to store Error state
                {
                    SetErrorState($"State was changed due to data corruption with message '{e.Message}'");
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
            if (failureInformation.IsInvalidIndex(IsStaleInternal()) == false)
                return;

            var message = failureInformation.GetErrorMessage();

            if (_logger.IsOperationsEnabled)
                _logger.Operations(message);

            SetErrorState(message);
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
            _initialManagedAllocations = new Size(GC.GetAllocatedBytesForCurrentThread(), SizeUnit.Bytes);

            bool mightBeMore = false;

            using (DocumentDatabase.PreventFromUnloading())
            using (CultureHelper.EnsureInvariantCulture())
            using (var context = QueryOperationContext.Allocate(DocumentDatabase, this))
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
            {
                indexContext.PersistentContext.LongLivedTransactions = true;
                context.SetLongLivedTransactions(true);

                using (var tx = indexContext.OpenWriteTransaction())
                using (CurrentIndexingScope.Current =
                    new CurrentIndexingScope(this, DocumentDatabase.DocumentsStorage, context, Definition, indexContext, GetOrAddSpatialField, _unmanagedBuffersPool))
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
                                    mightBeMore |= work.Execute(context, indexContext, writeOperation, scope,
                                        cancellationToken);

                                    if (mightBeMore)
                                        _mre.Set();
                                }
                            }

                            var current = new Size(GC.GetAllocatedBytesForCurrentThread(), SizeUnit.Bytes);
                            stats.AddAllocatedBytes((current - _initialManagedAllocations).GetValue(SizeUnit.Bytes));

                            if (writeOperation.IsValueCreated)
                            {
                                using (var indexWriteOperation = writeOperation.Value)
                                {
                                    indexWriteOperation.Commit(stats);
                                }

                                UpdateThreadAllocations(indexContext, null, null, false);
                            }

                            IndexFieldsPersistence.Persist(indexContext);
                            _indexStorage.WriteReferences(CurrentIndexingScope.Current, tx);
                        }

                        using (stats.For(IndexingOperation.Storage.Commit))
                        {
                            tx.InnerTransaction.LowLevelTransaction.RetrieveCommitStats(out CommitStats commitStats);

                            tx.InnerTransaction.LowLevelTransaction.LastChanceToReadFromWriteTransactionBeforeCommit += llt =>
                            {
                                llt.ImmutableExternalState = IndexPersistence.BuildStreamCacheAfterTx(llt.Transaction);
                            };

                            tx.InnerTransaction.LowLevelTransaction.AfterCommitWhenNewReadTransactionsPrevented += llt =>
                            {
                                IndexPersistence.PublishIndexCacheToNewTransactions((IndexTransactionCache)llt.ImmutableExternalState);

                                if (writeOperation.IsValueCreated == false)
                                    return;

                                using (stats.For(IndexingOperation.Lucene.RecreateSearcher))
                                {
                                    // we need to recreate it after transaction commit to prevent it from seeing uncommitted changes
                                    // also we need this to be called when new read transaction are prevented in order to ensure
                                    // that queries won't get the searcher having 'old' state but see 'new' changes committed here
                                    // e.g. the old searcher could have a segment file in its in-memory state which has been removed in this tx
                                    IndexPersistence.RecreateSearcher(llt.Transaction);
                                    IndexPersistence.RecreateSuggestionsSearchers(llt.Transaction);
                                }
                            };

                            tx.Commit();
                            SlowWriteNotification.Notify(commitStats, DocumentDatabase);
                            stats.RecordCommitStats(commitStats.NumberOfModifiedPages, commitStats.NumberOf4KbsWrittenToDisk);
                        }
                    }
                    catch
                    {
                        DisposeIndexWriterOnError(writeOperation);
                        throw;
                    }

                    return mightBeMore;
                }
            }
        }

        private void DisposeIndexWriterOnError(Lazy<IndexWriteOperation> writeOperation)
        {
            try
            {
                IndexPersistence.DisposeWriters();
            }
            finally
            {
                if (writeOperation.IsValueCreated)
                    writeOperation.Value.Dispose();
            }
        }

        public abstract IIndexedItemEnumerator GetMapEnumerator(IEnumerable<IndexItem> items, string collection, TransactionOperationContext indexContext,
            IndexingStatsScope stats, IndexType type);

        public abstract void HandleDelete(Tombstone tombstone, string collection, IndexWriteOperation writer,
            TransactionOperationContext indexContext, IndexingStatsScope stats);

        public abstract int HandleMap(IndexItem indexItem, IEnumerable mapResults, IndexWriteOperation writer,
            TransactionOperationContext indexContext, IndexingStatsScope stats);

        private void HandleIndexChange(IndexChange change)
        {
            if (string.Equals(change.Name, Name, StringComparison.OrdinalIgnoreCase) == false)
                return;

            if (change.Type == IndexChangeTypes.IndexMarkedAsErrored)
                Stop();
        }

        protected virtual void SubscribeToChanges(DocumentDatabase documentDatabase)
        {
            if (documentDatabase != null)
                documentDatabase.Changes.OnDocumentChange += HandleDocumentChange;

            if (Definition.HasCompareExchange)
                documentDatabase.ServerStore.Cluster.Changes.OnCompareExchangeChange += HandleCompareExchangeChange;
        }

        protected virtual void UnsubscribeFromChanges(DocumentDatabase documentDatabase)
        {
            if (documentDatabase != null)
                documentDatabase.Changes.OnDocumentChange -= HandleDocumentChange;

            if (Definition.HasCompareExchange)
                documentDatabase.ServerStore.Cluster.Changes.OnCompareExchangeChange -= HandleCompareExchangeChange;
        }

        protected virtual void HandleDocumentChange(DocumentChange change)
        {
            if (HandleAllDocs == false && Collections.Contains(change.CollectionName) == false)
                return;
            _mre.Set();
        }

        protected virtual void HandleCompareExchangeChange(CompareExchangeChange change)
        {
            if (string.Equals(DocumentDatabase.Name, change.Database, StringComparison.OrdinalIgnoreCase) == false)
                return;
            _mre.Set();
        }

        public virtual void DeleteErrors()
        {
            using (DrainRunningQueries())
            {
                AssertIndexState(assertState: false);

                _indexStorage.DeleteErrors();
            }
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

        public virtual void SetState(IndexState state, bool inMemoryOnly = false, bool ignoreWriteError = false)
        {
            if (State == state)
                return;

            using (DrainRunningQueries())
            {
                AssertIndexState(assertState: false);

                if (State == state)
                    return;

                var message = $"Changing state for '{Name}' from '{State}' to '{state}'.";

                if (state != IndexState.Error)
                {
                    _errorStateReason = null;

                    if (_logger.IsInfoEnabled)
                        _logger.Info(message);
                }
                else
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations(message + $" Error state reason: {_errorStateReason}");
                }

                var oldState = State;
                State = state;

                if (inMemoryOnly)
                    return;

                try
                {
                    // this might fail if we can't write, so we first update the in memory state
                    _indexStorage.WriteState(state);
                }
                catch (Exception e)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Failed to write {state} state of '{Name}' index to the storage", e);

                    if (ignoreWriteError == false)
                        throw;
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

        public virtual IndexProgress GetProgress(QueryOperationContext queryContext, bool? isStale = null)
        {
            using (CurrentlyInUse(out var valid))
            {
                queryContext.AssertOpenedTransactions();

                var disposed = DocumentDatabase.DatabaseShutdown.IsCancellationRequested || _disposeOne.Disposed;
                if (valid == false || disposed)
                {
                    var progress = new IndexProgress
                    {
                        Name = Name,
                        Type = Type,
                        SourceType = SourceType,
                        IndexRunningStatus = Status,
                        Collections = new Dictionary<string, IndexProgress.CollectionStats>(StringComparer.OrdinalIgnoreCase)
                    };

                    if (disposed)
                        return progress;

                    UpdateIndexProgress(queryContext, progress, null);
                    return progress;
                }

                if (_contextPool == null)
                    throw new ObjectDisposedException("Index " + Name);

                using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var progress = new IndexProgress
                    {
                        Name = Name,
                        Type = Type,
                        SourceType = SourceType,
                        IsStale = isStale ?? IsStale(queryContext, context),
                        IndexRunningStatus = Status,
                        Collections = new Dictionary<string, IndexProgress.CollectionStats>(StringComparer.OrdinalIgnoreCase)
                    };

                    var stats = _indexStorage.ReadStats(tx);

                    UpdateIndexProgress(queryContext, progress, stats);

                    return progress;
                }
            }
        }

        private void UpdateIndexProgress(QueryOperationContext queryContext, IndexProgress progress, IndexStats stats)
        {
            if (progress.IndexRunningStatus == IndexRunningStatus.Running)
            {
                var indexingPerformance = _lastStats?.ToIndexingPerformanceLiveStats();
                if (indexingPerformance?.DurationInMs > 0)
                {
                    progress.ProcessedPerSecond = indexingPerformance.InputCount / (indexingPerformance.DurationInMs / 1000);
                }
            }

            foreach (var collection in GetCollections(queryContext, out var isAllDocs))
            {
                var collectionNameForStats = isAllDocs == false ? collection : Constants.Documents.Collections.AllDocumentsCollection;
                var collectionStats = stats?.Collections[collectionNameForStats];

                var lastEtags = GetLastEtags(_inMemoryIndexProgress, collectionNameForStats,
                    collectionStats?.LastProcessedDocumentEtag ?? 0,
                    collectionStats?.LastProcessedTombstoneEtag ?? 0);

                if (progress.Collections.TryGetValue(collectionNameForStats, out var progressStats) == false)
                {
                    progressStats = progress.Collections[collectionNameForStats] = new IndexProgress.CollectionStats
                    {
                        LastProcessedItemEtag = lastEtags.LastProcessedDocumentEtag,
                        LastProcessedTombstoneEtag = lastEtags.LastProcessedTombstoneEtag
                    };
                }

                UpdateProgressStats(queryContext, progressStats, collection);
            }

            var referencedCollections = GetReferencedCollections();
            if (referencedCollections != null)
            {
                using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
                using (indexContext.OpenReadTransaction())
                {
                    foreach (var referencedCollection in referencedCollections)
                    {
                        foreach (var value in referencedCollection.Value)
                        {
                            var collectionName = value.Name;
                            if (progress.Collections.TryGetValue(collectionName, out var progressStats))
                            {
                                // the collection is already monitored
                                continue;
                            }

                            var lastReferenceEtag = _indexStorage.ReferencesForDocuments.ReadLastProcessedReferenceEtag(indexContext.Transaction.InnerTransaction, referencedCollection.Key, value);
                            var lastReferenceTombstoneEtag = _indexStorage.ReferencesForDocuments.ReadLastProcessedReferenceTombstoneEtag(indexContext.Transaction.InnerTransaction, referencedCollection.Key, value);
                            var lastEtags = GetLastEtags(_inMemoryReferencesIndexProgress, collectionName, lastReferenceEtag, lastReferenceTombstoneEtag);

                            progressStats = progress.Collections[collectionName] = new IndexProgress.CollectionStats
                            {
                                LastProcessedItemEtag = lastEtags.LastProcessedDocumentEtag,
                                LastProcessedTombstoneEtag = lastEtags.LastProcessedTombstoneEtag
                            };

                            UpdateProgressStats(queryContext, progressStats, value.Name);
                        }
                    }
                }
            }
        }

        protected virtual void UpdateProgressStats(QueryOperationContext queryContext, IndexProgress.CollectionStats progressStats, string collectionName)
        {
            progressStats.NumberOfItemsToProcess +=
                DocumentDatabase.DocumentsStorage.GetNumberOfDocumentsToProcess(
                    queryContext.Documents, collectionName, progressStats.LastProcessedItemEtag, out var totalCount);
            progressStats.TotalNumberOfItems += totalCount;

            progressStats.NumberOfTombstonesToProcess +=
                DocumentDatabase.DocumentsStorage.GetNumberOfTombstonesToProcess(
                    queryContext.Documents, collectionName, progressStats.LastProcessedTombstoneEtag, out totalCount);
            progressStats.TotalNumberOfTombstones += totalCount;
        }

        private IEnumerable<string> GetCollections(QueryOperationContext queryContext, out bool isAllDocs)
        {
            if (Collections.Count == 1 && Collections.Contains(Constants.Documents.Collections.AllDocumentsCollection))
            {
                isAllDocs = true;
                return DocumentDatabase.DocumentsStorage.GetCollections(queryContext.Documents).Select(x => x.Name);
            }

            isAllDocs = false;
            return Collections;
        }

        public IndexProgress.CollectionStats GetStats(string collection)
        {
            return _inMemoryIndexProgress.GetOrAdd(collection, _ => new IndexProgress.CollectionStats());
        }

        public IndexProgress.CollectionStats GetReferencesStats(string collection)
        {
            return _inMemoryReferencesIndexProgress.GetOrAdd(collection, _ => new IndexProgress.CollectionStats());
        }

        private static (long LastProcessedDocumentEtag, long LastProcessedTombstoneEtag) GetLastEtags(
            ConcurrentDictionary<string, IndexProgress.CollectionStats> indexProgressStats,
            string collection, long lastProcessedDocumentEtag, long lastProcessedTombstoneEtag)
        {
            if (indexProgressStats.TryGetValue(collection, out var stats) == false)
                return (lastProcessedDocumentEtag, lastProcessedTombstoneEtag);

            var lastDocumentEtag = Math.Max(lastProcessedDocumentEtag, stats.LastProcessedItemEtag);
            var lastTombstoneEtag = Math.Max(lastProcessedTombstoneEtag, stats.LastProcessedTombstoneEtag);
            return (lastDocumentEtag, lastTombstoneEtag);
        }

        public virtual IndexStats GetStats(bool calculateLag = false, bool calculateStaleness = false, bool calculateMemoryStats = false,
            QueryOperationContext queryContext = null)
        {
            using (CurrentlyInUse(out var valid))
            {
                if (valid == false)
                {
                    return new IndexStats
                    {
                        Name = Name,
                        Type = Type,
                        SourceType = SourceType,
                        LockMode = Definition?.LockMode ?? IndexLockMode.Unlock,
                        Priority = Definition?.Priority ?? IndexPriority.Normal,
                        State = State,
                        Status = Status,
                        Collections = Collections.ToDictionary(x => x, _ => new IndexStats.CollectionStats())
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
                    stats.SourceType = SourceType;
                    stats.Type = Type;
                    stats.EntriesCount = reader.EntriesCount();
                    stats.LockMode = Definition.LockMode;
                    stats.Priority = Definition.Priority;
                    stats.State = State;
                    stats.Status = Status;

                    stats.MappedPerSecondRate = MapsPerSec?.OneMinuteRate ?? 0;
                    stats.ReducedPerSecondRate = ReducesPerSec?.OneMinuteRate ?? 0;

                    stats.LastBatchStats = _lastStats?.ToIndexingPerformanceLiveStats();
                    stats.LastQueryingTime = _lastQueryingTime;

                    if (Type == IndexType.MapReduce || Type == IndexType.JavaScriptMapReduce)
                    {
                        var mapReduceIndex = this as MapReduceIndex;
                        stats.ReduceOutputCollection = mapReduceIndex.OutputReduceToCollection?.GetCollectionOfReduceOutput();
                        stats.ReduceOutputReferencePattern = mapReduceIndex.OutputReduceToCollection?.GetPattern();
                        stats.PatternReferencesCollectionName = mapReduceIndex.OutputReduceToCollection?.GetReferenceDocumentsCollectionName();
                    }

                    if (calculateStaleness || calculateLag)
                    {
                        if (queryContext == null)
                            throw new InvalidOperationException("Cannot calculate staleness or lag without valid context.");

                        queryContext.AssertOpenedTransactions();

                        if (calculateStaleness)
                            stats.IsStale = IsStale(queryContext, context);

                        if (calculateLag)
                        {
                            foreach (var collection in Collections)
                            {
                                var collectionStats = stats.Collections[collection];

                                var lastDocumentEtag = GetLastItemEtagInCollection(queryContext, collection);
                                var lastTombstoneEtag = GetLastTombstoneEtagInCollection(queryContext, collection);

                                collectionStats.DocumentLag = Math.Max(0,
                                    lastDocumentEtag - collectionStats.LastProcessedDocumentEtag);
                                collectionStats.TombstoneLag = Math.Max(0,
                                    lastTombstoneEtag - collectionStats.LastProcessedTombstoneEtag);
                            }
                        }
                    }

                    if (calculateMemoryStats)
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
                var threadAllocationsValue = _indexingThread.CurrentThreadStats;
                stats.ThreadAllocations.SizeInBytes = threadAllocationsValue.TotalAllocated;
                if (stats.ThreadAllocations.SizeInBytes < 0)
                    stats.ThreadAllocations.SizeInBytes = 0;
                stats.MemoryBudget.SizeInBytes = _currentMaximumAllowedMemory.GetValue(SizeUnit.Bytes);
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

        public virtual async Task StreamQuery(HttpResponse response, IStreamQueryResultWriter<Document> writer,
            IndexQueryServerSide query, QueryOperationContext queryContext, OperationCancelToken token)
        {
            var result = new StreamDocumentQueryResult(response, writer, token);
            await QueryInternal(result, query, queryContext, pulseDocsReadingTransaction: true, token);
            result.Flush();

            DocumentDatabase.QueryMetadataCache.MaybeAddToCache(query.Metadata, Name);
        }

        public virtual async Task StreamIndexEntriesQuery(HttpResponse response, IStreamQueryResultWriter<BlittableJsonReaderObject> writer,
            IndexQueryServerSide query, QueryOperationContext queryContext, OperationCancelToken token)
        {
            var result = new StreamDocumentIndexEntriesQueryResult(response, writer, token);
            await IndexEntriesQueryInternal(result, query, queryContext, token);
            result.Flush();
            DocumentDatabase.QueryMetadataCache.MaybeAddToCache(query.Metadata, Name);
        }

        public virtual async Task<DocumentQueryResult> Query(
            IndexQueryServerSide query,
            QueryOperationContext queryContext,
            OperationCancelToken token)
        {
            var result = new DocumentQueryResult();
            await QueryInternal(result, query, queryContext, pulseDocsReadingTransaction: false, token: token);
            return result;
        }

        private async Task QueryInternal<TQueryResult>(
            TQueryResult resultToFill,
            IndexQueryServerSide query,
            QueryOperationContext queryContext,
            bool pulseDocsReadingTransaction,
            OperationCancelToken token)
            where TQueryResult : QueryResultServerSide<Document>
        {
            QueryInternalPreparation(query);

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

            using (var marker = MarkQueryAsRunning(query))
            {
                var queryDuration = Stopwatch.StartNew();
                AsyncWaitForIndexing wait = null;
                (long? DocEtag, long? ReferenceEtag, long? CompareExchangeReferenceEtag)? cutoffEtag = null;

                var stalenessScope = query.Timings?.For(nameof(QueryTimingsScope.Names.Staleness), start: false);

                while (true)
                {
                    AssertIndexState();
                    marker.HoldLock();

                    // we take the awaiter _before_ the indexing transaction happens,
                    // so if there are any changes, it will already happen to it, and we'll
                    // query the index again. This is important because of:
                    // https://issues.hibernatingrhinos.com/issue/RavenDB-5576
                    var frozenAwaiter = GetIndexingBatchAwaiter();
                    using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
                    using (var indexTx = indexContext.OpenReadTransaction())
                    {
                        if (queryContext.AreTransactionsOpened() == false)
                            queryContext.OpenReadTransaction();

                        // we have to open read tx for mapResults _after_ we open index tx

                        bool isStale;
                        using (stalenessScope?.Start())
                        {
                            if (query.WaitForNonStaleResults && cutoffEtag == null)
                                cutoffEtag = GetCutoffEtag(queryContext);

                            isStale = IsStale(queryContext, indexContext, cutoffEtag?.DocEtag, cutoffEtag?.ReferenceEtag, cutoffEtag?.CompareExchangeReferenceEtag);
                            if (WillResultBeAcceptable(isStale, query, wait) == false)
                            {
                                ThrowIfPartOfGraphQuery(query); //precaution

                                queryContext.CloseTransaction();

                                Debug.Assert(query.WaitForNonStaleResultsTimeout != null);

                                if (wait == null)
                                    wait = new AsyncWaitForIndexing(queryDuration, query.WaitForNonStaleResultsTimeout.Value, this);

                                marker.ReleaseLock();

                                await wait.WaitForIndexingAsync(frozenAwaiter).ConfigureAwait(false);
                                continue;
                            }
                        }

                        FillQueryResult(resultToFill, isStale, query.Metadata, queryContext, indexContext);

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
                                IncludeTimeSeriesCommand includeTimeSeriesCommand = null;

                                var fieldsToFetch = new FieldsToFetch(query, Definition);

                                var includeDocumentsCommand = new IncludeDocumentsCommand(
                                    DocumentDatabase.DocumentsStorage, queryContext.Documents,
                                    query.Metadata.Includes,
                                    fieldsToFetch.IsProjection);

                                var includeCompareExchangeValuesCommand = IncludeCompareExchangeValuesCommand.ExternalScope(queryContext, query.Metadata.CompareExchangeValueIncludes);

                                if (query.Metadata.CounterIncludes != null)
                                {
                                    includeCountersCommand = new IncludeCountersCommand(
                                        DocumentDatabase,
                                        queryContext.Documents,
                                        query.Metadata.CounterIncludes.Counters);
                                }

                                if (query.Metadata.TimeSeriesIncludes != null)
                                {
                                    includeTimeSeriesCommand = new IncludeTimeSeriesCommand(
                                        queryContext.Documents,
                                        query.Metadata.TimeSeriesIncludes.TimeSeries);
                                }

                                var retriever = GetQueryResultRetriever(query, queryScope, queryContext.Documents, fieldsToFetch, includeDocumentsCommand, includeCompareExchangeValuesCommand);

                                IEnumerable<IndexReadOperation.QueryResult> documents;

                                if (query.Metadata.HasMoreLikeThis)
                                {
                                    documents = reader.MoreLikeThis(
                                        query,
                                        retriever,
                                        queryContext.Documents,
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
                                        queryContext.Documents,
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
                                        queryContext.Documents,
                                        GetOrAddSpatialField,
                                        token.Token);
                                }

                                try
                                {
                                    var enumerator = documents.GetEnumerator();

                                    if (pulseDocsReadingTransaction)
                                    {
                                        var originalEnumerator = enumerator;

                                        enumerator = new PulsedTransactionEnumerator<IndexReadOperation.QueryResult, QueryResultsIterationState>(queryContext.Documents,
                                            state => originalEnumerator,
                                            new QueryResultsIterationState(queryContext.Documents, DocumentDatabase.Configuration.Databases.PulseReadTransactionLimit));
                                    }

                                    using (enumerator)
                                    {
                                        while (enumerator.MoveNext())
                                        {
                                            var document = enumerator.Current;

                                            resultToFill.TotalResults = totalResults.Value;
                                            if (query.Offset != null || query.Limit != null)
                                            {
                                                resultToFill.CappedMaxResults = Math.Min(
                                                    query.Limit ?? int.MaxValue,
                                                    totalResults.Value - (query.Offset ?? 0)
                                                );
                                            }

                                            resultToFill.AddResult(document.Result);

                                            if (document.Highlightings != null)
                                                resultToFill.AddHighlightings(document.Highlightings);

                                            if (document.Explanation != null)
                                                resultToFill.AddExplanation(document.Explanation);

                                            using (gatherScope?.Start())
                                            {
                                                includeDocumentsCommand.Gather(document.Result);
                                                includeCompareExchangeValuesCommand?.Gather(document.Result);
                                            }

                                            includeCountersCommand?.Fill(document.Result);

                                            includeTimeSeriesCommand?.Fill(document.Result);
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    if (resultToFill.SupportsExceptionHandling == false)
                                        throw;

                                    resultToFill.HandleException(e);
                                }

                                using (fillScope?.Start())
                                {
                                    includeDocumentsCommand.Fill(resultToFill.Includes);
                                    includeCompareExchangeValuesCommand?.Materialize();
                                }

                                if (includeCountersCommand != null)
                                    resultToFill.AddCounterIncludes(includeCountersCommand);

                                if (includeTimeSeriesCommand != null)
                                    resultToFill.AddTimeSeriesIncludes(includeTimeSeriesCommand);

                                if (includeCompareExchangeValuesCommand != null)
                                    resultToFill.AddCompareExchangeValueIncludes(includeCompareExchangeValuesCommand);

                                resultToFill.RegisterTimeSeriesFields(query, fieldsToFetch);

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

        private static void ThrowIfPartOfGraphQuery(IndexQueryServerSide query)
        {
            if (query.IsPartOfGraphQuery)
                throw new InvalidOperationException(
                    "Tried to close transaction in the middle of a graph query. This is not supposed to happen and it is likely a bug and should be reported.");
        }

        private async Task IndexEntriesQueryInternal<TQueryResult>(
            TQueryResult resultToFill,
            IndexQueryServerSide query,
            QueryOperationContext queryContext,
            OperationCancelToken token)
          where TQueryResult : QueryResultServerSide<BlittableJsonReaderObject>
        {
            QueryInternalPreparation(query);

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

            using (var marker = MarkQueryAsRunning(query))
            {
                var queryDuration = Stopwatch.StartNew();
                AsyncWaitForIndexing wait = null;
                (long? DocEtag, long? ReferenceEtag, long? CompareExchangeReferenceEtag)? cutoffEtag = null;

                var stalenessScope = query.Timings?.For(nameof(QueryTimingsScope.Names.Staleness), start: false);

                while (true)
                {
                    AssertIndexState();
                    marker.HoldLock();
                    var frozenAwaiter = GetIndexingBatchAwaiter();
                    using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
                    using (var indexTx = indexContext.OpenReadTransaction())
                    {
                        if (queryContext.AreTransactionsOpened() == false)
                            queryContext.OpenReadTransaction();

                        bool isStale;
                        using (stalenessScope?.Start())
                        {
                            if (query.WaitForNonStaleResults && cutoffEtag == null)
                                cutoffEtag = GetCutoffEtag(queryContext);

                            isStale = IsStale(queryContext, indexContext, cutoffEtag?.DocEtag, cutoffEtag?.ReferenceEtag, cutoffEtag?.CompareExchangeReferenceEtag);
                            if (WillResultBeAcceptable(isStale, query, wait) == false)
                            {
                                queryContext.CloseTransaction();

                                Debug.Assert(query.WaitForNonStaleResultsTimeout != null);

                                if (wait == null)
                                    wait = new AsyncWaitForIndexing(queryDuration, query.WaitForNonStaleResultsTimeout.Value, this);

                                marker.ReleaseLock();

                                await wait.WaitForIndexingAsync(frozenAwaiter).ConfigureAwait(false);
                                continue;
                            }
                        }

                        FillQueryResult(resultToFill, isStale, query.Metadata, queryContext, indexContext);

                        using (var reader = IndexPersistence.OpenIndexReader(indexTx.InnerTransaction))
                        {
                            var totalResults = new Reference<int>();

                            foreach (var indexEntry in reader.IndexEntries(query, totalResults, queryContext.Documents, GetOrAddSpatialField, token.Token))
                            {
                                resultToFill.TotalResults = totalResults.Value;
                                resultToFill.AddResult(indexEntry);
                            }
                        }
                        return;
                    }
                }
            }
        }

        private void QueryInternalPreparation(IndexQueryServerSide query)
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
        }

        public virtual async Task<FacetedQueryResult> FacetedQuery(
            FacetQuery facetQuery,
            QueryOperationContext queryContext,
            OperationCancelToken token)
        {
            AssertIndexState();

            if (State == IndexState.Idle)
                SetState(IndexState.Normal);

            var query = facetQuery.Query;

            MarkQueried(DocumentDatabase.Time.GetUtcNow());
            AssertQueryDoesNotContainFieldsThatAreNotIndexed(query.Metadata);

            using (var marker = MarkQueryAsRunning(query))
            {
                var result = new FacetedQueryResult();

                var queryDuration = Stopwatch.StartNew();
                AsyncWaitForIndexing wait = null;
                (long? DocEtag, long? ReferenceEtag, long? CompareExchangeReferenceEtag)? cutoffEtag = null;

                while (true)
                {

                    token.ThrowIfCancellationRequested();

                    AssertIndexState();
                    marker.HoldLock();

                    using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
                    {
                        // we take the awaiter _before_ the indexing transaction happens,
                        // so if there are any changes, it will already happen to it, and we'll
                        // query the index again. This is important because of:
                        // https://issues.hibernatingrhinos.com/issue/RavenDB-5576
                        var frozenAwaiter = GetIndexingBatchAwaiter();
                        using (var indexTx = indexContext.OpenReadTransaction())
                        {
                            if (queryContext.AreTransactionsOpened() == false)
                                queryContext.OpenReadTransaction();
                            // we have to open read tx for mapResults _after_ we open index tx

                            if (query.WaitForNonStaleResults && cutoffEtag == null)
                                cutoffEtag = GetCutoffEtag(queryContext);

                            var isStale = IsStale(queryContext, indexContext, cutoffEtag?.DocEtag, cutoffEtag?.ReferenceEtag, cutoffEtag?.CompareExchangeReferenceEtag);

                            if (WillResultBeAcceptable(isStale, query, wait) == false)
                            {
                                queryContext.CloseTransaction();
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
                                queryContext, indexContext);

                            queryContext.CloseTransaction();

                            using (var reader = IndexPersistence.OpenFacetedIndexReader(indexTx.InnerTransaction))
                            {
                                result.Results = reader.FacetedQuery(facetQuery, queryContext.Documents, GetOrAddSpatialField, token.Token);
                                result.TotalResults = result.Results.Count;
                                return result;
                            }
                        }
                    }
                }
            }
        }

        public virtual TermsQueryResultServerSide GetTerms(string field, string fromValue, long pageSize,
            QueryOperationContext queryContext, OperationCancelToken token)
        {
            AssertIndexState();

            using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
            using (queryContext.OpenReadTransaction())
            using (var tx = indexContext.OpenReadTransaction())
            {
                var result = new TermsQueryResultServerSide
                {
                    IndexName = Name,
                    ResultEtag =
                        CalculateIndexEtag(queryContext, indexContext, null, IsStale(queryContext, indexContext))
                };

                using (var reader = IndexPersistence.OpenIndexReader(tx.InnerTransaction))
                {
                    result.Terms = reader.Terms(field, fromValue, pageSize, token.Token);
                }

                return result;
            }
        }

        public virtual async Task<SuggestionQueryResult> SuggestionQuery(
            IndexQueryServerSide query,
            QueryOperationContext queryContext,
            OperationCancelToken token)
        {
            AssertIndexState();

            if (State == IndexState.Idle)
                SetState(IndexState.Normal);

            MarkQueried(DocumentDatabase.Time.GetUtcNow());
            AssertQueryDoesNotContainFieldsThatAreNotIndexed(query.Metadata);

            using (var marker = MarkQueryAsRunning(query))
            {
                var result = new SuggestionQueryResult();

                var queryDuration = Stopwatch.StartNew();
                AsyncWaitForIndexing wait = null;
                (long? DocEtag, long? ReferenceEtag, long? CompareExchangeReferenceEtag)? cutoffEtag = null;

                while (true)
                {
                    AssertIndexState();
                    marker.HoldLock();

                    using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
                    {
                        // we take the awaiter _before_ the indexing transaction happens,
                        // so if there are any changes, it will already happen to it, and we'll
                        // query the index again. This is important because of:
                        // https://issues.hibernatingrhinos.com/issue/RavenDB-5576
                        var frozenAwaiter = GetIndexingBatchAwaiter();
                        using (var indexTx = indexContext.OpenReadTransaction())
                        {
                            if (queryContext.AreTransactionsOpened() == false)
                                queryContext.OpenReadTransaction();
                            // we have to open read tx for mapResults _after_ we open index tx

                            if (query.WaitForNonStaleResults && cutoffEtag == null)
                                cutoffEtag = GetCutoffEtag(queryContext);

                            var isStale = IsStale(queryContext, indexContext, cutoffEtag?.DocEtag, cutoffEtag?.ReferenceEtag, cutoffEtag?.CompareExchangeReferenceEtag);

                            if (WillResultBeAcceptable(isStale, query, wait) == false)
                            {
                                queryContext.CloseTransaction();
                                Debug.Assert(query.WaitForNonStaleResultsTimeout != null);

                                if (wait == null)
                                    wait = new AsyncWaitForIndexing(queryDuration,
                                        query.WaitForNonStaleResultsTimeout.Value, this);

                                marker.ReleaseLock();

                                await wait.WaitForIndexingAsync(frozenAwaiter).ConfigureAwait(false);
                                continue;
                            }

                            FillSuggestionQueryResult(result, isStale, query.Metadata, queryContext, indexContext);

                            queryContext.CloseTransaction();

                            foreach (var selectField in query.Metadata.SelectFields)
                            {
                                var suggestField = (SuggestionField)selectField;
                                using (var reader = IndexPersistence.OpenSuggestionIndexReader(indexTx.InnerTransaction, suggestField.Name))
                                    result.Results.Add(reader.Suggestions(query, suggestField, queryContext.Documents, token.Token));
                            }

                            result.TotalResults = result.Results.Count;
                            return result;
                        }
                    }
                }
            }
        }

        public virtual async Task<IndexEntriesQueryResult> IndexEntries(
            IndexQueryServerSide query,
            QueryOperationContext queryContext,
            OperationCancelToken token)
        {
            var result = new IndexEntriesQueryResult();
            await IndexEntriesQueryInternal(result, query, queryContext, token);
            return result;
        }

        public abstract (ICollection<string> Static, ICollection<string> Dynamic) GetEntriesFields();

        protected List<string> GetDynamicEntriesFields(HashSet<string> staticFields)
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
            using (var indexTx = indexContext.OpenReadTransaction())
            using (var reader = IndexPersistence.OpenIndexReader(indexTx.InnerTransaction))
            {
                return reader.DynamicEntriesFields(staticFields).ToList();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AssertIndexState(bool assertState = true)
        {
            DocumentDatabase?.DatabaseShutdown.ThrowIfCancellationRequested();

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

        private (long DocumentCutoff, long? ReferenceCutoff, long? CompareExchangeReferenceCutoff) GetCutoffEtag(QueryOperationContext queryContext)
        {
            long cutoffEtag = 0;

            foreach (var collection in Collections)
            {
                var etag = GetLastEtagInCollection(queryContext, collection);

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
                        var etag = GetLastEtagInCollection(queryContext, refCollection.Name);

                        if (referenceCutoffEtag == null || etag > referenceCutoffEtag)
                            referenceCutoffEtag = etag;
                    }
            }

            long? compareExchangeReferenceCutoff = null;
            if (Definition.HasCompareExchange)
                compareExchangeReferenceCutoff = GetLastCompareExchangeEtag();

            return (cutoffEtag, referenceCutoffEtag, compareExchangeReferenceCutoff);

            long GetLastCompareExchangeEtag()
            {
                var lastCompareExchangeEtag = queryContext.Documents.DocumentDatabase.ServerStore.Cluster.GetLastCompareExchangeIndexForDatabase(queryContext.Server, queryContext.Documents.DocumentDatabase.Name);
                var lastCompareExchangeTombstoneEtag = queryContext.Documents.DocumentDatabase.ServerStore.Cluster.GetLastCompareExchangeTombstoneIndexForDatabase(queryContext.Server, queryContext.Documents.DocumentDatabase.Name);

                return Math.Max(lastCompareExchangeEtag, lastCompareExchangeTombstoneEtag);
            }
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
            QueryOperationContext queryContext, TransactionOperationContext indexContext)
        {
            result.IndexName = Name;
            result.IsStale = isStale;
            result.IndexTimestamp = LastIndexingTime ?? DateTime.MinValue;
            result.LastQueryTime = _lastQueryingTime ?? DateTime.MinValue;
            result.ResultEtag = CalculateIndexEtag(queryContext, indexContext, q, result.IsStale) ^ facetSetupEtag;
            result.NodeTag = DocumentDatabase.ServerStore.NodeTag;
        }

        private void FillSuggestionQueryResult(SuggestionQueryResult result, bool isStale, QueryMetadata q,
            QueryOperationContext queryContext, TransactionOperationContext indexContext)
        {
            result.IndexName = Name;
            result.IsStale = isStale;
            result.IndexTimestamp = LastIndexingTime ?? DateTime.MinValue;
            result.LastQueryTime = _lastQueryingTime ?? DateTime.MinValue;
            result.ResultEtag = CalculateIndexEtag(queryContext, indexContext, q, result.IsStale);
            result.NodeTag = DocumentDatabase.ServerStore.NodeTag;
        }

        private void FillQueryResult<TResult, TInclude>(QueryResultBase<TResult, TInclude> result, bool isStale, QueryMetadata q,
            QueryOperationContext queryContext, TransactionOperationContext indexContext)
        {
            result.IndexName = Name;
            result.IsStale = isStale;
            result.IndexTimestamp = LastIndexingTime ?? DateTime.MinValue;
            result.LastQueryTime = _lastQueryingTime ?? DateTime.MinValue;
            result.ResultEtag = CalculateIndexEtag(queryContext, indexContext, q, result.IsStale);
            result.NodeTag = DocumentDatabase.ServerStore.NodeTag;
        }

        private IndexQueryDoneRunning MarkQueryAsRunning(IIndexQuery query)
        {
            if (_firstQuery && _didWork == false)
            {
                _firstBatchTimeout = query.WaitForNonStaleResultsTimeout / 2 ?? DefaultWaitForNonStaleResultsTimeout / 2;
                _firstQuery = false;
            }

            return new IndexQueryDoneRunning(this);
        }

        protected IndexQueryDoneRunning CurrentlyInUse(out bool available)
        {
            var queryDoneRunning = new IndexQueryDoneRunning(this);
            available = queryDoneRunning.TryHoldLock();
            return queryDoneRunning;
        }

        protected IndexQueryDoneRunning CurrentlyInUse()
        {
            var queryDoneRunning = new IndexQueryDoneRunning(this);
            queryDoneRunning.HoldLock();
            return queryDoneRunning;
        }

        internal static readonly TimeSpan DefaultWaitForNonStaleResultsTimeout = TimeSpan.FromSeconds(15); // this matches default timeout from client

        private readonly ConcurrentLruRegexCache _regexCache = new ConcurrentLruRegexCache(1024);

        internal static bool WillResultBeAcceptable(bool isStale, IndexQueryBase<BlittableJsonReaderObject> query, AsyncWaitForIndexing wait)
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

        protected virtual unsafe long CalculateIndexEtag(QueryOperationContext queryContext,
            TransactionOperationContext indexContext, QueryMetadata q, bool isStale)
        {
            var length = MinimumSizeForCalculateIndexEtagLength(q);

            var indexEtagBytes = stackalloc byte[length];

            CalculateIndexEtagInternal(indexEtagBytes, isStale, State, queryContext, indexContext);

            UseAllDocumentsCounterCmpXchgAndTimeSeriesEtags(queryContext, q, length, indexEtagBytes);

            unchecked
            {
                return (long)Hashing.XXHash64.Calculate(indexEtagBytes, (ulong)length);
            }
        }

        protected static unsafe void UseAllDocumentsCounterCmpXchgAndTimeSeriesEtags(QueryOperationContext queryContext,
            QueryMetadata q, int length, byte* indexEtagBytes)
        {
            if (q == null)
                return;

            if (q.HasIncludeOrLoad)
            {
                Debug.Assert(length > sizeof(long) * 4);

                long* buffer = (long*)indexEtagBytes;
                buffer[0] = DocumentsStorage.ReadLastDocumentEtag(queryContext.Documents.Transaction.InnerTransaction);
                buffer[1] = DocumentsStorage.ReadLastTombstoneEtag(queryContext.Documents.Transaction.InnerTransaction);
                //buffer[2] - last processed doc etag
                //buffer[3] - last process tombstone etag
            }

            var hasCounters = q.CounterIncludes != null || q.HasCounterSelect;
            var hasTimeSeries = q.TimeSeriesIncludes != null || q.HasTimeSeriesSelect;
            var hasCmpXchg = q.HasCmpXchg || q.HasCmpXchgSelect || q.HasCmpXchgIncludes;

            if (hasCounters)
            {
                Debug.Assert(length > sizeof(long) * 5, "The index-etag buffer does not have enough space for last counter etag");

                var offset = length - sizeof(long) *
                                       (1 + (hasCmpXchg ? 1 : 0) +
                                        (hasTimeSeries ? 1 : 0));

                *(long*)(indexEtagBytes + offset) = DocumentsStorage.ReadLastCountersEtag(queryContext.Documents.Transaction.InnerTransaction);
            }

            if (hasTimeSeries)
            {
                Debug.Assert(length > sizeof(long) * 5, "The index-etag buffer does not have enough space for last time series etag");

                var offset = length - (sizeof(long) * (hasCmpXchg ? 2 : 1));

                *(long*)(indexEtagBytes + offset) = DocumentsStorage.ReadLastTimeSeriesEtag(queryContext.Documents.Transaction.InnerTransaction);
            }

            if (hasCmpXchg)
            {
                Debug.Assert(length > sizeof(long) * 5, "The index-etag buffer does not have enough space for last compare exchange index");

                *(long*)(indexEtagBytes + length - sizeof(long)) =
                    queryContext.Documents.DocumentDatabase.ServerStore.Cluster
                        .GetLastCompareExchangeIndexForDatabase(queryContext.Server, queryContext.Documents.DocumentDatabase.Name);
            }
        }

        protected int MinimumSizeForCalculateIndexEtagLength(QueryMetadata q)
        {
            var length = sizeof(long) * 4 * Collections.Count + // last document etag, last tombstone etag and last mapped etags per collection
                         sizeof(int) + // definition hash
                         1 + // isStale
                         1; // index state

            if (q == null)
                return length;

            if (q.CounterIncludes != null || q.HasCounterSelect)
                length += sizeof(long); // last counter etag

            if (q.TimeSeriesIncludes != null || q.HasTimeSeriesSelect)
                length += sizeof(long); // last time series etag

            if (q.HasCmpXchg || q.HasCmpXchgSelect || q.HasCmpXchgIncludes)
                length += sizeof(long); //last cmpxchg etag

            return length;
        }

        protected unsafe void CalculateIndexEtagInternal(byte* indexEtagBytes, bool isStale, IndexState indexState,
            QueryOperationContext queryContext, TransactionOperationContext indexContext)
        {
            foreach (var collection in Collections)
            {
                var lastDocEtag = GetLastItemEtagInCollection(queryContext, collection);
                var lastTombstoneEtag = GetLastTombstoneEtagInCollection(queryContext, collection);
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

        public long GetIndexEtag(QueryOperationContext context, QueryMetadata q)
        {
            using (CurrentlyInUse(out var valid))
            {
                if (valid == false)
                    return DateTime.UtcNow.Ticks; // must be always different

                using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
                {
                    using (indexContext.OpenReadTransaction())
                    using (OpenReadTransaction(context))
                    {
                        return CalculateIndexEtag(context, indexContext, q, IsStale(context, indexContext));
                    }
                }

                static IDisposable OpenReadTransaction(QueryOperationContext context)
                {
                    if (context.AreTransactionsOpened())
                        return null;

                    return context.OpenReadTransaction();
                }
            }
        }

        public string TombstoneCleanerIdentifier => $"Index '{Name}'";

        public virtual Dictionary<string, long> GetLastProcessedTombstonesPerCollection(ITombstoneAware.TombstoneType tombstoneType)
        {
            if (tombstoneType != ITombstoneAware.TombstoneType.Documents)
                return null;

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

        internal Dictionary<string, long> GetLastProcessedDocumentTombstonesPerCollection(RavenTransaction tx)
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

        public abstract IQueryResultRetriever GetQueryResultRetriever(IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsOperationContext documentsContext, FieldsToFetch fieldsToFetch, IncludeDocumentsCommand includeDocumentsCommand, IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand);

        public abstract void SaveLastState();

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

                var limit = DocumentDatabase.IsEncrypted
                    ? Configuration.EncryptedTransactionSizeLimit ?? Configuration.TransactionSizeLimit
                    : Configuration.TransactionSizeLimit;

                if (limit != null && Type == IndexType.MapReduce)
                    limit = limit * 0.75;

                _transactionSizeLimit = new Lazy<Size?>(() => limit);

                return _transactionSizeLimit.Value;
            }
        }

        private DateTime _lastCheckedFlushLock;

        public bool ShouldReleaseTransactionBecauseFlushIsWaiting(IndexingStatsScope stats)
        {
            if (GlobalFlushingBehavior.GlobalFlusher.Value.HasLowNumberOfFlushingResources == false)
                return false;

            var now = DateTime.UtcNow;
            if ((now - _lastCheckedFlushLock).TotalSeconds < 1)
                return false;

            _lastCheckedFlushLock = now;

            var gotLock = _indexStorage.Environment().FlushInProgressLock.TryEnterReadLock(0);
            try
            {
                if (gotLock == false)
                {
                    stats.RecordMapCompletedReason("Environment flush was waiting for us and global flusher was about to use all free flushing resources");
                    return true;
                }
            }
            finally
            {
                if (gotLock)
                    _indexStorage.Environment().FlushInProgressLock.ExitReadLock();
            }

            return false;
        }

        public bool CanContinueBatch(
            IndexingStatsScope stats,
            QueryOperationContext queryContext,
            TransactionOperationContext indexingContext,
            IndexWriteOperation indexWriteOperation,
            long count)
        {
            var txAllocationsInBytes = UpdateThreadAllocations(indexingContext, indexWriteOperation, stats, updateReduceStats: false);

            //We need to take the read transaction encryption size into account as we might read alot of document and produce very little indexing output.
            txAllocationsInBytes += queryContext.Documents.Transaction.InnerTransaction.LowLevelTransaction.AdditionalMemoryUsageSize.GetValue(SizeUnit.Bytes);

            if (_indexDisabled)
            {
                stats.RecordMapCompletedReason("Index was disabled");
                return false;
            }

            var cpuCreditsAlertFlag = DocumentDatabase.ServerStore.Server.CpuCreditsBalance.BackgroundTasksAlertRaised;
            if (cpuCreditsAlertFlag.IsRaised())
            {
                HandleStoppedBatchesConcurrently(stats, count,
                   canContinue: () => cpuCreditsAlertFlag.IsRaised() == false,
                   reason: "CPU credits balance is low");

                stats.RecordMapCompletedReason($"The batch was stopped after processing {count:#,#;;0} documents because the CPU credits balance is almost completely used");
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

            if (DocumentDatabase.Is32Bits)
            {
                IPagerLevelTransactionState pagerLevelTransactionState = queryContext.Documents.Transaction?.InnerTransaction?.LowLevelTransaction;
                var total32BitsMappedSize = pagerLevelTransactionState?.GetTotal32BitsMappedSize();
                if (total32BitsMappedSize > MappedSizeLimitOn32Bits)
                {
                    stats.RecordMapCompletedReason($"Running in 32 bits and have {total32BitsMappedSize} mapped in docs ctx");
                    return false;
                }

                pagerLevelTransactionState = indexingContext.Transaction?.InnerTransaction?.LowLevelTransaction;
                total32BitsMappedSize = pagerLevelTransactionState?.GetTotal32BitsMappedSize();
                if (total32BitsMappedSize > MappedSizeLimitOn32Bits)
                {
                    stats.RecordMapCompletedReason($"Running in 32 bits and have {total32BitsMappedSize} mapped in index ctx");
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

            if (Configuration.ManagedAllocationsBatchLimit != null && 
                count % 128 == 0)
            {
                var currentManagedAllocations = new Size(GC.GetAllocatedBytesForCurrentThread(), SizeUnit.Bytes);
                var diff = currentManagedAllocations - _initialManagedAllocations;
                stats.AddAllocatedBytes(diff.GetValue(SizeUnit.Bytes));

                if (diff > Configuration.ManagedAllocationsBatchLimit.Value)
                {
                    stats.RecordMapCompletedReason($"Reached managed allocations limit ({Configuration.ManagedAllocationsBatchLimit.Value}). Allocated {diff} in current batch");
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
                    Interlocked.Increment(ref _allocationCleanupNeeded);

                    queryContext.Documents.DoNotReuse = true;
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

            txAllocations += indexingContext.Transaction.InnerTransaction.LowLevelTransaction.AdditionalMemoryUsageSize.GetValue(SizeUnit.Bytes);

            long indexWriterAllocations = 0;
            long luceneFilesAllocations = 0;

            if (indexWriteOperation != null)
            {
                var allocations = indexWriteOperation.GetAllocations();
                indexWriterAllocations = allocations.RamSizeInBytes;
                luceneFilesAllocations = allocations.FilesAllocationsInBytes;
            }

            var totalTxAllocations = txAllocations + luceneFilesAllocations;

            if (stats != null)
            {
                var allocatedForStats = threadAllocations + totalTxAllocations + indexWriterAllocations;
                if (updateReduceStats)
                {
                    stats.RecordReduceAllocations(allocatedForStats);
                }
                else
                {
                    stats.RecordMapAllocations(allocatedForStats);
                }
            }

            var allocatedForProcessing = threadAllocations + indexWriterAllocations +
                                         // we multiple it to take into account additional work
                                         // that will need to be done during the commit phase of the index
                                         (long)(totalTxAllocations * _txAllocationsRatio);

            _threadAllocations.CurrentlyAllocatedForProcessing = allocatedForProcessing;

            return totalTxAllocations;
        }

        private void HandleStoppedBatchesConcurrently(
            IndexingStatsScope stats, long count,
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
                    _logger.Info($"{Name} is still waiting for other indexes to complete their batches because there is a {reason} condition in action...");
            }
        }

        public void Compact(Action<IOperationProgress> onProgress, CompactionResult result, CancellationToken token)
        {
            if (_isCompactionInProgress)
                throw new InvalidOperationException($"Index '{Name}' cannot be compacted because compaction is already in progress.");

            result.SizeBeforeCompactionInMb = CalculateIndexStorageSize().GetValue(SizeUnit.Megabytes);

            if (_environment.Options.IncrementalBackupEnabled)
                throw new InvalidOperationException(
                    $"Index '{Name}' cannot be compacted because incremental backup is enabled.");

            if (Configuration.RunInMemory)
                throw new InvalidOperationException(
                    $"Index '{Name}' cannot be compacted because it runs in memory.");

            result.AddMessage($"Starting compaction of index '{Name}'.");
            result.AddMessage($"Draining queries for {Name}.");
            onProgress?.Invoke(result.Progress);

            using (DrainRunningQueries())
            {
                _isCompactionInProgress = true;
                PathSetting compactPath = null;
                PathSetting tempPath = null;

                try
                {
                    var storageEnvironmentOptions = _environment.Options;

                    using (RestartEnvironment(onBeforeEnvironmentDispose: Optimize))
                    {
                        if (Type.IsMapReduce())
                        {
                            result.AddMessage($"Skipping data compaction of '{Name}' index because data compaction of map-reduce indexes isn't supported");
                            onProgress?.Invoke(result.Progress);
                            result.TreeName = null;
                            result.SizeAfterCompactionInMb = CalculateIndexStorageSize().GetValue(SizeUnit.Megabytes);

                            return;
                        }

                        var environmentOptions =
                                                (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)storageEnvironmentOptions;
                        var srcOptions = StorageEnvironmentOptions.ForPath(environmentOptions.BasePath.FullPath, environmentOptions.TempPath?.FullPath, null, DocumentDatabase.IoChanges,
                            DocumentDatabase.CatastrophicFailureNotification);

                        InitializeOptions(srcOptions, DocumentDatabase, Name, schemaUpgrader: false);

                        compactPath = Configuration.StoragePath.Combine(IndexDefinitionBase.GetIndexNameSafeForFileSystem(Name) + "_Compact");
                        tempPath = Configuration.TempPath?.Combine(IndexDefinitionBase.GetIndexNameSafeForFileSystem(Name) + "_Temp_Compact");

                        using (var compactOptions = (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)
                            StorageEnvironmentOptions.ForPath(compactPath.FullPath, tempPath?.FullPath, null, DocumentDatabase.IoChanges,
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
                            }, null, token);
                        }

                        // reset tree name back to null after processing
                        result.TreeName = null;

                        IOExtensions.DeleteDirectory(environmentOptions.BasePath.FullPath);
                        IOExtensions.MoveDirectory(compactPath.FullPath, environmentOptions.BasePath.FullPath);

                        if (tempPath != null)
                            IOExtensions.DeleteDirectory(tempPath.FullPath);
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

                void Optimize()
                {
                    result.AddMessage($"Starting data optimization of index '{Name}'.");
                    onProgress?.Invoke(result.Progress);

                    using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
                    using (var txw = indexContext.OpenWriteTransaction())
                    using (var writer = IndexPersistence.OpenIndexWriter(indexContext.Transaction.InnerTransaction, indexContext))
                    {
                        writer.Optimize();

                        txw.Commit();
                    }
                }
            }
        }

        public IDisposable RestartEnvironment(Action onBeforeEnvironmentDispose = null)
        {
            // shutdown environment
            if (_currentlyRunningQueriesLock.IsWriteLockHeld == false)
                throw new InvalidOperationException("Expected to be called only via DrainRunningQueries");

            // here we ensure that we aren't currently running any indexing,
            // because we'll shut down the environment for this index, reads
            // are handled using the DrainRunningQueries portion
            var thread = GetWaitForIndexingThreadToExit(disableIndex: false);
            thread?.Join(Timeout.Infinite);

            onBeforeEnvironmentDispose?.Invoke();

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
                    _environment = StorageLoader.OpenEnvironment(options, StorageEnvironmentWithType.StorageEnvironmentType.Index);
                    InitializeComponentsUsingEnvironment(DocumentDatabase, _environment);
                }
                catch
                {
                    Dispose();
                    options.Dispose();
                    throw;
                }

                if (thread != null)
                {
                    // we want to start indexing thread only if we stopped it
                    StartIndexingThread();
                }
            });
        }

        public Size CalculateIndexStorageSize()
        {
            var storageReport = _environment.GenerateSizeReport(includeTempBuffers: false);
            var sizeOnDiskInBytes = storageReport.DataFileInBytes + storageReport.JournalsInBytes;
            return new Size(sizeOnDiskInBytes, SizeUnit.Bytes);
        }

        public long GetLastEtagInCollection(QueryOperationContext queryContext, string collection)
        {
            long lastDocEtag;
            long lastTombstoneEtag;
            if (collection == Constants.Documents.Collections.AllDocumentsCollection)
            {
                lastDocEtag = DocumentsStorage.ReadLastDocumentEtag(queryContext.Documents.Transaction.InnerTransaction);
                lastTombstoneEtag = DocumentsStorage.ReadLastTombstoneEtag(queryContext.Documents.Transaction.InnerTransaction);
            }
            else
            {
                lastDocEtag = DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(queryContext.Documents.Transaction.InnerTransaction, collection);
                lastTombstoneEtag = DocumentDatabase.DocumentsStorage.GetLastTombstoneEtag(queryContext.Documents.Transaction.InnerTransaction, collection);
            }
            return Math.Max(lastDocEtag, lastTombstoneEtag);
        }

        public virtual long GetLastItemEtagInCollection(QueryOperationContext queryContext, string collection)
        {
            return collection == Constants.Documents.Collections.AllDocumentsCollection
                ? DocumentsStorage.ReadLastDocumentEtag(queryContext.Documents.Transaction.InnerTransaction)
                : DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(queryContext.Documents.Transaction.InnerTransaction, collection);
        }

        public virtual long GetLastTombstoneEtagInCollection(QueryOperationContext queryContext, string collection)
        {
            return collection == Constants.Documents.Collections.AllDocumentsCollection
                ? DocumentsStorage.ReadLastTombstoneEtag(queryContext.Documents.Transaction.InnerTransaction)
                : DocumentDatabase.DocumentsStorage.GetLastTombstoneEtag(queryContext.Documents.Transaction.InnerTransaction, collection);
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

        public void LowMemory(LowMemorySeverity lowMemorySeverity)
        {
            _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;
            Interlocked.Increment(ref _allocationCleanupNeeded);
            _lowMemoryFlag.Raise();
        }

        public void LowMemoryOver()
        {
            var oldValue = _lowMemoryPressure;
            var newValue = Math.Max(0, oldValue - 1);

            if (Interlocked.CompareExchange(ref _lowMemoryPressure, newValue, oldValue) == oldValue && newValue == 0)
            {
                _lowMemoryFlag.Lower();
            }
        }

        internal void SimulateLowMemory()
        {
            _lowMemoryPressure = LowMemoryPressure;
            LowMemory(LowMemorySeverity.ExtremelyLow);
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

        private static bool TryFindIndexDefinition(string directoryName, RawDatabaseRecord record, out IndexDefinition staticDef, out AutoIndexDefinition autoDef)
        {
            var indexes = record.Indexes;
            var autoIndexes = record.AutoIndexes;

            if (indexes != null)
            {
                foreach (var index in indexes)
                {
                    if (directoryName == IndexDefinitionBase.GetIndexNameSafeForFileSystem(index.Key))
                    {
                        staticDef = index.Value;
                        autoDef = null;
                        return true;
                    }
                }
            }

            if (autoIndexes != null)
            {
                foreach (var index in autoIndexes)
                {
                    if (directoryName == IndexDefinitionBase.GetIndexNameSafeForFileSystem(index.Key))
                    {
                        autoDef = index.Value;
                        staticDef = null;
                        return true;
                    }
                }
            }

            staticDef = null;
            autoDef = null;

            return false;
        }

        protected struct IndexQueryDoneRunning : IDisposable
        {
            private static readonly TimeSpan DefaultLockTimeout = TimeSpan.FromSeconds(3);

            private static readonly TimeSpan ExtendedLockTimeout = TimeSpan.FromSeconds(30);

            private readonly Index _parent;
            private bool _hasLock;

            public IndexQueryDoneRunning(Index parent)
            {
                _parent = parent;
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
            }
        }

        internal struct ExitWriteLock : IDisposable
        {
            private readonly ReaderWriterLockSlim _rwls;

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

        public int Dump(string path, Action<IOperationProgress> onProgress)
        {
            if (Directory.Exists(path) == false)
                Directory.CreateDirectory(path);

            using (CurrentlyInUse())
            using (var tx = _environment.ReadTransaction())
            {
                var state = new Indexing.VoronState(tx);

                var files = IndexPersistence.LuceneDirectory.ListAll(state);
                var currentFile = 0;
                var buffer = new byte[64 * 1024];
                var sp = Stopwatch.StartNew();
                foreach (var file in files)
                {
                    using (var input = IndexPersistence.LuceneDirectory.OpenInput(file, state))
                    using (var output = File.Create(Path.Combine(path, file)))
                    {
                        var currentFileLength = input.Length(state);
                        var message = "Exporting file: " + file;
                        onProgress(new AdminIndexHandler.DumpIndexProgress
                        {
                            Message = message,
                            TotalFiles = files.Length,
                            ProcessedFiles = currentFile,
                            CurrentFileSizeInBytes = currentFileLength
                        });
                        long currentFileOverallReadBytes = 0;
                        while (currentFileLength > currentFileOverallReadBytes)
                        {
                            int read = (int)Math.Min(buffer.Length, currentFileLength - currentFileOverallReadBytes);
                            input.ReadBytes(buffer, 0, read, state);
                            currentFileOverallReadBytes += read;
                            output.Write(buffer, 0, read);
                            if (sp.ElapsedMilliseconds > 1000)
                            {
                                onProgress(new AdminIndexHandler.DumpIndexProgress
                                {
                                    Message = message,
                                    TotalFiles = files.Length,
                                    ProcessedFiles = currentFile,
                                    CurrentFileSizeInBytes = currentFileLength,
                                    CurrentFileCopiedBytes = currentFileOverallReadBytes
                                });
                                sp.Restart();
                            }
                        }
                    }
                    currentFile++;
                }

                return files.Length;
            }
        }
    }
}
