using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Nito.AsyncEx;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.DataArchival;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions.Corax;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Extensions;
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
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Raven.Server.Documents.Indexes.Test;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Documents.Sharding;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Indexes;
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
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Voron;
using Voron.Data.Containers;
using Voron.Data.PostingLists;
using Voron.Debugging;
using Voron.Exceptions;
using Voron.Impl;
using Voron.Impl.Compaction;
using Voron.Impl.Journal;
using AsyncManualResetEvent = Sparrow.Server.AsyncManualResetEvent;
using Constants = Raven.Client.Constants;
using FacetQuery = Raven.Server.Documents.Queries.Facets.FacetQuery;
using NativeMemory = Sparrow.Utils.NativeMemory;
using Size = Sparrow.Size;

namespace Raven.Server.Documents.Indexes
{
    public abstract class Index<TIndexDefinition, TField> : Index
        where TIndexDefinition : IndexDefinitionBaseServerSide<TField> where TField : IndexFieldBase
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

        internal IndexPersistenceBase IndexPersistence;

        internal IndexFieldsPersistence IndexFieldsPersistence;

        private readonly AsyncManualResetEvent _indexingBatchCompleted = new AsyncManualResetEvent();

        private readonly SemaphoreSlim _doingIndexingWork = new SemaphoreSlim(1, 1);

        private readonly SemaphoreSlim _executingIndexing = new SemaphoreSlim(1, 1);


        private long _allocatedAfterPreviousCleanup = 0;

        /// <summary>
        /// Cancelled if the database is in shutdown process.
        /// </summary>
        private CancellationTokenSource _indexingProcessCancellationTokenSource;

        internal CancellationToken IndexingProcessCancellationToken => _indexingProcessCancellationTokenSource.Token;

        private bool _indexDisabled;

        private readonly ConcurrentDictionary<string, IndexProgress.CollectionStats> _inMemoryIndexProgress =
            new ConcurrentDictionary<string, IndexProgress.CollectionStats>();

        private readonly ConcurrentDictionary<string, IndexProgress.CollectionStats> _inMemoryReferencesIndexProgress =
            new ConcurrentDictionary<string, IndexProgress.CollectionStats>();

        private ShardedDocumentDatabase _shardedDocumentDatabase;

        internal DocumentDatabase DocumentDatabase;

        internal PoolOfThreads.LongRunningWork _indexingThread;

        private bool CalledUnderIndexingThread => _indexingThread?.ManagedThreadId == Thread.CurrentThread.ManagedThreadId;

        private bool _initialized;

        internal UnmanagedBuffersPoolWithLowMemoryHandling _unmanagedBuffersPool;

        internal StorageEnvironment _environment;

        internal TransactionContextPool _contextPool;
        private ByteStringContext _indexAllocator;
        private List<(Slice First, Slice Second)> _compoundFields;

        internal ThrottledManualResetEventSlim _mre;
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
        public bool IsRolling => Definition.DeploymentMode == IndexDeploymentMode.Rolling;
        public bool DeployedOnAllNodes
        {
            get
            {
                if (IsRolling == false)
                    return true;

                // if we have no replacement - we are deploying, otherwise the replacement is deploying
                if (DocumentDatabase.IndexStore.HasReplacement(Name))
                    return true;

                return GetNumberOfDeployedNodes() == -1;
            }
        }

        public string NormalizedName => Name.Replace(Constants.Documents.Indexing.SideBySideIndexNamePrefix, string.Empty);

        private string _lastPendingStatus;
        public MultipleUseFlag ForceReplace = new MultipleUseFlag();

        protected readonly bool HandleAllDocs;

        protected internal MeterMetric MapsPerSec;
        protected internal MeterMetric ReducesPerSec;

        protected internal IndexingConfiguration Configuration;

        protected PerformanceHintsConfiguration PerformanceHintsConfig;

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

        private readonly AsyncReaderWriterLock _currentlyRunningQueriesLock = new AsyncReaderWriterLock();
        private readonly AsyncLocal<bool> _isRunningQueriesWriteLockTaken = new AsyncLocal<bool>();
        private readonly MultipleUseFlag _priorityChanged = new MultipleUseFlag();
        private readonly MultipleUseFlag _hadRealIndexingWorkToDo = new MultipleUseFlag();
        internal bool HadRealIndexingWork => _hadRealIndexingWorkToDo.IsRaised();

        private readonly MultipleUseFlag _definitionChanged = new MultipleUseFlag();
        private Size _initialManagedAllocations;

        private readonly ConcurrentDictionary<string, SpatialField> _spatialFields = new ConcurrentDictionary<string, SpatialField>(StringComparer.OrdinalIgnoreCase);

        internal readonly QueryBuilderFactories _queryBuilderFactories;

        private string IndexingThreadName => "Indexing of " + Name + " of " + _indexStorage.DocumentDatabase.Name;

        private readonly WarnIndexOutputsPerDocument.WarningDetails _indexOutputsPerDocumentWarning = new WarnIndexOutputsPerDocument.WarningDetails
        {
            MaxNumberOutputsPerDocument = int.MinValue,
            Suggestion = "Please verify index definitions and consider a re-design of your entities or indexes for better indexing performance."
        };

        private IndexingReferenceLoadWarning.WarningDetails _referenceLoadWarning;

        private bool _updateReferenceLoadWarning;

        private static readonly Size DefaultMaximumMemoryAllocation = new Size(32, SizeUnit.Megabytes);

        public long? LastTransactionId => _environment?.CurrentReadTransactionId;

        internal bool IsLowMemory => _lowMemoryFlag.IsRaised();

        private readonly double _txAllocationsRatio;

        private readonly string _itemType;

        internal bool SourceDocumentIncludedInOutput;
        private bool _alreadyNotifiedAboutIncludingDocumentInOutput;

        public bool IsTestRun => TestRun != null;

        public TestIndexRun TestRun;
        
        private HashSet<string> _fieldsReportedAsComplex = new();
        private bool _newComplexFieldsToReport = false;
        
        protected Index(IndexType type, IndexSourceType sourceType, IndexDefinitionBaseServerSide definition)
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

            _disposeOnce = new DisposeOnce<SingleAttempt>(() =>
            {
                using (DrainRunningQueries())
                    DisposeIndex();
            });
        }

        public void ScheduleIndexingRun()
        {
            _mre.Set(ignoreThrottling: true);
        }

        protected virtual void DisposeIndex()
        {
            if (_isRunningQueriesWriteLockTaken.Value == false)
                throw new InvalidOperationException("Are you trying to dispose an index without a lock?");

            _isRunningQueriesWriteLockTaken.Value = true;

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
            
            exceptionAggregator.Execute(() => { _indexAllocator?.Dispose(); });

            exceptionAggregator.Execute(() => { _indexingProcessCancellationTokenSource?.Dispose(); });

            exceptionAggregator.Execute(() => { _mre?.Dispose(); });

            exceptionAggregator.ThrowIfNeeded();
        }

        public static Index Open(string path, DocumentDatabase documentDatabase, bool generateNewDatabaseId, out SearchEngineType searchEngineType)
        {
            var logger = LoggingSource.Instance.GetLogger<Index>(documentDatabase.Name);
            StorageEnvironment environment = null;
            searchEngineType = SearchEngineType.None;

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
                    try
                    {
                        searchEngineType = IndexStorage.ReadSearchEngineType(name, environment);
                    }
                    catch
                    {
                        // Since we only want to present the index search engine to the user when it's possible, we can simply ignore it when it's not possible
                        // (for instance, if the index dates back to the pre-Corax era).
                    }
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
                            var definition = IndexStore.CreateAutoDefinition(autoDef, documentDatabase.Configuration.Indexing.AutoIndexDeploymentMode);

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

                if (documentDatabase.Configuration.Indexing.SkipDatabaseIdValidationOnIndexOpening == false && generateNewDatabaseId == false)
                {
                    var databaseId = IndexStorage.ReadDatabaseId(name, environment);
                    if (databaseId != null) // backward compatibility
                    {
                        if (databaseId != documentDatabase.DbBase64Id)
                            throw new IndexOpenException($"Could not open index because stored database ID ('{databaseId}') is different than current database ID ('{documentDatabase.DbBase64Id}'). A common reason for this is that the index was copied from another database.");
                    }
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

        public SearchEngineType SearchEngineType;

        public IndexSourceType SourceType { get; }

        public IndexState State { get; protected set; }

        public IndexDefinitionBaseServerSide Definition { get; private set; }

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
            if (_disposeOnce.Disposed)
                ThrowObjectDisposed();

            return _indexingBatchCompleted.GetFrozenAwaiter();
        }

        [DoesNotReturn]
        internal static void ThrowObjectDisposed()
        {
            throw new ObjectDisposedException("index");
        }

        protected void Initialize(DocumentDatabase documentDatabase, IndexingConfiguration configuration, PerformanceHintsConfiguration performanceHints)
        {
            InitializeMetrics(configuration);

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

        private void InitializeMetrics(IndexingConfiguration configuration)
        {
            if (configuration.EnableMetrics)
            {
                ReducesPerSec = new MeterMetric();
                MapsPerSec = new MeterMetric();
            }
            else
            {
                ReducesPerSec = null;
                MapsPerSec = null;
            }
        }

        public CurrentIndexingScope CreateIndexingScope(TransactionOperationContext indexContext, QueryOperationContext queryContext)
        {
            return new CurrentIndexingScope(this, DocumentDatabase.DocumentsStorage, queryContext, Definition, indexContext, GetOrAddSpatialField, _unmanagedBuffersPool);
        }

        private StorageEnvironmentOptions CreateStorageEnvironmentOptions(DocumentDatabase documentDatabase, IndexingConfiguration configuration)
        {
            var name = IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(Name);

            var indexPath = configuration.StoragePath.Combine(name);

            var indexTempPath = configuration.TempPath?.Combine(name);

            var options = configuration.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly(indexPath.FullPath, indexTempPath?.FullPath ?? Path.Combine(indexPath.FullPath, "Temp"),
                    documentDatabase.IoChanges, documentDatabase.CatastrophicFailureNotification)
                : StorageEnvironmentOptions.ForPath(indexPath.FullPath, indexTempPath?.FullPath, null,
                    documentDatabase.IoChanges, documentDatabase.CatastrophicFailureNotification);

            var searchEngineType = Type.IsAuto() ? configuration.AutoIndexingEngineType : configuration.StaticIndexingEngineType;
            InitializeOptions(options, documentDatabase, name, searchEngineType: searchEngineType);

            return options;
        }

        private static void InitializeOptions(StorageEnvironmentOptions options, DocumentDatabase documentDatabase, string name, bool schemaUpgrader = true, SearchEngineType searchEngineType = SearchEngineType.None)
        {
            options.OnNonDurableFileSystemError += documentDatabase.HandleNonDurableFileSystemError;
            options.OnRecoveryError += (s, e) => documentDatabase.HandleOnIndexRecoveryError(name, s, e);
            options.OnIntegrityErrorOfAlreadySyncedData += (s, e) => documentDatabase.HandleOnIndexIntegrityErrorOfAlreadySyncedData(name, s, e);
            options.OnRecoverableFailure += documentDatabase.HandleRecoverableFailure;
            options.CompressTxAboveSizeInBytes = documentDatabase.Configuration.Storage.CompressTxAboveSize.GetValue(SizeUnit.Bytes);
            options.ForceUsing32BitsPager = documentDatabase.Configuration.Storage.ForceUsing32BitsPager;
            options.EnablePrefetching = documentDatabase.Configuration.Storage.EnablePrefetching;
            options.DiscardVirtualMemory = documentDatabase.Configuration.Storage.DiscardVirtualMemory;
            options.TimeToSyncAfterFlushInSec = (int)documentDatabase.Configuration.Storage.TimeToSyncAfterFlush.AsTimeSpan.TotalSeconds;
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
            options.MaxNumberOfRecyclableJournals = documentDatabase.Configuration.Storage.MaxNumberOfRecyclableJournals;

            if (documentDatabase.ServerStore.GlobalIndexingScratchSpaceMonitor != null)
                options.ScratchSpaceUsage.AddMonitor(documentDatabase.ServerStore.GlobalIndexingScratchSpaceMonitor);

            if (schemaUpgrader)
            {
                options.SchemaVersion = SchemaUpgrader.CurrentVersion.GetIndexVersionAndStorageType(searchEngineType).Version;
                options.OnVersionReadingTransaction = tx =>
                {
                    var searchEngineTypeFromSchema = SearchEngineType.None;
                    var configurationTree = tx.ReadTree(IndexStorage.IndexSchema.ConfigurationTree);
                    if (configurationTree != null)
                    {
                        var result = configurationTree.Read(IndexStorage.IndexSchema.SearchEngineType);
                        if (result != null)
                            if (Enum.TryParse(result.Reader.ToStringValue(), out searchEngineTypeFromSchema) == false)
                                searchEngineTypeFromSchema = SearchEngineType.None;
                    }

                    var currentVersion = SchemaUpgrader.CurrentVersion.GetIndexVersionAndStorageType(searchEngineTypeFromSchema);
                    options.SchemaVersion = currentVersion.Version;
                    options.SchemaUpgrader = SchemaUpgrader.Upgrader(currentVersion.Type, null, null, null);
                };
            }

            if (options is not StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)
                return;

            string disableMarkerPath = options.BasePath.Combine("disable.marker").FullPath;
            if (File.Exists(disableMarkerPath))
            {
                throw new IndexOpenException(
                    $"Unable to open index: '{name}', it has been manually disabled via the file: '{disableMarkerPath}'. To re-enable, remove the disable.marker file and enable indexing.");
            }
        }

        internal IDisposable DrainRunningQueries()
        {
            if (_isRunningQueriesWriteLockTaken.Value)
                return null;

            IDisposable currentlyRunningQueriesWriteLock;

            try
            {
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
                    currentlyRunningQueriesWriteLock = _currentlyRunningQueriesLock.WriterLock(cts.Token);

                _isRunningQueriesWriteLockTaken.Value = true;
            }
            catch (OperationCanceledException)
            {
                if (_disposeOnce.Disposed)
                    ThrowObjectDisposed();

                throw new TimeoutException("After waiting for 10 seconds for all running queries ");
            }

            return new ExitWriteLock(currentlyRunningQueriesWriteLock, this);
        }

        protected void Initialize(
            StorageEnvironment environment,
            DocumentDatabase documentDatabase,
            IndexingConfiguration configuration,
            PerformanceHintsConfiguration performanceHints)
        {
            configuration.InitializeAnalyzers(documentDatabase.Name);

            if (_disposeOnce.Disposed)
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
                _shardedDocumentDatabase = documentDatabase as ShardedDocumentDatabase;
                Configuration = configuration;
                PerformanceHintsConfig = performanceHints;

                _mre = new ThrottledManualResetEventSlim(Configuration.ThrottlingTimeInterval?.AsTimeSpan, timerManagement: ThrottledManualResetEventSlim.TimerManagement.Manual);
                _logger = LoggingSource.Instance.GetLogger<Index>(documentDatabase.Name);
                _environment = environment;
                var safeName = IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(Name);
                _unmanagedBuffersPool = new UnmanagedBuffersPoolWithLowMemoryHandling($"Indexes//{safeName}");
                _regexCache = new(ConcurrentLruRegexCache.DefaultCapacity, documentDatabase.Configuration.Queries.RegexTimeout.AsTimeSpan);
                InitializeComponentsUsingEnvironment(documentDatabase, _environment);

                InitializeCompoundFields();

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

        public bool HasCompoundField(Slice first, Slice second, out int bindingId)
        {
            bindingId = 0;
            if (_compoundFields == null)
                return false;
            var span = CollectionsMarshal.AsSpan(_compoundFields);
            for (int i = 0; i < span.Length; i++)
            {
                ref var cur = ref span[i];
                if (cur.First.AsSpan().SequenceEqual(first.AsSpan()) &&
                    cur.Second.AsSpan().SequenceEqual(second.AsSpan()))
                {
                    bindingId = 1 + Definition.IndexFields.Count - span.Length + i; // 1 is ID()/hash(key) field.
                    return true;
                }
            }
            return false;
        }
        
        private void InitializeCompoundFields()
        {
            var indexDefinition = GetIndexDefinition();
            if (indexDefinition.CompoundFields is not {Count: > 0})
                return;
            
            _indexAllocator = new ByteStringContext(SharedMultipleUseFlag.None);
            _compoundFields = new List<(Slice, Slice)>();

            for (int index = 0; index < indexDefinition.CompoundFields.Count; index++)
            {
                string[] compoundField = indexDefinition.CompoundFields[index];
                Debug.Assert(compoundField.Length == 2);
                Slice.From(_indexAllocator, compoundField[0], out var fst);
                Slice.From(_indexAllocator, compoundField[1], out var snd);
                _compoundFields.Add((fst, snd));
            }
        }

        private bool IsStaleInternal(List<string> stalenessReasons = null)
        {
            if (_indexingProcessCancellationTokenSource.IsCancellationRequested)
                return true;

            using (var context = QueryOperationContext.Allocate(DocumentDatabase, this))
            using (context.OpenReadTransaction())
            {
                return IsStale(context, stalenessReasons: stalenessReasons);
            }
        }

        private void InitializeComponentsUsingEnvironment(DocumentDatabase documentDatabase, StorageEnvironment environment)
        {
            _contextPool?.Dispose();
            _contextPool = new TransactionContextPool(environment, documentDatabase.Configuration.Memory.MaxContextSizeToKeep);

            _indexStorage = new IndexStorage(this, _contextPool, documentDatabase);
            _indexStorage.Initialize(documentDatabase, environment);

            IndexPersistence?.Dispose();

            SearchEngineType = IndexStorage.ReadSearchEngineType(Name, environment);

            switch (SearchEngineType)
            {
                case SearchEngineType.None:
                case SearchEngineType.Lucene:
                    SearchEngineType = SearchEngineType.Lucene;
                    IndexPersistence = new LuceneIndexPersistence(this, documentDatabase.IndexStore.IndexReadOperationFactory);

                    break;
                case SearchEngineType.Corax:
                    SearchEngineType = SearchEngineType.Corax;
                    IndexPersistence = new CoraxIndexPersistence(this, documentDatabase.IndexStore.IndexReadOperationFactory);
                    break;
                default:
                    throw new InvalidDataException($"Cannot read search engine type for {Name}. Please reset the index.");
            }

            IndexPersistence.Initialize(environment);

            IndexFieldsPersistence = new IndexFieldsPersistence(this);
            IndexFieldsPersistence.Initialize();
        }

        protected virtual void OnInitialization()
        {
            _numberOfDeployedNodes = GetNumberOfDeployedNodes();
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
                MaxNumberOfOutputsPerDocument = _indexStorage.ReadMaxNumberOfOutputsPerDocument(tx);
                ArchivedDataProcessingBehavior = _indexStorage.ReadArchivedDataProcessingBehavior(tx);
            }
        }

        public virtual void Start()
        {
            if (_disposeOnce.Disposed)
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

        private readonly ManualResetEventSlim _rollingEvent = new ManualResetEventSlim();

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
                    if (_disposeOnce.Disposed == false)
                    {
                        ReportUnexpectedIndexingError(ode);
                    }
                    // else we are been disposed of and we can ignore this error.
                }
                catch (Exception e)
                {
                    ReportUnexpectedIndexingError(e);
                }
            }, null, ThreadNames.ForIndex(IndexingThreadName, Name, _indexStorage.DocumentDatabase.Name));

            RollIfNeeded();
        }

        private int _numberOfDeployedNodes;

        public void RollIfNeeded()
        {
            if (_initialized == false)
                return;

            if (_indexingThread == null)
                return;

            RaiseNotificationIfNeeded();

            GetPendingAndReplaceStatus(out var pending, out var shouldReplace);

            if (shouldReplace)
            {
                if (ForceReplace.Raise() == false)
                    return; // need to wait for the replace to occur
            }

            if (shouldReplace || pending == false)
            {
                _rollingEvent.Set();

                CompleteIfRollingSideBySideRemoved();
            }
        }

        private void CompleteIfRollingSideBySideRemoved()
        {
            // this for the case when we removed a rolling side-by-side, we need the original node to complete the deployment.
            // if the original has no work to do, we need to force it.

            if (_rollingCompletionTask?.IsCompletedSuccessfully == true)
                return;

            if (_rollingCompletionTask?.IsCompleted == false)
                return;

            if (GetNumberOfDeployedNodes() != -1 && IsStaleInternal() == false)
                _mre?.Set();
        }

        private int GetNumberOfDeployedNodes()
        {
            return DocumentDatabase.IndexStore.GetRollingProgress(NormalizedName)?.ActiveDeployments.Values.Count(x => x.State == RollingIndexState.Done) ?? -1;
        }

        private void RaiseNotificationIfNeeded()
        {
            if (IsRolling && DocumentDatabase.IndexStore.HasReplacement(Name) == false)
            {
                var numberOfDeployedNodes = _numberOfDeployedNodes;
                var currentDeployedNodes = GetNumberOfDeployedNodes();

                if (Interlocked.CompareExchange(ref _numberOfDeployedNodes, currentDeployedNodes, numberOfDeployedNodes) != numberOfDeployedNodes)
                    return;

                // only one can replace it
                if (numberOfDeployedNodes != currentDeployedNodes)
                {
                    DocumentDatabase.Changes.RaiseNotifications(
                        new IndexChange { Name = Name, Type = IndexChangeTypes.RollingIndexChanged });
                }
            }
        }

        public bool IsPending
        {
            get
            {
                GetPendingAndReplaceStatus(out var pending, out _);
                return pending;
            }
        }

        private void GetPendingAndReplaceStatus(out bool pending, out bool replace)
        {
            pending = false;
            replace = false;

            if (IsRolling == false)
                return;

            if (State == IndexState.Normal)
            {
                pending = DocumentDatabase.IndexStore.ShouldSkipThisNodeWhenRolling(this, out _lastPendingStatus, out replace);
            }
        }

        private Task _rollingCompletionTask;

        private void MaybeFinishRollingDeployment()
        {
            // we remember that task so we wouldn't flood the cluster with commands
            if (_rollingCompletionTask != null)
                return;

            if (DocumentDatabase.IndexStore.MaybeFinishRollingDeployment(Definition.Name, Definition.ClusterState?.LastRollingDeploymentIndex) == false)
                return;

            if (IsStaleInternal())
                return;

            var nodeTag = DocumentDatabase.ServerStore.NodeTag;

            try
            {
                DocumentDatabase.IndexStore.ForTestingPurposes?.BeforeRollingIndexFinished?.Invoke(this);

                // We may send the command multiple times so we need a new Id every time.
                var command = new PutRollingIndexCommand(DocumentDatabase.Name, Definition.Name, nodeTag, DocumentDatabase.Time.GetUtcNow(), RaftIdGenerator.NewId());
                _rollingCompletionTask = DocumentDatabase.ServerStore.SendToLeaderAsync(command).ContinueWith(async t =>
                {
                    try
                    {
                        var result = await t;
                        await DocumentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(result.Index, TimeSpan.FromSeconds(15));

                        DocumentDatabase.IndexStore.ForTestingPurposes?.OnRollingIndexFinished?.Invoke(this);
                    }
                    catch (Exception e)
                    {
                        _rollingCompletionTask = null;

                        // we need to retry
                        ScheduleIndexingRun();

                        if (_logger.IsOperationsEnabled)
                            _logger.Operations($"Failed to send {nameof(PutRollingIndexCommand)} after finished indexing '{Definition.Name}' in node {nodeTag}.", e);
                    }
                });

            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Failed to send {nameof(PutRollingIndexCommand)} after finished indexing '{Definition.Name}' in node {nodeTag}.", e);
            }
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
            if (_disposeOnce.Disposed)
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
                    _mre.Set(ignoreThrottling: true);
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

        public virtual void Update(IndexDefinitionBaseServerSide definition, IndexingConfiguration configuration)
        {
            Debug.Assert(Type.IsStatic());

            configuration.InitializeAnalyzers(DocumentDatabase.Name);
            InitializeMetrics(configuration);

            using (DrainRunningQueries())
            {
                var status = Status;
                if (status == IndexRunningStatus.Running)
                    Stop();

                _indexStorage.WriteDefinition(definition);

                bool startIndex = UpdateIndexState(definition);

                Definition = definition;
                Configuration = configuration;

                if (Configuration.ThrottlingTimeInterval?.AsTimeSpan != _mre.ThrottlingInterval)
                    _mre.Update(Configuration.ThrottlingTimeInterval?.AsTimeSpan);

                OnInitialization();

                _priorityChanged.Raise();

                if (status == IndexRunningStatus.Running || startIndex)
                    Start();
            }
        }

        internal bool UpdateIndexState(IndexDefinitionBaseServerSide definition, bool autoIndex = false)
        {
            var startIndex = false;
            if (definition.ClusterState?.LastStateIndex > (Definition.ClusterState?.LastStateIndex ?? -1))
            {
                switch (definition.State)
                {
                    case IndexState.Disabled:
                        Disable();
                        break;
                    case IndexState.Normal:
                        startIndex = true;
                        if (autoIndex)
                        {
                            Definition.ClusterState ??= new IndexDefinitionClusterState();
                            Definition.ClusterState.LastStateIndex = definition.ClusterState.LastStateIndex;
                        }
                        SetState(definition.State);
                        break;
                    case IndexState.Error:
                        SetState(definition.State); // Just in case we change to error manually ==> indexState == error and the index is paused
                        break;
                    case IndexState.Idle:
                        if (autoIndex)
                            SetState(definition.State);
                        break;
                }
            }

            return startIndex;
        }

        private DisposeOnce<SingleAttempt> _disposeOnce;

        public bool IsDisposed => _disposeOnce.Disposed;

        public void Dispose()
        {
            _disposeOnce.Dispose();
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

            return new DocumentIndexItem(document.Id, document.LowerId, document.Etag, document.LastModified, document.Data.Size, document, document.Flags);
        }

        protected virtual IndexItem GetTombstoneByEtag(QueryOperationContext queryContext, long etag)
        {
            var tombstone = DocumentDatabase.DocumentsStorage.GetTombstoneByEtag(queryContext.Documents, etag);
            if (tombstone == null)
                return default;

            return new DocumentIndexItem(tombstone.LowerId, tombstone.LowerId, tombstone.Etag, tombstone.LastModified, 0, tombstone, tombstone.Flags);
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

            if (_shardedDocumentDatabase?.ShardingConfiguration.HasActiveMigrations() == true)
            {
                if (stalenessReasons == null)
                    return true;

                stalenessReasons.Add("There are active migrations of buckets between shards.");
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

        private NativeMemory.ThreadStats _indexingThreadStats;

        protected void ExecuteIndexing()
        {
            _priorityChanged.Raise();
            NativeMemory.EnsureRegistered();
            _indexingThreadStats = NativeMemory.CurrentThreadStats;

            using (CultureHelper.EnsureInvariantCulture())
            using (EnsureSingleIndexingThread())
            {
                // if we are starting indexing e.g. manually after failure
                // we need to reset errors to give it a chance
                ResetErrors();

                var storageEnvironment = _environment;
                if (storageEnvironment == null)
                    return; // can be null if we disposed immediately

                _mre.EnableThrottlingTimer();

                try
                {

                    storageEnvironment.OnLogsApplied += HandleLogsApplied;

                    SubscribeToChanges(DocumentDatabase);

                    if (IsRolling)
                    {
                        DocumentDatabase.IndexStore.ForTestingPurposes?.BeforeRollingIndexStart?.Invoke(this);

                        while (true)
                        {

                            WaitHandle.WaitAny(new[] { _mre.WaitHandle, _rollingEvent.WaitHandle, _indexingProcessCancellationTokenSource.Token.WaitHandle });
                            _indexingProcessCancellationTokenSource.Token.ThrowIfCancellationRequested();

                            if (_indexDisabled)
                                return;

                            var replaceStatus = ReplaceIfNeeded(batchCompleted: false, didWork: false);

                            if (replaceStatus == ReplaceStatus.Succeeded)
                                return;

                            if (replaceStatus == ReplaceStatus.NotNeeded)
                            {
                                if (_mre.IsSet == false)
                                    break;
                            }

                            _mre.Reset();

                            _indexingProcessCancellationTokenSource.Token.WaitHandle.WaitOne(TimeSpan.FromMilliseconds(500)); // replace will be re-tried
                        }

                        DocumentDatabase.IndexStore.ForTestingPurposes?.OnRollingIndexStart?.Invoke(this);
                    }

                    if (IndexPersistence.RequireOnBeforeExecuteIndexing())
                    {
                        var onBeforeExecutionStats = _lastStats = new IndexingStatsAggregator(DocumentDatabase.IndexStore.Identities.GetNextIndexingStatsId(), _lastStats);
                        try
                        {
                            AddIndexingPerformance(onBeforeExecutionStats);
                            IndexPersistence.OnBeforeExecuteIndexing(onBeforeExecutionStats, _indexingProcessCancellationTokenSource.Token);
                        }
                        catch (OperationCanceledException)
                        {
                            //this will be handled below.
                        }
                        finally
                        {
                            onBeforeExecutionStats.Complete();
                            NotifyAboutCompletedBatch(false);
                        }
                    }
                    
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

                        var batchCompleted = false;

                        bool didWork = false;

                        var stats = _lastStats = new IndexingStatsAggregator(DocumentDatabase.IndexStore.Identities.GetNextIndexingStatsId(), _lastStats);

                        try
                        {
                            if (_logger.IsInfoEnabled)
                                _logger.Info($"Starting indexing for '{Name}'.");

                            LastIndexingTime = stats.StartTime;

                            AddIndexingPerformance(stats);

                            using (var scope = stats.CreateScope())
                            {
                                try
                                {
                                    _indexingProcessCancellationTokenSource.Token.ThrowIfCancellationRequested();

                                    if (DocumentDatabase.ServerStore.ServerWideConcurrentlyRunningIndexesLock != null)
                                    {
                                        if (DocumentDatabase.ServerStore.ServerWideConcurrentlyRunningIndexesLock.TryAcquire(TimeSpan.Zero,
                                            _indexingProcessCancellationTokenSource.Token) == false)
                                        {
                                            using (scope.For(IndexingOperation.Wait.AcquireConcurrentlyRunningIndexesLock))
                                            {
                                                DocumentDatabase.ServerStore.ServerWideConcurrentlyRunningIndexesLock.Acquire(_indexingProcessCancellationTokenSource
                                                    .Token);
                                            }
                                        }
                                    }

                                    try
                                    {
                                        _doingIndexingWork.Wait(_indexingProcessCancellationTokenSource.Token);

                                        try
                                        {

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
                                            _mre.Set(ignoreThrottling: true);
                                            throw;
                                        }
                                        finally
                                        {
                                            _doingIndexingWork.Release();

                                            if (_batchStopped)
                                            {
                                                _batchStopped = false;
                                                DocumentDatabase.IndexStore.StoppedConcurrentIndexBatches.Release();
                                            }

                                            _threadAllocations.CurrentlyAllocatedForProcessing = 0;
                                            _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;

                                            TimeSpentIndexing.Stop();
                                        }
                                    }
                                    finally
                                    {
                                        DocumentDatabase.ServerStore.ServerWideConcurrentlyRunningIndexesLock?.Release();
                                    }

                                    _indexingBatchCompleted.SetAndResetAtomically();

                                    if (didWork)
                                    {
                                        ResetErrors();
                                        _hadRealIndexingWorkToDo.Raise();
                                    }
                                    else
                                    {
                                        MaybeFinishRollingDeployment();
                                    }

                                    if (_logger.IsInfoEnabled)
                                        _logger.Info($"Finished indexing for '{Name}'.'");
                                }
                                catch (TimeoutException te)
                                {
                                    if (_logger.IsOperationsEnabled)
                                        _logger.Operations($"Failed to open write transaction, indexing will be retried", te);
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
                                    Debug.Assert(_indexingProcessCancellationTokenSource.IsCancellationRequested,
                                        $"Got {nameof(OperationCanceledException)} while the index was not canceled");

                                    // We are here only in the case of indexing process cancellation.
                                    scope.RecordBatchCompletedReason(IndexingWorkType.Map, "Operation canceled.");
                                    return;
                                }
                                catch (Exception e) when (e.IsOutOfMemory())
                                {
                                    HandleOutOfMemoryException(scope, storageEnvironment, e);
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

                                if (ReplaceIfNeeded(batchCompleted, didWork) == ReplaceStatus.Succeeded)
                                    return;
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

                            var throttlingInterval = _mre.ThrottlingInterval;

                            if (throttlingInterval != null && throttlingInterval.Value.TotalMilliseconds > timeToWaitForMemoryCleanup)
                            {
                                // when we're throttling the index then let's wait twice as much as the throttling interval to ensure
                                // that we cleanup the memory only when we really don't have any work

                                timeToWaitForMemoryCleanup = (int)(2 * throttlingInterval.Value.TotalMilliseconds);
                            }

                            var forceMemoryCleanup = false;

                            if (_lowMemoryFlag.IsRaised())
                            {
                                ReduceMemoryUsage(storageEnvironment, IndexCleanup.Basic | IndexCleanup.Writers);
                            }
                            else if (_allocationCleanupNeeded > 0)
                            {
                                if (_mre.IsSetScheduled == false)
                                {
                                    // if there is nothing to do and no work has been scheduled already (when running in throttled mode) then
                                    // immediately cleanup everything 
                                    timeToWaitForMemoryCleanup = 0;
                                }

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
                                        var llt = tx.LowLevelTransaction;
                                        var waj = _environment.Options.Encryption.WriteAheadJournal;
                                        waj.ZeroCompressionBuffer(ref llt.PagerTransactionState);
                                    }
                                }

                                // allocation cleanup has been requested multiple times or
                                // there is no work to be done, and hasn't been for a while,
                                // so this is a good time to release resources we won't need
                                // anytime soon

                                var mode = IndexCleanup.Basic;
                                if (NoQueryRecently())
                                    mode |= IndexCleanup.Readers;

                                ReduceMemoryUsage(storageEnvironment, mode);

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
                        catch (Exception e) when (e.IsOutOfMemory())
                        {
                            HandleOutOfMemoryException(null, storageEnvironment, e);
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
                    _forTestingPurposes?.ActionToCallInFinallyOfExecuteIndexing?.Invoke();

                    _inMemoryIndexProgress.Clear();

                    if (storageEnvironment != null)
                        storageEnvironment.OnLogsApplied -= HandleLogsApplied;

                    UnsubscribeFromChanges(DocumentDatabase);

                    _mre.DisableThrottlingTimer();
                }
            }
        }

        public enum ReplaceStatus
        {
            NotNeeded,
            Failed,
            Succeeded
        }

        public ReplaceStatus ReplaceIfNeeded(bool batchCompleted, bool didWork)
        {
            try
            {
                if (ForceReplace.Lower() || ShouldReplace())
                {
                    if (Definition.Name.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix) == false)
                        return ReplaceStatus.NotNeeded; // already replaced

                    var originalName = Name.Replace(Constants.Documents.Indexing.SideBySideIndexNamePrefix, string.Empty, StringComparison.OrdinalIgnoreCase);
                    _isReplacing = true;

                    if (batchCompleted)
                    {
                        Size totalSizeOfJournals = Size.Zero;
                        foreach (var journalSize in _environment.Journal.Files.Select(i => i.JournalSize))
                            totalSizeOfJournals += journalSize;


                        if (totalSizeOfJournals >= Configuration.MinimumTotalSizeOfJournalsToRunFlushAndSyncWhenReplacingSideBySideIndex)
                            FlushAndSync(_environment, (int)Configuration.MaxTimeToWaitAfterFlushAndSyncWhenReplacingSideBySideIndex.AsTimeSpan.TotalMilliseconds, tryCleanupRecycledJournals: true);

                        // this side-by-side index will be replaced in a second, notify about indexing success
                        // so we know that indexing batch is no longer in progress
                        NotifyAboutCompletedBatch(didWork);
                    }

                    try
                    {
                        try
                        {
                            DocumentDatabase.IndexStore.ReplaceIndexes(originalName, Definition.Name, _indexingProcessCancellationTokenSource.Token);
                            StartIndexingThread();
                            return ReplaceStatus.Succeeded;
                        }
                        catch (OperationCanceledException)
                        {
                            // this can fail if the indexes lock is currently held, so we'll retry
                            // however, we might be requested to shutdown, so we want to skip replacing
                            // in this case, worst case scenario we'll handle this in the next batch
                            return ReplaceStatus.Failed;
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
                _mre.Set(ignoreThrottling: true); // try again

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Could not replace index '{Name}'.", e);

                return ReplaceStatus.Failed;
            }

            return ReplaceStatus.NotNeeded;
        }

        private void PauseIfCpuCreditsBalanceIsTooLow()
        {
            int numberOfTimesSlept = 0;
            bool indexAlreadyAddedToWarning = false;

            while (DocumentDatabase.ServerStore.Server.CpuCreditsBalance.BackgroundTasksAlertRaised.IsRaised() && _indexDisabled == false)
            {
                _indexingProcessCancellationTokenSource.Token.ThrowIfCancellationRequested();

                // give us a bit more than a measuring cycle to gain more CPU credits
                Thread.Sleep(1250);

                if (indexAlreadyAddedToWarning == false && numberOfTimesSlept++ > 5)
                {
                    DocumentDatabase.NotificationCenter.Indexing.AddIndexNameToCpuCreditsExhaustionWarning(Name);
                    DocumentDatabase.NotificationCenter.Indexing.ProcessCpuCreditsExhaustion();

                    indexAlreadyAddedToWarning = true;
                }
            }
            
            DocumentDatabase.NotificationCenter.Indexing.RemoveIndexNameFromCpuCreditsExhaustionWarning(Name);
        }

        private void NotifyAboutCompletedBatch(bool didWork)
        {
            DocumentDatabase.Changes.RaiseNotifications(new IndexChange { Name = Name, Type = IndexChangeTypes.BatchCompleted });

            if (didWork)
            {
                _didWork = true;
                _firstBatchTimeout = null;
            }
            
            TestRun?.BatchCompleted.Set();

            var batchCompletedAction = DocumentDatabase.IndexStore.IndexBatchCompleted;
            batchCompletedAction?.Invoke((Name, didWork));
        }

        public void Cleanup(IndexCleanup mode)
        {
            if (_initialized == false)
                return;

            ReduceMemoryUsage(_environment, mode);
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

            var currentPriority = ThreadHelper.GetThreadPriority();
            if (currentPriority == newPriority)
                return;

            ThreadHelper.TrySetThreadPriority(newPriority, IndexingThreadName, _logger);
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

        private void ReduceMemoryUsage(StorageEnvironment environment, IndexCleanup mode)
        {
            if (_doingIndexingWork.Wait(0) == false)
                return;

            try
            {
                var indexingStats = _indexingThreadStats ?? NativeMemory.CurrentThreadStats;

                var allocatedBeforeCleanup = indexingStats.TotalAllocated;
                if (allocatedBeforeCleanup == _allocatedAfterPreviousCleanup)
                    return;

                DocumentDatabase.DocumentsStorage.ContextPool.Clean();
                _contextPool.Clean();

                if (CalledUnderIndexingThread)
                {
                    ByteStringMemoryCache.CleanForCurrentThread();
                }

                IndexPersistence.Clean(mode);
                environment?.Cleanup();

                _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;

                _allocatedAfterPreviousCleanup = indexingStats.TotalAllocated;
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info($"Reduced the memory usage of index '{Name}' (mode:{mode}). " +
                                 $"Before: {new Size(allocatedBeforeCleanup, SizeUnit.Bytes)}, " +
                                 $"after: {new Size(_allocatedAfterPreviousCleanup, SizeUnit.Bytes)}");
                }
            }
            finally
            {
                _doingIndexingWork.Release();
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

                FlushAndSync(storageEnvironment, timeToWaitInMilliseconds, true);
                return;
            }

            if (_logger.IsOperationsEnabled)
                _logger.Operations($"Disk full error occurred for '{Name}'. Setting index to errored state", dfe);

            if (State == IndexState.Error)
                return;

            storageEnvironment.Options.TryCleanupRecycledJournals();
            SetErrorState($"State was changed due to excessive number of disk full errors ({diskFullErrors}).");
        }

        private void FlushAndSync(StorageEnvironment storageEnvironment, int timeToWaitInMilliseconds, bool tryCleanupRecycledJournals)
        {
            try
            {
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
            }
            catch (OperationCanceledException)
            {
                // index was deleted or database was shutdown
                return;
            }
            catch (AggregateException ae) when (ae.ExtractSingleInnerException() is OperationCanceledException)
            {
                // index was deleted or database was shutdown
                return;
            }

            storageEnvironment.Cleanup(tryCleanupRecycledJournals);
        }

        private void SetErrorState(string reason)
        {
            _errorStateReason = reason;
            SetState(IndexState.Error, ignoreWriteError: true);
        }

        private void HandleOutOfMemoryException(IndexingStatsScope scope, StorageEnvironment storageEnvironment, Exception exception)
        {
            try
            {
                if (exception.IsPageFileTooSmall())
                {
                    // throw a better exception
                    exception = new OutOfMemoryException("The paging file is too small for this operation to complete, consider increasing the size of the page file", exception);
                }

                scope?.AddMemoryError(exception);
                var outOfMemoryErrors = Interlocked.Add(ref _lowMemoryPressure, LowMemoryPressure);
                _lowMemoryFlag.Raise();

                if (storageEnvironment.ScratchBufferPool.NumberOfScratchBuffers > 1)
                {
                    // we'll try to clear the scratch buffers to free up some memory
                    var timeToWaitInMilliseconds = (int)Math.Min(Math.Pow(2, outOfMemoryErrors), 30) * 1000;

                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"After out of memory error in index : '{Name}', " +
                                           $"going to try flushing and syncing the environment to cleanup the scratch buffers. " +
                                           $"Will wait for flush for: {timeToWaitInMilliseconds}ms", exception);

                    FlushAndSync(storageEnvironment, timeToWaitInMilliseconds, false);
                }

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
                    _mre.Set(ignoreThrottling: true);
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

        public virtual HandleReferencesBase.InMemoryReferencesInfo GetInMemoryReferencesState(string collection, bool isCompareExchange)
        {
            return HandleReferencesBase.InMemoryReferencesInfo.Default;
        }

        public void InitializeTestRun(DocumentsOperationContext context, int docsToProcessPerCollection, int numberOfCollections)
        {
            TestRun = new TestIndexRun(context, docsToProcessPerCollection, numberOfCollections);
        }

        public bool DoIndexingWork(IndexingStatsScope stats, CancellationToken cancellationToken)
        {
            _threadAllocations = NativeMemory.CurrentThreadStats;
            _initialManagedAllocations = new Size(GC.GetAllocatedBytesForCurrentThread(), SizeUnit.Bytes);

            bool mightBeMore = false;

            using (DocumentDatabase.PreventFromUnloadingByIdleOperations())
            using (CultureHelper.EnsureInvariantCulture())
            using (var context = QueryOperationContext.Allocate(DocumentDatabase, this))
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
            {
                indexContext.PersistentContext.LongLivedTransactions = true;
                context.SetLongLivedTransactions(true);

                using (var tx = indexContext.OpenWriteTransaction())
                using (CurrentIndexingScope.Current = CreateIndexingScope(indexContext, context))
                {
                    var writeOperation = new Lazy<IndexWriteOperationBase>(() =>
                    {
                        var writer = IndexPersistence.OpenIndexWriter(indexContext.Transaction.InnerTransaction, indexContext);

                        if (IsTestRun)
                            writer = TestRun.CreateIndexWriteOperationWrapper(writer, this);

                        return writer;
                    });
                    try
                    {
                        long? entriesCount = null;

                        using (InitializeIndexingWork(indexContext))
                        {
                            foreach (var work in _indexWorkers)
                            {
                                using (var scope = stats.For(work.Name))
                                {
                                    var result = work.Execute(context, indexContext, writeOperation, scope, cancellationToken);
                                    mightBeMore |= result.MoreWorkFound;

                                    if (mightBeMore)
                                    {
                                        var ignoreThrottling = result.BatchContinuationResult == CanContinueBatchResult.False; // if batch was stopped because of memory limit or batch size then let it continue immediately

                                        _mre.Set(ignoreThrottling);
                                    }
                                }
                            }

                            var current = new Size(GC.GetAllocatedBytesForCurrentThread(), SizeUnit.Bytes);
                            stats.SetAllocatedManagedBytes((current - _initialManagedAllocations).GetValue(SizeUnit.Bytes));

                            if (writeOperation.IsValueCreated)
                            {
                                using (var indexWriteOperation = writeOperation.Value)
                                {
                                    indexWriteOperation.Commit(stats);

                                    entriesCount = writeOperation.Value.EntriesCount();
                                }

                                UpdateThreadAllocations(indexContext, null, null, IndexingWorkType.None);
                            }

                            IndexFieldsPersistence.Persist(indexContext);
                            HandleReferences(tx);

                            HandleMismatchedReferences();
                            HandleComplexFieldsAlert();
                        }

                        using (stats.For(IndexingOperation.Storage.Commit))
                        {
                            tx.InnerTransaction.LowLevelTransaction.RetrieveCommitStats(out CommitStats commitStats);

                            tx.InnerTransaction.LowLevelTransaction.LastChanceToReadFromWriteTransactionBeforeCommit += llt =>
                            {
                                llt.ImmutableExternalState = IndexPersistence.BuildStreamCacheAfterTx(llt.Transaction);
                            };

                            tx.InnerTransaction.LowLevelTransaction.AfterCommitWhenNewTransactionsPrevented += llt =>
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

                                if (entriesCount != null)
                                    stats.RecordEntriesCountAfterTxCommit(entriesCount.Value);
                            };

                            tx.InnerTransaction.LowLevelTransaction.OnDispose += _ => IndexPersistence.CleanWritersIfNeeded();

                            tx.Commit();
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

        private void HandleReferences(RavenTransaction tx)
        {
            _indexStorage.WriteReferences(CurrentIndexingScope.Current, tx);

            if (_updateReferenceLoadWarning == false)
                return;

            DocumentDatabase.NotificationCenter.Indexing.AddWarning(Name, _referenceLoadWarning);

            _updateReferenceLoadWarning = false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetFieldIsIndexedAsJsonViaCoraxAutoIndex(IndexField field)
        {
            var fieldName = field.OriginalName ?? field.Name;
            if (Type.IsAuto() == false || _fieldsReportedAsComplex.Contains(fieldName))
                return;
            
            DocumentDatabase.NotificationCenter.Indexing.AddComplexFieldWarning(Name, fieldName);
            _fieldsReportedAsComplex.Add(fieldName);
            _newComplexFieldsToReport = true;
        }
        
        private void HandleComplexFieldsAlert()
        {
            if (_newComplexFieldsToReport)
            {
                DocumentDatabase.NotificationCenter.Indexing.ProcessComplexFields();
                _newComplexFieldsToReport = false;
            }
        }
        
        private void HandleMismatchedReferences()
        {
            if (CurrentIndexingScope.Current.MismatchedReferencesWarningHandler == null || CurrentIndexingScope.Current.MismatchedReferencesWarningHandler.IsEmpty)
                return;

            MismatchedReferencesLoadWarning warning = new(Name, CurrentIndexingScope.Current.MismatchedReferencesWarningHandler.GetLoadFailures());

            DocumentDatabase.NotificationCenter.Indexing.AddWarning(warning);

            CurrentIndexingScope.Current.MismatchedReferencesWarningHandler = null;
        }

        private void DisposeIndexWriterOnError(Lazy<IndexWriteOperationBase> writeOperation)
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

        public abstract void HandleDelete(Tombstone tombstone, string collection, Lazy<IndexWriteOperationBase> writer,
            TransactionOperationContext indexContext, IndexingStatsScope stats);

        public abstract int HandleMap(IndexItem indexItem, IEnumerable mapResults, Lazy<IndexWriteOperationBase> writer,
            TransactionOperationContext indexContext, IndexingStatsScope stats);

        public virtual bool MustDeleteArchivedDocument(IndexItem indexItem)
        {
            return true;
        }

        public virtual void DeleteArchived(IndexItem indexItem, string collection, Lazy<IndexWriteOperationBase> writer, TransactionOperationContext indexContext, IndexingStatsScope stats, LazyStringValue lowerId)
        {
            if (MustDeleteArchivedDocument(indexItem))
                HandleDelete(new Tombstone { LowerId = lowerId}, collection, writer, indexContext, stats);
        }
        
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
            {
                documentDatabase.Changes.OnDocumentChange += HandleDocumentChange;

                if (Definition.HasCompareExchange)
                    documentDatabase.ServerStore.Cluster.Changes.OnCompareExchangeChange += HandleCompareExchangeChange;
            }
        }

        protected virtual void UnsubscribeFromChanges(DocumentDatabase documentDatabase)
        {
            if (documentDatabase != null)
            {
                documentDatabase.Changes.OnDocumentChange -= HandleDocumentChange;

                if (Definition.HasCompareExchange)
                    documentDatabase.ServerStore.Cluster.Changes.OnCompareExchangeChange -= HandleCompareExchangeChange;
            }
        }

        protected virtual void HandleDocumentChange(DocumentChange change)
        {
            if (HandleAllDocs == false && Collections.Contains(change.CollectionName) == false)
                return;
            _mre.Set();
        }

        protected virtual void HandleCompareExchangeChange(CompareExchangeChange change)
        {
            if (DocumentDatabase.CompareExchangeStorage.ShouldHandleChange(change) == false)
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

                try
                {
                    return _indexStorage.ReadErrorsCount();
                }
                catch (Exception e)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations("Failed to get index error count", e);

                    return 1;
                }
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
                    _mre.Set(ignoreThrottling: true);
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
                    _mre.Set(ignoreThrottling: true);
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
                Cleanup(IndexCleanup.All);
            }
        }

        public void Rename(string name)
        {
            _indexStorage.Rename(name);
        }

        internal virtual IndexProgress GetProgress(QueryOperationContext queryContext, Stopwatch overallDuration, bool? isStale = null)
        {
            using (CurrentlyInUse(out var valid))
            {
                queryContext.AssertOpenedTransactions();

                var disposed = DocumentDatabase.DatabaseShutdown.IsCancellationRequested || _disposeOnce.Disposed;
                if (valid == false || disposed)
                {
                    var progress = new IndexProgress
                    {
                        Name = Name,
                        Type = Type,
                        SourceType = SourceType,
                        IndexRunningStatus = Status,
                        Collections = new Dictionary<string, IndexProgress.CollectionStats>(StringComparer.OrdinalIgnoreCase),
                    };

                    if (disposed)
                        return progress;

                    UpdateIndexProgress(queryContext, progress, null, overallDuration);
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
                        Collections = new Dictionary<string, IndexProgress.CollectionStats>(StringComparer.OrdinalIgnoreCase),
                    };

                    var stats = _indexStorage.ReadStats(tx);

                    UpdateIndexProgress(queryContext, progress, stats, overallDuration);

                    return progress;
                }
            }
        }

        private void UpdateIndexProgress(QueryOperationContext queryContext, IndexProgress progress, IndexStats stats, Stopwatch overallDuration)
        {
            if (DeployedOnAllNodes == false)
            {
                progress.IndexRollingStatus = DocumentDatabase.IndexStore.GetRollingProgress(NormalizedName);
            }

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

                UpdateProgressStats(queryContext, progressStats, collection, overallDuration);
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

                            UpdateProgressStats(queryContext, progressStats, value.Name, overallDuration);
                        }
                    }
                }
            }
        }

        internal virtual void UpdateProgressStats(QueryOperationContext queryContext, IndexProgress.CollectionStats progressStats, string collectionName,
            Stopwatch overallDuration)
        {
            progressStats.NumberOfItemsToProcess +=
                DocumentDatabase.DocumentsStorage.GetNumberOfDocumentsToProcess(
                    queryContext.Documents, collectionName, progressStats.LastProcessedItemEtag, out var totalCount, overallDuration);
            progressStats.TotalNumberOfItems += totalCount;

            progressStats.NumberOfTombstonesToProcess +=
                DocumentDatabase.DocumentsStorage.GetNumberOfTombstonesToProcess(
                    queryContext.Documents, collectionName, progressStats.LastProcessedTombstoneEtag, out totalCount, overallDuration);
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

        internal IndexProgress.CollectionStats GetStats(string collection)
        {
            return _inMemoryIndexProgress.GetOrAdd(collection, _ => new IndexProgress.CollectionStats());
        }

        internal IndexProgress.CollectionStats GetReferencesStats(string collection)
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

        public virtual IndexStats GetStats(bool calculateLag = false, bool calculateStaleness = false,
            bool calculateMemoryStats = false, bool calculateLastBatchStats = false,
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
                        SearchEngineType = SearchEngineType,
                        SourceType = SourceType,
                        LockMode = Definition?.LockMode ?? IndexLockMode.Unlock,
                        ArchivedDataProcessingBehavior = Definition?.ArchivedDataProcessingBehavior ?? GetDefaultArchivedDataProcessingBehavior(),
                        Priority = Definition?.Priority ?? IndexPriority.Normal,
                        State = State,
                        Status = Status,
                        Collections = Collections.ToDictionary(x => x, _ => new IndexStats.CollectionStats()),
                        ReferencedCollections = GetReferencedCollectionNames()
                    };
                }

                if (_contextPool == null)
                    throw new ObjectDisposedException("Index " + Name);

                using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var stats = _indexStorage.ReadStats(tx);

                    stats.Name = Name;
                    stats.SourceType = SourceType;
                    stats.SearchEngineType = SearchEngineType;
                    stats.Type = Type;
                    stats.LockMode = Definition.LockMode;
                    stats.ArchivedDataProcessingBehavior = Definition.ArchivedDataProcessingBehavior;
                    stats.Priority = Definition.Priority;
                    stats.State = State;
                    stats.Status = Status;

                    stats.MappedPerSecondRate = MapsPerSec?.OneMinuteRate ?? 0;
                    stats.ReducedPerSecondRate = ReducesPerSec?.OneMinuteRate ?? 0;

                    if (calculateLastBatchStats)
                        stats.LastBatchStats = _lastStats?.ToIndexingPerformanceLiveStats();

                    stats.LastQueryingTime = _lastQueryingTime;

                    stats.ReferencedCollections = GetReferencedCollectionNames();


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

        internal HashSet<string> GetReferencedCollectionNames()
        {
            // multiple maps can reference the same collections, we wants to return distinct names only
            return GetReferencedCollections()?
                .SelectMany(p => p.Value?.Select(z => z.Name))
                .ToHashSet();
        }

        private IndexStats.MemoryStats GetMemoryStats()
        {
            var stats = new IndexStats.MemoryStats();

            var name = IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(Name);

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

        public bool NoQueryRecently()
        {
            var last = _lastQueryingTime;
            return last.HasValue == false ||
                   DocumentDatabase.Time.GetUtcNow() - last.Value > Configuration.TimeSinceLastQueryAfterWhichDeepCleanupCanBeExecuted.AsTimeSpan;
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
            var result = new StreamDocumentQueryResult(response, writer, queryContext.Documents, Definition.ClusterState.LastIndex, token);
            await QueryInternal(result, query, queryContext, pulseDocsReadingTransaction: true, token);
            result.Flush();

            DocumentDatabase.QueryMetadataCache.MaybeAddToCache(query.Metadata, Name);
        }

        public virtual async Task StreamIndexEntriesQuery(HttpResponse response, IStreamQueryResultWriter<BlittableJsonReaderObject> writer,
            IndexQueryServerSide query, QueryOperationContext queryContext, bool ignoreLimit, OperationCancelToken token)
        {
            var result = new StreamDocumentIndexEntriesQueryResult(response, writer, Definition.ClusterState.LastIndex, token);
            await IndexEntriesQueryInternal(result, query, queryContext, ignoreLimit, token);
            result.Flush();

            DocumentDatabase.QueryMetadataCache.MaybeAddToCache(query.Metadata, Name);
        }

        public virtual async Task<DocumentIdQueryResult> IdQuery(
            IndexQueryServerSide query,
            QueryOperationContext queryContext,
            DeterminateProgress progress,
            Action<DeterminateProgress> onProgress,
            OperationCancelToken token)
        {
            var result = new DocumentIdQueryResult(progress, onProgress, Definition.ClusterState.LastIndex, token);
            await QueryInternal(result, query, queryContext, pulseDocsReadingTransaction: false, token: token);
            return result;
        }

        public virtual async Task<DocumentQueryResult> Query(
            IndexQueryServerSide query,
            QueryOperationContext queryContext,
            OperationCancelToken token)
        {
            var result = new DocumentQueryResult(Definition.ClusterState.LastIndex);
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

            QueryRunner.AssertValidQuery(query, resultToFill);

            using (var marker = MarkQueryAsRunning(query))
            {
                var queryDuration = Stopwatch.StartNew();
                AsyncWaitForIndexing wait = null;
                (long? DocEtag, long? ReferenceEtag, long? CompareExchangeReferenceEtag)? cutoffEtag = null;

                var stalenessScope = query.Timings?.For(nameof(QueryTimingsScope.Names.Staleness), start: false);

                while (true)
                {
                    AssertIndexState();
                    await marker.HoldLockAsync();

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

                        using (var reader = IndexPersistence.OpenIndexReader(indexTx.InnerTransaction, query))
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

                                Reference<long> totalResults = new();
                                Reference<long> skippedResults = new();
                                Reference<long> scannedResults = new();
                                IncludeCountersCommand includeCountersCommand = null;
                                IncludeTimeSeriesCommand includeTimeSeriesCommand = null;
                                IncludeRevisionsCommand includeRevisionsCommand = new(DocumentDatabase, queryContext.Documents, query.Metadata.RevisionIncludes);

                                var fieldsToFetch = new FieldsToFetch(query, Definition, Type);

                                var includeDocumentsCommand = new IncludeDocumentsCommand(
                                    DocumentDatabase.DocumentsStorage, queryContext.Documents,
                                    query.Metadata.Includes,
                                    fieldsToFetch.IsProjection);

                                if (query.Metadata.RevisionIncludes != null)
                                {
                                    includeRevisionsCommand = new IncludeRevisionsCommand(
                                        DocumentDatabase,
                                        queryContext.Documents,
                                        query.Metadata.RevisionIncludes);
                                }

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

                                var retriever = GetQueryResultRetriever(query, queryScope, queryContext.Documents, SearchEngineType, fieldsToFetch, includeDocumentsCommand, includeCompareExchangeValuesCommand, includeRevisionsCommand);

                                IEnumerable<IndexReadOperationBase.QueryResult> documents;

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
                                        scannedResults,
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
                                        scannedResults,
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

                                        enumerator = new PulsedTransactionEnumerator<IndexReadOperationBase.QueryResult, QueryResultsIterationState>(queryContext.Documents,
                                            state => originalEnumerator,
                                            new QueryResultsIterationState(queryContext.Documents, DocumentDatabase.Configuration.Databases.PulseReadTransactionLimit));
                                    }

                                    using (enumerator)
                                    {
                                        while (enumerator.MoveNext())
                                        {
                                            var document = enumerator.Current;
                                            //Streaming:
                                            UpdateQueryStatistics();
                                            
                                            await resultToFill.AddResultAsync(document.Result, token.Token);

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

                                            includeRevisionsCommand?.Fill(document.Result);
                                        }
                                        
                                        // Corax: we have to update the statistics again (after all) due to take parameter in OrderBy clauses.
                                        UpdateQueryStatistics();
                                        void UpdateQueryStatistics()
                                        {
                                            resultToFill.TotalResults = totalResults.Value;

                                            if (query.Offset != null || query.Limit != null)
                                            {
                                                resultToFill.CappedMaxResults = Math.Max(0, Math.Min(
                                                    query.Limit ?? long.MaxValue,
                                                    totalResults.Value - (query.Offset ?? 0)
                                                ));
                                            }
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    if (resultToFill.SupportsExceptionHandling == false)
                                        throw;

                                    await resultToFill.HandleExceptionAsync(e, token.Token);
                                }

                                using (fillScope?.Start())
                                {
                                    includeDocumentsCommand.Fill(resultToFill.Includes, query.ReturnOptions?.MissingIncludeAsNull ?? false);
                                    includeCompareExchangeValuesCommand?.Materialize(maxAllowedAtomicGuardIndex: null);
                                }

                                if (includeCountersCommand != null)
                                    resultToFill.AddCounterIncludes(includeCountersCommand);

                                if (includeTimeSeriesCommand != null)
                                    resultToFill.AddTimeSeriesIncludes(includeTimeSeriesCommand);

                                if (includeCompareExchangeValuesCommand != null)
                                    resultToFill.AddCompareExchangeValueIncludes(includeCompareExchangeValuesCommand);

                                if (includeRevisionsCommand != null)
                                    resultToFill.AddRevisionIncludes(includeRevisionsCommand);

                                resultToFill.RegisterTimeSeriesFields(query, fieldsToFetch);
                                resultToFill.RegisterSpatialProperties(query);

                                resultToFill.TotalResults = Math.Max(totalResults.Value, resultToFill.Results.Count);
                                resultToFill.SkippedResults = skippedResults.Value;
                                resultToFill.ScannedResults = scannedResults.Value;
                                resultToFill.IncludedPaths = query.Metadata.Includes;
                                if (query.Metadata.FilterScript != null)
                                {
                                    resultToFill.ScannedResults = scannedResults.Value;
                                }
                            }
                        }

                        return;
                    }
                }
            }
        }

        private async Task IndexEntriesQueryInternal<TQueryResult>(
            TQueryResult resultToFill,
            IndexQueryServerSide query,
            QueryOperationContext queryContext,
            bool ignoreLimit,
            OperationCancelToken token)
          where TQueryResult : QueryResultServerSide<BlittableJsonReaderObject>
        {
            QueryInternalPreparation(query);

            QueryRunner.AssertValidQuery(query, resultToFill);

            using (var marker = MarkQueryAsRunning(query))
            {
                var queryDuration = Stopwatch.StartNew();
                AsyncWaitForIndexing wait = null;
                (long? DocEtag, long? ReferenceEtag, long? CompareExchangeReferenceEtag)? cutoffEtag = null;

                var stalenessScope = query.Timings?.For(nameof(QueryTimingsScope.Names.Staleness), start: false);

                while (true)
                {
                    AssertIndexState();
                    await marker.HoldLockAsync();
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
                            var totalResults = new Reference<long>();

                            foreach (var indexEntry in reader.IndexEntries(query, totalResults, queryContext.Documents, GetOrAddSpatialField, ignoreLimit, token.Token))
                            {
                                resultToFill.TotalResults = totalResults.Value;
                                await resultToFill.AddResultAsync(indexEntry, token.Token);
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

                var stalenessScope = query.Timings?.For(nameof(QueryTimingsScope.Names.Staleness), start: false);

                while (true)
                {
                    token.ThrowIfCancellationRequested();

                    AssertIndexState();
                    await marker.HoldLockAsync();

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
                                        wait = new AsyncWaitForIndexing(queryDuration,
                                            query.WaitForNonStaleResultsTimeout.Value, this);

                                    marker.ReleaseLock();

                                    await wait.WaitForIndexingAsync(frozenAwaiter).ConfigureAwait(false);
                                    continue;
                                }
                            }

                            FillFacetedQueryResult(result, isStale,
                                facetQuery.FacetsEtag, facetQuery.Query.Metadata,
                                queryContext, indexContext);

                            if (facetQuery.Query.Metadata.HasIncludeOrLoad == false)
                                queryContext.CloseTransaction();

                            using (var reader = IndexPersistence.OpenFacetedIndexReader(indexTx.InnerTransaction))
                            {
                                using (var queryScope = query.Timings?.For(nameof(QueryTimingsScope.Names.Query)))
                                {
                                    result.Results = reader.FacetedQuery(facetQuery, queryScope, queryContext.Documents, GetOrAddSpatialField, token.Token);

                                    if (facetQuery.Query.Metadata.HasIncludeOrLoad)
                                    {
                                        using (var includesScope = queryScope?.For(nameof(QueryTimingsScope.Names.Includes)))
                                        {
                                            var cmd = new IncludeDocumentsCommand(DocumentDatabase.DocumentsStorage, queryContext.Documents, query.Metadata.Includes,
                                                isProjection: true);

                                            using (includesScope?.For(nameof(QueryTimingsScope.Names.Gather)))
                                                cmd.Gather(result.Results);

                                            using (includesScope?.For(nameof(QueryTimingsScope.Names.Fill)))
                                                cmd.Fill(result.Includes, query.ReturnOptions?.MissingIncludeAsNull ?? false);
                                        }
                                    }

                                    result.TotalResults = result.Results.Count;

                                    return result;
                                }
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
                    result.Terms = reader.Terms(field, fromValue, pageSize, token.Token).ToList();
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
                    await marker.HoldLockAsync();

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
                                using (var reader = IndexPersistence.OpenSuggestionIndexReader(indexTx.InnerTransaction,
                                           suggestField.Name))
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
            bool ignoreLimit,
            OperationCancelToken token)
        {
            var result = new IndexEntriesQueryResult(Definition.ClusterState.LastIndex);
            await IndexEntriesQueryInternal(result, query, queryContext, ignoreLimit, token);
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

            if (_disposeOnce.Disposed)
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
                var lastCompareExchangeEtag = queryContext.Documents.DocumentDatabase.CompareExchangeStorage.GetLastCompareExchangeIndex(queryContext.Server);
                var lastCompareExchangeTombstoneEtag = queryContext.Documents.DocumentDatabase.CompareExchangeStorage.GetLastCompareExchangeTombstoneIndex(queryContext.Server);

                return Math.Max(lastCompareExchangeEtag, lastCompareExchangeTombstoneEtag);
            }
        }

        [DoesNotReturn]
        private void ThrowErrored()
        {
            throw new InvalidOperationException(
                $"Index '{Name}' is marked as errored. Please check index errors available at '/databases/{DocumentDatabase.Name}/indexes/errors?name={Name}'.");
        }

        [DoesNotReturn]
        private void ThrowMarkedAsError(string errorStateReason)
        {
            throw new InvalidOperationException($"Index '{Name}' is marked as errored. {errorStateReason}");
        }

        [DoesNotReturn]
        private void ThrowWasDisposed()
        {
            throw new ObjectDisposedException($"Index '{Name}' was already disposed.");
        }

        [DoesNotReturn]
        private void ThrowNotInitialized()
        {
            throw new InvalidOperationException($"Index '{Name}' was not initialized.");
        }

        [DoesNotReturn]
        private void ThrowCompactionInProgress()
        {
            throw new IndexCompactionInProgressException($"Index '{Name}' is currently being compacted.");
        }

        private void AssertQueryDoesNotContainFieldsThatAreNotIndexed(QueryMetadata metadata)
        {
            foreach (var field in metadata.IndexFieldNames)
            {
                AssertKnownField(field, metadata);
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

                    AssertKnownField(f, metadata);
                }
            }
        }

        private void AssertKnownField(string f, QueryMetadata queryMetadata)
        {
            // the catch all field name means that we have dynamic fields names

            if (Definition.HasDynamicFields || IndexPersistence.ContainsField(f))
                return;

            ThrowInvalidField(f, queryMetadata);
        }

        private static void ThrowInvalidField(string f, QueryMetadata queryMetadata)
        {
            throw new ArgumentException($"The field '{f}' is not indexed in '{queryMetadata.IndexName}', cannot query/sort on fields that are not indexed in query: " + queryMetadata.QueryText);
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

        private IDisposable EnsureSingleIndexingThread()
        {
            try
            {
                _executingIndexing.Wait(_indexingProcessCancellationTokenSource.Token);
            }
            catch (OperationCanceledException)
            {
                return null;
            }

            return new DisposableAction(() =>
            {
                DocumentDatabase.IndexStore.ForTestingPurposes?.BeforeIndexThreadExit?.Invoke(this);
                _executingIndexing.Release();
            });
        }

        internal static readonly TimeSpan DefaultWaitForNonStaleResultsTimeout = TimeSpan.FromSeconds(15); // this matches default timeout from client

        private ConcurrentLruRegexCache _regexCache;

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

        public unsafe long CalculateIndexEtagWithReferences(
            HandleReferences handleReferences, HandleReferences handleCompareExchangeReferences,
            QueryOperationContext queryContext, TransactionOperationContext indexContext, QueryMetadata query, bool isStale,
            HashSet<string> referencedCollections, AbstractStaticIndexBase compiled)
        {
            var minLength = MinimumSizeForCalculateIndexEtagLength(query);
            var length = minLength;

            if (handleReferences != null)
            {
                // last referenced collection etags (document + tombstone)
                // last processed reference collection etags (document + tombstone)
                // last processed in memory (early exit batch) etags (document + tombstone)
                length += sizeof(long) * 6 * Collections.Count * referencedCollections.Count;
            }

            if (handleCompareExchangeReferences != null)
            {
                // last referenced collection etags (document + tombstone)
                // last processed reference collection etags (document + tombstone)
                // last processed in memory (early exit batch) etags (document + tombstone)
                length += sizeof(long) * 6 * compiled.CollectionsWithCompareExchangeReferences.Count;
            }

            var indexEtagBytes = stackalloc byte[length];

            CalculateIndexEtagInternal(indexEtagBytes, isStale, State, queryContext, indexContext);
            UseAllDocumentsCounterCmpXchgAndTimeSeriesEtags(queryContext, query, length, indexEtagBytes);

            var writePos = indexEtagBytes + minLength;

            return StaticIndexHelper.CalculateIndexEtag(this, compiled, length, indexEtagBytes, writePos, queryContext, indexContext);
        }

        private static unsafe void UseAllDocumentsCounterCmpXchgAndTimeSeriesEtags(QueryOperationContext queryContext, QueryMetadata q, int length, byte* indexEtagBytes)
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

                *(long*)(indexEtagBytes + length - sizeof(long)) = queryContext.Documents.DocumentDatabase.CompareExchangeStorage.GetLastCompareExchangeIndex(queryContext.Server);
            }
        }

        private int MinimumSizeForCalculateIndexEtagLength(QueryMetadata q)
        {
            var length = sizeof(long) * 4 * Collections.Count + // last document etag, last tombstone etag and last mapped etags per collection
                         sizeof(int) + // definition hash
                         1 + // isStale
                         1 + // index state
                         sizeof(long); // created timestamp (binary)

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

        private unsafe void CalculateIndexEtagInternal(byte* indexEtagBytes, bool isStale, IndexState indexState,
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
            indexEtagBytes += sizeof(byte);
            *(long*)indexEtagBytes = _indexStorage.CreatedTimestampAsBinary;
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

        public Dictionary<TombstoneDeletionBlockageSource, HashSet<string>> GetDisabledSubscribersCollections(HashSet<string> tombstoneCollections)
        {
            var dict = new Dictionary<TombstoneDeletionBlockageSource, HashSet<string>>();

            if (Status is not (IndexRunningStatus.Disabled or IndexRunningStatus.Paused))
                return dict;

            var source = new TombstoneDeletionBlockageSource(ITombstoneAware.TombstoneDeletionBlockerType.Index, Name);
            dict[source] = Collections;

            return dict;
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

        public abstract IQueryResultRetriever GetQueryResultRetriever(
            IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsOperationContext documentsContext, SearchEngineType searchEngineType, FieldsToFetch fieldsToFetch,
            IncludeDocumentsCommand includeDocumentsCommand, IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand, IncludeRevisionsCommand includeRevisionsCommand);

        public abstract void SaveLastState();

        protected void HandleSourceDocumentIncludedInMapOutput()
        {
            if (_alreadyNotifiedAboutIncludingDocumentInOutput || SourceDocumentIncludedInOutput == false || PerformanceHintsConfig.AlertWhenSourceDocumentIncludedInOutput == false)
                return;

            DocumentDatabase.NotificationCenter.Add(PerformanceHint.Create(
                DocumentDatabase.Name,
                $"Index '{Name}' is including the origin document in output.",
                $"Putting the whole document as one of the fields of the index entry isn't usually intentional. Especially when it is a fanout index because the document is included multiple times. Please verify your index definition for better indexing performance.",
                PerformanceHintType.Indexing,
                NotificationSeverity.Warning,
                nameof(Index)));

            _alreadyNotifiedAboutIncludingDocumentInOutput = true;
        }

        protected void HandleIndexOutputsPerDocument(LazyStringValue documentId, int numberOfOutputs, IndexingStatsScope stats)
        {
            stats.RecordNumberOfProducedOutputs(numberOfOutputs);

            if (numberOfOutputs > MaxNumberOfOutputsPerDocument)
                MaxNumberOfOutputsPerDocument = numberOfOutputs;

            if (PerformanceHintsConfig.MaxWarnIndexOutputsPerDocument <= 0 || numberOfOutputs <= PerformanceHintsConfig.MaxWarnIndexOutputsPerDocument)
                return;

            _indexOutputsPerDocumentWarning.NumberOfExceedingDocuments++;

            if (_indexOutputsPerDocumentWarning.MaxNumberOutputsPerDocument < numberOfOutputs)
            {
                _indexOutputsPerDocumentWarning.MaxNumberOutputsPerDocument = numberOfOutputs;
                _indexOutputsPerDocumentWarning.SampleDocumentId = documentId;
            }

            DocumentDatabase.NotificationCenter.Indexing.AddWarning(Name, _indexOutputsPerDocumentWarning);
        }

        public void CheckReferenceLoadsPerformanceHintLimit(HandleReferencesBase.Reference reference, int numberOfLoads)
        {
            if (numberOfLoads < PerformanceHintsConfig.MaxNumberOfLoadsPerReference)
                return;

            _referenceLoadWarning ??= new IndexingReferenceLoadWarning.WarningDetails();

            if (_referenceLoadWarning.Add(reference, numberOfLoads))
                _updateReferenceLoadWarning = true;
        }

        public virtual Dictionary<string, HashSet<CollectionName>> GetReferencedCollections()
        {
            return null;
        }

        public virtual bool WorksOnAnyCollection(HashSet<string> collections)
        {
            if (Collections.Count == 0)
                return false;

            return Collections.Overlaps(collections);
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

        
        
        public ArchivedDataProcessingBehavior ArchivedDataProcessingBehavior { get; private set; }

        internal ArchivedDataProcessingBehavior GetDefaultArchivedDataProcessingBehavior()
        {
            return SourceType == IndexSourceType.Documents
                ? Type.IsAuto()
                    ? DocumentDatabase.Configuration.Indexing.AutoIndexArchivedDataProcessingBehavior
                    : DocumentDatabase.Configuration.Indexing.StaticIndexArchivedDataProcessingBehavior
                : ArchivedDataProcessingBehavior.IncludeArchived;
        }

        private DateTime _lastCheckedFlushLock;

        private bool ShouldReleaseTransactionBecauseFlushIsWaiting(IndexingStatsScope stats, IndexingWorkType type)
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
                    stats.RecordBatchCompletedReason(type, "Environment flush was waiting for us and global flusher was about to use all free flushing resources");
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

        public enum CanContinueBatchResult
        {
            None,
            True,
            False,
            RenewTransaction
        }

        public CanContinueBatchResult CanContinueBatch(in CanContinueBatchParameters parameters, ref TimeSpan maxTimeForDocumentTransactionToRemainOpen)
        {
            if (Configuration.MapBatchSize.HasValue && parameters.Count >= Configuration.MapBatchSize.Value)
            {
                parameters.Stats.RecordBatchCompletedReason(parameters.WorkType, $"Reached maximum configured map batch size ({Configuration.MapBatchSize.Value:#,#;;0}).");
                return CanContinueBatchResult.False;
            }

            if (parameters.CurrentEtag >= parameters.MaxEtag && parameters.Stats.Duration >= Configuration.MapTimeoutAfterEtagReached.AsTimeSpan)
            {
                parameters.Stats.RecordBatchCompletedReason(parameters.WorkType, $"Reached maximum etag that was seen when batch started ({parameters.MaxEtag:#,#;;0}) and map duration ({parameters.Stats.Duration}) exceeded configured limit ({Configuration.MapTimeoutAfterEtagReached.AsTimeSpan})");
                return CanContinueBatchResult.False;
            }

            if (parameters.Count % 128 != 0)
            {
                // do the actual check only every N ops
                return CanContinueBatchResult.True;
            }

            if (parameters.Sw.Elapsed > maxTimeForDocumentTransactionToRemainOpen)
            {
                if (parameters.QueryContext.Documents.ShouldRenewTransactionsToAllowFlushing() || _forTestingPurposes is { ShouldRenewTransaction: true })
                    return CanContinueBatchResult.RenewTransaction;

                // if we haven't had writes in the meantime, there is no point
                // in replacing the database transaction, and it will probably cost more
                // let us check again later to see if we need to
                maxTimeForDocumentTransactionToRemainOpen =
                    maxTimeForDocumentTransactionToRemainOpen.Add(
                        Configuration.MaxTimeForDocumentTransactionToRemainOpen.AsTimeSpan);
            }

            if (parameters.Stats.Duration >= Configuration.MapTimeout.AsTimeSpan)
            {
                parameters.Stats.RecordBatchCompletedReason(parameters.WorkType, $"Exceeded maximum configured map duration ({Configuration.MapTimeout.AsTimeSpan}). Was {parameters.Stats.Duration}");
                return CanContinueBatchResult.False;
            }

            if (ShouldReleaseTransactionBecauseFlushIsWaiting(parameters.Stats, parameters.WorkType))
            {
                parameters.Stats.RecordBatchCompletedReason(parameters.WorkType, "Releasing the transaction because we have a pending flush");
                return CanContinueBatchResult.False;
            }

            var txAllocationsInBytes = UpdateThreadAllocations(parameters.IndexingContext, parameters.IndexWriteOperation, parameters.Stats, parameters.WorkType);

            // we need to take the read transaction encryption size into account as we might read a lot of documents and produce very little indexing output.
            txAllocationsInBytes += parameters.QueryContext.Documents.Transaction.InnerTransaction.LowLevelTransaction.AdditionalMemoryUsageSize.GetValue(SizeUnit.Bytes);

            if (_indexDisabled)
            {
                parameters.Stats.RecordBatchCompletedReason(parameters.WorkType, "Index was disabled");
                return CanContinueBatchResult.False;
            }

            var cpuCreditsAlertFlag = DocumentDatabase.ServerStore.Server.CpuCreditsBalance.BackgroundTasksAlertRaised;
            if (cpuCreditsAlertFlag.IsRaised())
            {
                HandleStoppedBatchesConcurrently(parameters.Stats, parameters.Count,
                   canContinue: () => cpuCreditsAlertFlag.IsRaised() == false,
                   reason: "CPU credits balance is low", parameters.WorkType);

                parameters.Stats.RecordBatchCompletedReason(parameters.WorkType, $"The batch was stopped after processing {parameters.Count:#,#;;0} documents because the CPU credits balance is almost completely used");
                return CanContinueBatchResult.False;
            }

            if (_lowMemoryFlag.IsRaised() && parameters.Count > MinBatchSize)
            {
                HandleStoppedBatchesConcurrently(parameters.Stats, parameters.Count,
                    canContinue: () => _lowMemoryFlag.IsRaised() == false,
                    reason: "low memory", parameters.WorkType);

                parameters.Stats.RecordBatchCompletedReason(parameters.WorkType, $"The batch was stopped after processing {parameters.Count:#,#;;0} documents because of low memory");
                return CanContinueBatchResult.False;
            }

            if (_firstBatchTimeout.HasValue && parameters.Stats.Duration > _firstBatchTimeout)
            {
                parameters.Stats.RecordBatchCompletedReason(parameters.WorkType,
                    $"Stopping the first batch after {_firstBatchTimeout} to ensure just created index has some results");

                _firstBatchTimeout = null;

                return CanContinueBatchResult.False;
            }

            if (parameters.Stats.ErrorsCount >= IndexStorage.MaxNumberOfKeptErrors)
            {
                parameters.Stats.RecordBatchCompletedReason(parameters.WorkType,
                    $"Number of errors ({parameters.Stats.ErrorsCount}) reached maximum number of allowed errors per batch ({IndexStorage.MaxNumberOfKeptErrors})");
                return CanContinueBatchResult.False;
            }

            if (DocumentDatabase.Is32Bits)
            {
                var llt = parameters.QueryContext.Documents.Transaction?.InnerTransaction?.LowLevelTransaction;
                var total32BitsMappedSize = llt?.PagerTransactionState.GetTotal32BitsMappedSize();
                if (total32BitsMappedSize > MappedSizeLimitOn32Bits)
                {
                    parameters.Stats.RecordBatchCompletedReason(parameters.WorkType, $"Running in 32 bits and have {total32BitsMappedSize} mapped in docs ctx");
                    return CanContinueBatchResult.False;
                }

                llt = parameters.IndexingContext.Transaction?.InnerTransaction?.LowLevelTransaction;
                total32BitsMappedSize = llt?.PagerTransactionState.GetTotal32BitsMappedSize();
                if (total32BitsMappedSize > MappedSizeLimitOn32Bits)
                {
                    parameters.Stats.RecordBatchCompletedReason(parameters.WorkType, $"Running in 32 bits and have {total32BitsMappedSize} mapped in index ctx");
                    return CanContinueBatchResult.False;
                }
            }

            if (TransactionSizeLimit != null)
            {
                var txAllocations = new Size(txAllocationsInBytes, SizeUnit.Bytes);
                if (txAllocations > TransactionSizeLimit.Value)
                {
                    parameters.Stats.RecordBatchCompletedReason(parameters.WorkType, $"Reached transaction size limit ({TransactionSizeLimit.Value}). Allocated {new Size(txAllocationsInBytes, SizeUnit.Bytes)} in current transaction");
                    return CanContinueBatchResult.False;
                }
            }

            if (Configuration.ManagedAllocationsBatchLimit != null)
            {
                var currentManagedAllocations = new Size(GC.GetAllocatedBytesForCurrentThread(), SizeUnit.Bytes);
                var diff = currentManagedAllocations - _initialManagedAllocations;
                parameters.Stats.SetAllocatedManagedBytes(diff.GetValue(SizeUnit.Bytes));

                if (diff > Configuration.ManagedAllocationsBatchLimit.Value)
                {
                    parameters.Stats.RecordBatchCompletedReason(parameters.WorkType, $"Reached managed allocations limit ({Configuration.ManagedAllocationsBatchLimit.Value}). Allocated {diff} in current batch");
                    return CanContinueBatchResult.False;
                }
            }

            if (Configuration.ScratchSpaceLimit != null &&
                _environment.Options.ScratchSpaceUsage.ScratchSpaceInBytes > Configuration.ScratchSpaceLimit.Value.GetValue(SizeUnit.Bytes) && parameters.Count > MinBatchSize)
            {
                _scratchSpaceLimitExceeded = true;

                parameters.Stats.RecordBatchCompletedReason(parameters.WorkType,
                    $"Reached scratch space limit ({Configuration.ScratchSpaceLimit.Value}). Current scratch space is {new Size(_environment.Options.ScratchSpaceUsage.ScratchSpaceInBytes, SizeUnit.Bytes)}");

                return CanContinueBatchResult.False;
            }

            var globalIndexingScratchSpaceUsage = DocumentDatabase.ServerStore.GlobalIndexingScratchSpaceMonitor;

            if (globalIndexingScratchSpaceUsage?.IsLimitExceeded == true && parameters.Count > MinBatchSize)
            {
                _scratchSpaceLimitExceeded = true;

                parameters.Stats.RecordBatchCompletedReason(parameters.WorkType,
                    $"Reached global scratch space limit for indexing ({globalIndexingScratchSpaceUsage.LimitAsSize}). Current scratch space is {globalIndexingScratchSpaceUsage.ScratchSpaceAsSize}");

                return CanContinueBatchResult.False;
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
                    DocumentDatabase.ServerStore.Server.MetricCacher,
                    _logger,
                    out var memoryUsage) == false)
                {
                    Interlocked.Increment(ref _allocationCleanupNeeded);

                    parameters.QueryContext.Documents.DoNotReuse = true;
                    parameters.IndexingContext.DoNotReuse = true;

                    switch (parameters.WorkType)
                    {
                        case IndexingWorkType.Map:
                            if (parameters.Stats.MapAttempts >= Configuration.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)
                                canContinue = false;
                            break;
                        case IndexingWorkType.References:
                            if (parameters.Count >= Configuration.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)
                                canContinue = false;
                            break;
                        case IndexingWorkType.Cleanup:
                            if (parameters.Stats.TombstoneDeleteSuccesses >= Configuration.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)
                                canContinue = false;
                            break;
                    }

                    if (canContinue == false)
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

                        HandleStoppedBatchesConcurrently(parameters.Stats, parameters.Count,
                            canContinue: MemoryUsageGuard.CanIncreaseMemoryUsageForThread,
                            reason: "cannot budget additional memory", parameters.WorkType);

                        parameters.Stats.RecordBatchCompletedReason(parameters.WorkType, "Cannot budget additional memory for batch");
                    }
                }

                if (memoryUsage != null)
                {
                    switch (parameters.WorkType)
                    {
                        case IndexingWorkType.Map:
                            parameters.Stats.RecordMapMemoryStats(memoryUsage.WorkingSet, memoryUsage.PrivateMemory, _currentMaximumAllowedMemory.GetValue(SizeUnit.Bytes));
                            break;
                        case IndexingWorkType.References:
                            parameters.Stats.RecordReferenceMemoryStats(memoryUsage.WorkingSet, memoryUsage.PrivateMemory, _currentMaximumAllowedMemory.GetValue(SizeUnit.Bytes));
                            break;
                    }
                }

                return canContinue ? CanContinueBatchResult.True : CanContinueBatchResult.False;
            }

            return CanContinueBatchResult.True;
        }

        public long UpdateThreadAllocations(
            TransactionOperationContext indexingContext,
            Lazy<IndexWriteOperationBase> indexWriteOperation,
            IndexingStatsScope stats,
            IndexingWorkType workType)
        {
            var threadAllocations = _threadAllocations.TotalAllocated;
            var txAllocations = indexingContext.Transaction.InnerTransaction.LowLevelTransaction.NumberOfModifiedPages
                                * Voron.Global.Constants.Storage.PageSize;

            txAllocations += indexingContext.Transaction.InnerTransaction.LowLevelTransaction.AdditionalMemoryUsageSize.GetValue(SizeUnit.Bytes);

            long indexWriterAllocations = 0;
            long luceneFilesAllocations = 0;

            if (indexWriteOperation?.IsValueCreated == true)
            {
                var allocations = indexWriteOperation.Value.GetAllocations();
                indexWriterAllocations = allocations.RamSizeInBytes;
                luceneFilesAllocations = allocations.FilesAllocationsInBytes;
            }

            var totalTxAllocations = txAllocations + luceneFilesAllocations;

            if (stats != null)
            {
                var allocatedForStats = threadAllocations + totalTxAllocations + indexWriterAllocations;

                switch (workType)
                {
                    case IndexingWorkType.References:
                        stats.RecordReferenceAllocations(allocatedForStats);
                        break;
                    case IndexingWorkType.Map:
                        stats.RecordMapAllocations(allocatedForStats);
                        break;
                    case IndexingWorkType.Reduce:
                        stats.RecordReduceAllocations(allocatedForStats);
                        break;
                }
            }

            stats?.SetAllocatedUnmanagedBytes(threadAllocations + txAllocations);

            var allocatedForProcessing = threadAllocations + indexWriterAllocations +
                                         // we multiple it to take into account additional work
                                         // that will need to be done during the commit phase of the index
                                         (long)(totalTxAllocations * _txAllocationsRatio);

            _threadAllocations.CurrentlyAllocatedForProcessing = allocatedForProcessing;

            return totalTxAllocations;
        }

        private void HandleStoppedBatchesConcurrently(
            IndexingStatsScope stats, long count,
            Func<bool> canContinue, string reason, IndexingWorkType type)
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
            stats.RecordBatchCompletedReason(type, message);
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

        public void Compact(Action<IOperationProgress> onProgress, CompactionResult result, bool shouldSkipOptimization, CancellationToken token)
        {
            if (IndexPersistence is CoraxIndexPersistence)
                throw new NotSupportedException($"{nameof(Compact)} is not supported for Corax indexes.");

            AssertCompactionOrOptimizationIsNotInProgress(Name, nameof(Compact));

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

                    using (RestartEnvironment(onBeforeEnvironmentDispose: () =>
                    {
                        if (shouldSkipOptimization)
                            return;

                        result.AddMessage($"Starting data optimization of index '{Name}'.");
                        onProgress?.Invoke(result.Progress);
                        OptimizeInternal(token);
                    }))
                    {
                        DocumentDatabase.IndexStore?.ForTestingPurposes?.IndexCompaction?.Invoke();

                        if (Type.IsMapReduce())
                        {
                            result.AddMessage($"Skipping data compaction of '{Name}' index because data compaction of map-reduce indexes isn't supported");
                            onProgress?.Invoke(result.Progress);
                            result.TreeName = null;
                            result.SizeAfterCompactionInMb = CalculateIndexStorageSize().GetValue(SizeUnit.Megabytes);

                            return;
                        }

                        var environmentOptions = (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)storageEnvironmentOptions;
                        var srcOptions = StorageEnvironmentOptions.ForPath(environmentOptions.BasePath.FullPath, environmentOptions.TempPath?.FullPath, null, DocumentDatabase.IoChanges,
                            DocumentDatabase.CatastrophicFailureNotification);

                        InitializeOptions(srcOptions, DocumentDatabase, Name, schemaUpgrader: false);

                        compactPath = Configuration.StoragePath.Combine(IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(Name) + "_Compact");
                        tempPath = Configuration.TempPath?.Combine(IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(Name) + "_Temp_Compact");

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
            }
        }

        public void Optimize(IndexOptimizeResult result, CancellationToken token)
        {
            IndexPersistence.AssertCanOptimize();

            AssertCompactionOrOptimizationIsNotInProgress(Name, nameof(Optimize));

            try
            {
                _isCompactionInProgress = true;
                using (DrainRunningQueries())
                using (RestartEnvironment(onBeforeEnvironmentDispose: () =>
                       {
                           result.Message = $"Optimization of index {Name} started...";
                           OptimizeInternal(token);
                       }))
                {
                }
            }
            finally
            {
                _isCompactionInProgress = false;
            }
        }

        private void OptimizeInternal(CancellationToken token)
        {
            try
            {
                using (var context = QueryOperationContext.Allocate(DocumentDatabase, this))
                using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
                using (CurrentIndexingScope.Current = CreateIndexingScope(indexContext, context))
                using (var txw = indexContext.OpenWriteTransaction())
                using (var writer = IndexPersistence.OpenIndexWriter(indexContext.Transaction.InnerTransaction, indexContext))
                {
                    writer.Optimize(token);

                    txw.Commit();
                }
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations("Unable to complete optimization, index is not usable and may require reset of the index to recover", e);

                throw;
            }
        }

        private void AssertCompactionOrOptimizationIsNotInProgress(string name, string operation)
        {
            if (_isCompactionInProgress)
                throw new InvalidOperationException($"Index '{Name}' cannot be '{operation}' because compaction/optimization is already in progress.");
        }

        public IDisposable RestartEnvironment(Action onBeforeEnvironmentDispose = null)
        {
            // shutdown environment
            if (_isRunningQueriesWriteLockTaken.Value == false)
                throw new InvalidOperationException("Expected to be called only via DrainRunningQueries");

            if (Configuration.RunInMemory)
                throw new InvalidOperationException("Cannot restart the environment of an index running in-memory");

            // here we ensure that we aren't currently running any indexing,
            // because we'll shut down the environment for this index, reads
            // are handled using the DrainRunningQueries portion
            var thread = GetWaitForIndexingThreadToExit(disableIndex: false);
            thread?.Join(Timeout.Infinite);

            onBeforeEnvironmentDispose?.Invoke();

            IndexPersistence.Dispose();

            _environment.Dispose();

            return new DisposableAction(() =>
            {
                // restart environment
                if (_isRunningQueriesWriteLockTaken.Value == false)
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

        public virtual unsafe DetailedStorageReport GenerateStorageReport(bool calculateExactSizes)
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                var detailedReportInput = _environment.CreateDetailedReportInput(tx.InnerTransaction, calculateExactSizes);
                var llt = tx.InnerTransaction.LowLevelTransaction;
                var generator = new StorageReportGenerator(llt);

                generator.HandlePostingListDetails += (postingList, report) =>
                {
                    bool isLargePostingList = Corax.Constants.IndexWriter.LargePostingListsSetSlice.AsReadOnlySpan().SequenceEqual(postingList.Name.AsReadOnlySpan());
                    if (isLargePostingList == false)
                        return;

                    Span<long> buffer = stackalloc long[1024];
                    var it = postingList.Iterate();
                    while (it.Fill(buffer, out var read))
                    {
                        for (int i = 0; i < read; i++)
                        {
                            Container.Get(llt, buffer[i], out var item);
                            var state = (PostingListState*)item.Address;
                            report.BranchPages += state->BranchPages;
                            report.LeafPages += state->LeafPages;
                            report.PageCount += state->BranchPages + state->LeafPages;
                            report.AllocatedSpaceInBytes += StorageReportGenerator.PagesToBytes(state->BranchPages + state->LeafPages);

                        }
                    }

                };
                return generator.Generate(detailedReportInput);
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
                    if (directoryName == IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(index.Key))
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
                    if (directoryName == IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(index.Key))
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

        protected sealed class IndexQueryDoneRunning : IDisposable
        {
            private static readonly TimeSpan DefaultLockTimeout = TimeSpan.FromSeconds(3);

            private static readonly TimeSpan ExtendedLockTimeout = TimeSpan.FromSeconds(30);

            private static readonly CancellationToken CancelledToken;

            static IndexQueryDoneRunning()
            {
                var cts = new CancellationTokenSource();
                cts.Cancel();

                CancelledToken = cts.Token;
            }

            private readonly Index _parent;
            private IDisposable _lock;

            public IndexQueryDoneRunning(Index parent)
            {
                _parent = parent;
            }

            public void HoldLock()
            {
                var timeout = _parent._isReplacing
                    ? ExtendedLockTimeout
                    : DefaultLockTimeout;

                if (_lock != null)
                    ThrowLockAlreadyTaken();

                try
                {
                    using (var cts = new CancellationTokenSource(timeout))
                        _lock = _parent._currentlyRunningQueriesLock.ReaderLock(cts.Token);
                }
                catch (OperationCanceledException)
                {
                    ThrowLockTimeoutException();
                }
            }

            public async ValueTask HoldLockAsync()
            {
                var timeout = _parent._isReplacing
                    ? ExtendedLockTimeout
                    : DefaultLockTimeout;

                if (_lock != null)
                    ThrowLockAlreadyTaken();

                try
                {
                    using (var cts = new CancellationTokenSource(timeout))
                        _lock = await _parent._currentlyRunningQueriesLock.ReaderLockAsync(cts.Token);
                }
                catch (OperationCanceledException)
                {
                    ThrowLockTimeoutException();
                }
            }

            public bool TryHoldLock()
            {
                if (_lock != null)
                    ThrowLockAlreadyTaken();

                try
                {
                    _lock = _parent._currentlyRunningQueriesLock.ReaderLock(CancelledToken);
                }
                catch (OperationCanceledException)
                {
                    return false;
                }

                return true;
            }

            [DoesNotReturn]
            private void ThrowLockTimeoutException()
            {
                throw new TimeoutException(
                    $"Could not get the index read lock in a reasonable time, {_parent.Name} is probably undergoing maintenance now, try again later");
            }

            [DoesNotReturn]
            private void ThrowLockAlreadyTaken()
            {
                throw new InvalidOperationException(
                    $"Read lock of index {_parent.Name} was already taken");
            }

            public void ReleaseLock()
            {
                _lock?.Dispose();
                _lock = null;
            }

            public void Dispose()
            {
                ReleaseLock();
            }
        }

        internal sealed class ExitWriteLock : IDisposable
        {
            private readonly IDisposable _writeLock;
            private readonly Index _parent;

            public ExitWriteLock(IDisposable writeLock, Index parent)
            {
                _writeLock = writeLock;
                _parent = parent;
            }

            public void Dispose()
            {
                _writeLock.Dispose();
                _parent._isRunningQueriesWriteLockTaken.Value = false;
            }
        }

        public void AssertNotDisposed()
        {
            if (_disposeOnce.Disposed)
                ThrowObjectDisposed();
        }

        public int Dump(string path, Action<IOperationProgress> onProgress)
        {
            IndexPersistence.AssertCanDump();

            LuceneIndexPersistence indexPersistence = (LuceneIndexPersistence)IndexPersistence;
            if (Directory.Exists(path) == false)
                Directory.CreateDirectory(path);

            using (CurrentlyInUse())
            using (var tx = _environment.ReadTransaction())
            {
                var state = new Indexing.VoronState(tx);

                var files = indexPersistence.LuceneDirectory.ListAll(state);
                var currentFile = 0;
                var buffer = new byte[64 * 1024];
                var sp = Stopwatch.StartNew();
                foreach (var file in files)
                {
                    using (var input = indexPersistence.LuceneDirectory.OpenInput(file, state))
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

        internal TestingStuff _forTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (_forTestingPurposes != null)
                return _forTestingPurposes;

            return _forTestingPurposes = new TestingStuff();
        }

        internal sealed class TestingStuff
        {
            internal Action ActionToCallInFinallyOfExecuteIndexing;

            internal bool ShouldRenewTransaction;

            internal Action BeforeClosingDocumentsReadTransactionForHandleReferences;

            internal IDisposable CallDuringFinallyOfExecuteIndexing(Action action)
            {
                ActionToCallInFinallyOfExecuteIndexing = action;

                return new DisposableAction(() => ActionToCallInFinallyOfExecuteIndexing = null);
            }
        }
    }
}
