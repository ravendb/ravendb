using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Util;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Faceted;
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.Documents.Queries.Parser;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Suggestion;
using Raven.Server.Exceptions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Memory;
using Raven.Server.Utils;
using Raven.Server.Utils.Metrics;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Json;
using Voron;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Utils;
using Size = Sparrow.Size;
using Voron.Debugging;
using Voron.Exceptions;
using Voron.Impl;
using Voron.Impl.Compaction;

namespace Raven.Server.Documents.Indexes
{
    public abstract class Index<TIndexDefinition> : Index
        where TIndexDefinition : IndexDefinitionBase
    {
        public new TIndexDefinition Definition => (TIndexDefinition)base.Definition;

        protected Index(long etag, IndexType type, TIndexDefinition definition)
            : base(etag, type, definition)
        {
        }
    }

    public abstract class Index : IDocumentTombstoneAware, IDisposable, ILowMemoryHandler
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

        /// <summary>
        /// If not null, a batch of indexing work is in progress. Cancelling
        /// this token source will force the current indexing batch work to 
        /// end prematurely. This does NOT abort the indexing process, but
        /// merely forces the batch to restart.
        /// 
        /// Example usage is adjusting the memory allocation parameters.
        /// </summary>
        private CancellationTokenSource _batchProcessCancellationTokenSource;

        protected DocumentDatabase DocumentDatabase;

        private Thread _indexingThread;

        private bool _initialized;

        protected UnmanagedBuffersPoolWithLowMemoryHandling _unmanagedBuffersPool;

        private StorageEnvironment _environment;

        internal TransactionContextPool _contextPool;

        internal bool _disposed;
        private bool _disposing;

        protected readonly ManualResetEventSlim _mre = new ManualResetEventSlim();

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

        protected readonly bool HandleAllDocs;

        protected internal MeterMetric MapsPerSec = new MeterMetric();
        protected internal MeterMetric ReducesPerSec = new MeterMetric();

        protected internal IndexingConfiguration Configuration;

        protected PerformanceHintsConfiguration PerformanceHints;

        private bool _allocationCleanupNeeded;
        private Size _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;
        private NativeMemory.ThreadStats _threadAllocations;
        private string _errorStateReason;
        private bool _isCompactionInProgress;
        private readonly ReaderWriterLockSlim _currentlyRunningQueriesLock = new ReaderWriterLockSlim();
        private volatile bool _priorityChanged;
        private volatile bool _hadRealIndexingWorkToDo;
        private Func<bool> _indexValidationStalenessCheck = () => true;

        private string IndexingThreadName => "Indexing of " + Name + " of " + _indexStorage.DocumentDatabase.Name;

        private readonly WarnIndexOutputsPerDocument _indexOutputsPerDocumentWarning = new WarnIndexOutputsPerDocument
        {
            MaxNumberOutputsPerDocument = int.MinValue,
            Suggestion = "Please verify this index definition and consider a re-design of your entities or index for better indexing performance"
        };

        private static readonly Size DefaultMaximumMemoryAllocation = new Size(32, SizeUnit.Megabytes);

        protected Index(long etag, IndexType type, IndexDefinitionBase definition)
        {
            if (etag <= 0)
                throw new ArgumentException("Index etag must be greater than zero.", nameof(etag));

            Etag = etag;
            Type = type;
            Definition = definition;
            Collections = new HashSet<string>(Definition.Collections, StringComparer.OrdinalIgnoreCase);

            if (Collections.Contains(Constants.Documents.Collections.AllDocumentsCollection))
                HandleAllDocs = true;
        }

        public static Index Open(long etag, string path, DocumentDatabase documentDatabase)
        {
            StorageEnvironment environment = null;

            var name = Path.GetDirectoryName(path);
            var indexPath = path;

            var indexTempPath =
                documentDatabase.Configuration.Indexing.TempPath?.Combine(name);

            var journalPath = documentDatabase.Configuration.Indexing.JournalsStoragePath?.Combine(name);

            var options = StorageEnvironmentOptions.ForPath(indexPath, indexTempPath?.FullPath, journalPath?.FullPath,
                documentDatabase.IoChanges, documentDatabase.CatastrophicFailureNotification);
            try
            {
                options.OnNonDurableFileSystemError += documentDatabase.HandleNonDurableFileSystemError;
                options.OnRecoveryError += documentDatabase.HandleOnRecoveryError;
                options.CompressTxAboveSizeInBytes = documentDatabase.Configuration.Storage.CompressTxAboveSize.GetValue(SizeUnit.Bytes);
                options.SchemaVersion = 1;
                options.ForceUsing32BitsPager = documentDatabase.Configuration.Storage.ForceUsing32BitsPager;
                options.TimeToSyncAfterFlashInSec = (int)documentDatabase.Configuration.Storage.TimeToSyncAfterFlash.AsTimeSpan.TotalSeconds;
                options.NumOfConcurrentSyncsPerPhysDrive = documentDatabase.Configuration.Storage.NumberOfConcurrentSyncsPerPhysicalDrive;
                Sodium.CloneKey(out options.MasterKey, documentDatabase.MasterKey);

                environment = new StorageEnvironment(options);

                IndexType type;
                try
                {
                    type = IndexStorage.ReadIndexType(etag, environment);
                }
                catch (Exception e)
                {
                    throw new IndexOpenException(
                        $"Could not read index type from storage in '{path}'. This indicates index data file corruption.",
                        e);
                }

                switch (type)
                {
                    case IndexType.AutoMap:
                        return AutoMapIndex.Open(etag, environment, documentDatabase);
                    case IndexType.AutoMapReduce:
                        return AutoMapReduceIndex.Open(etag, environment, documentDatabase);
                    case IndexType.Map:
                        return MapIndex.Open(etag, environment, documentDatabase);
                    case IndexType.MapReduce:
                        return MapReduceIndex.Open(etag, environment, documentDatabase);
                    default:
                        throw new ArgumentException($"Unknown index type {type} for index {etag}");
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

        public long Etag { get; set; }

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


        public AsyncManualResetEvent.FrozenAwaiter GetIndexingBatchAwaiter()
        {
            if (_disposed)
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
                    throw new InvalidOperationException($"Index '{Name} ({Etag})' was already initialized.");

                var name = IndexDefinitionBase.GetIndexNameSafeForFileSystem(Name);

                var indexPath = configuration.StoragePath.Combine(name);

                var indexTempPath = configuration.TempPath?.Combine(name);

                var journalPath = configuration.JournalsStoragePath?.Combine(name);

                var options = configuration.RunInMemory
                    ? StorageEnvironmentOptions.CreateMemoryOnly(indexPath.FullPath, indexTempPath?.FullPath,
                        documentDatabase.IoChanges, documentDatabase.CatastrophicFailureNotification)
                    : StorageEnvironmentOptions.ForPath(indexPath.FullPath, indexTempPath?.FullPath, journalPath?.FullPath,
                        documentDatabase.IoChanges, documentDatabase.CatastrophicFailureNotification);

                options.OnNonDurableFileSystemError += documentDatabase.HandleNonDurableFileSystemError;
                options.OnRecoveryError += documentDatabase.HandleOnRecoveryError;

                options.SchemaVersion = 1;
                options.CompressTxAboveSizeInBytes = documentDatabase.Configuration.Storage.CompressTxAboveSize.GetValue(SizeUnit.Bytes);
                options.ForceUsing32BitsPager = documentDatabase.Configuration.Storage.ForceUsing32BitsPager;
                options.TimeToSyncAfterFlashInSec = (int)documentDatabase.Configuration.Storage.TimeToSyncAfterFlash.AsTimeSpan.TotalSeconds;
                options.NumOfConcurrentSyncsPerPhysDrive = documentDatabase.Configuration.Storage.NumberOfConcurrentSyncsPerPhysicalDrive;
                Sodium.CloneKey(out options.MasterKey, documentDatabase.MasterKey);

                try
                {
                    Initialize(new StorageEnvironment(options), documentDatabase, configuration, performanceHints);
                }
                catch (Exception)
                {
                    options.Dispose();
                    throw;
                }
            }
        }

        internal ExitWriteLock DrainRunningQueries()
        {
            if (_currentlyRunningQueriesLock.IsWriteLockHeld)
                return new ExitWriteLock();

            if (_currentlyRunningQueriesLock.TryEnterWriteLock(TimeSpan.FromSeconds(10)) == false)
            {
                throw new InvalidOperationException("After waiting for 10 seconds for all running queries ");
            }
            return new ExitWriteLock(_currentlyRunningQueriesLock);
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

        protected void Initialize(StorageEnvironment environment, DocumentDatabase documentDatabase, IndexingConfiguration configuration, PerformanceHintsConfiguration performanceHints)
        {
            if (_disposed)
                throw new ObjectDisposedException($"Index '{Name} ({Etag})' was already disposed.");

            using (DrainRunningQueries())
            {
                if (_initialized)
                    throw new InvalidOperationException($"Index '{Name} ({Etag})' was already initialized.");

                try
                {
                    Debug.Assert(Definition != null);

                    DocumentDatabase = documentDatabase;
                    Configuration = configuration;
                    PerformanceHints = performanceHints;

                    _environment = environment;
                    _unmanagedBuffersPool = new UnmanagedBuffersPoolWithLowMemoryHandling($"Indexes//{Etag}");
                    _contextPool = new TransactionContextPool(_environment);
                    _indexStorage = new IndexStorage(this, _contextPool, documentDatabase);
                    _logger = LoggingSource.Instance.GetLogger<Index>(documentDatabase.Name);
                    _indexStorage.Initialize(_environment);
                    IndexPersistence = new LuceneIndexPersistence(this);
                    IndexPersistence.Initialize(_environment);

                    LoadValues();

                    DocumentDatabase.DocumentTombstoneCleaner.Subscribe(this);

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

                    InitializeInternal();

                    _initialized = true;
                }
                catch (Exception)
                {
                    Dispose();
                    throw;
                }
            }
        }

        protected virtual void InitializeInternal()
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
            if (_disposed)
                throw new ObjectDisposedException($"Index '{Name} ({Etag})' was already disposed.");

            if (_initialized == false)
                throw new InvalidOperationException($"Index '{Name} ({Etag})' was not initialized.");

            using (DrainRunningQueries())
            {
                if (_indexingThread != null)
                    throw new InvalidOperationException($"Index '{Name} ({Etag})' is executing.");

                if (Configuration.Disabled)
                    return;

                if (State == IndexState.Disabled)
                    return;

                SetState(IndexState.Normal);

                _indexingProcessCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(DocumentDatabase.DatabaseShutdown);

                _indexingThread = new Thread(() =>
                {
                    try
                    {
                        ExecuteIndexing();
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsInfoEnabled)
                        {
                            _logger.Info("Failed to execute indexing in " + IndexingThreadName, e);
                        }
                    }
                })
                {
                    Name = IndexingThreadName,
                    IsBackground = true
                };

                _indexingThread.Start();
            }
        }

        public virtual void Stop()
        {
            if (_disposed)
                throw new ObjectDisposedException($"Index '{Name} ({Etag})' was already disposed.");

            if (_initialized == false)
                throw new InvalidOperationException($"Index '{Name} ({Etag})' was not initialized.");

            using (DrainRunningQueries())
            {
                if (_indexingThread == null)
                    return;

                _indexingProcessCancellationTokenSource.Cancel();

                var indexingThread = _indexingThread;
                _indexingThread = null;
                //Cancellation was requested, the thread will exit the indexing loop and terminate.
                //If we invoke Thread.Join from the indexing thread itself it will cause a deadlock
                if (Thread.CurrentThread != indexingThread)
                    indexingThread.Join();
            }
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

                InitializeInternal();

                _priorityChanged = true;

                if (status == IndexRunningStatus.Running)
                    Start();
            }
        }

        public virtual void Dispose()
        {
            _disposing = true;
            var needToLock = _currentlyRunningQueriesLock.IsWriteLockHeld == false;
            if (needToLock)
                _currentlyRunningQueriesLock.EnterWriteLock();
            try
            {
                if (_disposed)
                    return;

                _disposed = true;

                _indexingProcessCancellationTokenSource?.Cancel();

                //Does happen for faulty in memory indexes
                if (DocumentDatabase != null)
                {
                    DocumentDatabase.DocumentTombstoneCleaner.Unsubscribe(this);

                    DocumentDatabase.Changes.OnIndexChange -= HandleIndexChange;
                }
                    
                _indexValidationStalenessCheck = null;

                var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(Index)} '{Name}'");

                exceptionAggregator.Execute(() =>
                {
                    _indexingThread?.Join();
                    _indexingThread = null;
                });

                exceptionAggregator.Execute(() =>
                {
                    IndexPersistence?.Dispose();
                    IndexPersistence = null;
                });

                exceptionAggregator.Execute(() =>
                {
                    _environment?.Dispose();
                    _environment = null;
                });

                exceptionAggregator.Execute(() =>
                {
                    _unmanagedBuffersPool?.Dispose();
                    _unmanagedBuffersPool = null;
                });

                exceptionAggregator.Execute(() =>
                {
                    _contextPool?.Dispose();
                    _contextPool = null;
                });

                exceptionAggregator.Execute(() =>
                {
                    _indexingProcessCancellationTokenSource?.Dispose();
                });

                exceptionAggregator.ThrowIfNeeded();
            }
            finally
            {
                if (needToLock)
                    _currentlyRunningQueriesLock.ExitWriteLock();
            }
        }

        public virtual bool IsStale(DocumentsOperationContext databaseContext, long? cutoff = null)
        {
            Debug.Assert(databaseContext.Transaction != null);

            if (_isCompactionInProgress)
                return true;

            if (Type == IndexType.Faulty)
                return true;

            using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
            using (indexContext.OpenReadTransaction())
            {
                return IsStale(databaseContext, indexContext, cutoff);
            }
        }

        public enum IndexProgressStatus
        {
            Faulty = -1,
            Compacting = -2,
            Stale = -3
        }

        public virtual (bool IsStale, long LastProcessedEtag) GetIndexStats(DocumentsOperationContext databaseContext)
        {
            Debug.Assert(databaseContext.Transaction != null);

            if (Type == IndexType.Faulty)
                return (true, (long)IndexProgressStatus.Faulty);

            if (_isCompactionInProgress)
                return (true, (long)IndexProgressStatus.Compacting);

            using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
            using (indexContext.OpenReadTransaction())
            {
                var isStale = IsStale(databaseContext, indexContext);

                if (isStale)
                    return (true, (long)IndexProgressStatus.Stale);

                long lastEtag = 0;
                foreach (var collection in Collections)
                {
                    lastEtag = Math.Max(lastEtag, _indexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection));
                }

                return (false, lastEtag);
            }
        }

        protected virtual bool IsStale(DocumentsOperationContext databaseContext,
            TransactionOperationContext indexContext, long? cutoff = null)
        {
            if (Type == IndexType.Faulty)
                return true;

            foreach (var collection in Collections)
            {
                var lastDocEtag = GetLastDocumentEtagInCollection(databaseContext, collection);

                var lastProcessedDocEtag = _indexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection);

                if (cutoff == null)
                {
                    if (lastDocEtag > lastProcessedDocEtag)
                        return true;

                    var lastTombstoneEtag = GetLastTombstoneEtagInCollection(databaseContext, collection);

                    var lastProcessedTombstoneEtag =
                        _indexStorage.ReadLastProcessedTombstoneEtag(indexContext.Transaction, collection);

                    if (lastTombstoneEtag > lastProcessedTombstoneEtag)
                        return true;
                }
                else
                {
                    if (Math.Min(cutoff.Value, lastDocEtag) > lastProcessedDocEtag)
                        return true;

                    if (
                        DocumentDatabase.DocumentsStorage.GetNumberOfTombstonesWithDocumentEtagLowerThan(
                            databaseContext, collection, cutoff.Value) > 0)
                        return true;
                }
            }

            return false;
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
        /// TODO iftah, change visibility of function back to internal when finished with new client
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
            _priorityChanged = true;
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
                    _contextPool.SetMostWorkInGoingToHappenonThisThread();

                    DocumentDatabase.Changes.OnDocumentChange += HandleDocumentChange;
                    storageEnvironment.OnLogsApplied += HandleLogsApplied;

                    while (true)
                    {
                        ChangeIndexThreadPriorityIfNeeded();

                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Starting indexing for '{Name} ({Etag})'.");

                        _mre.Reset();

                        var stats = _lastStats = new IndexingStatsAggregator(DocumentDatabase.IndexStore.Identities.GetNextIndexingStatsId(), _lastStats);
                        LastIndexingTime = stats.StartTime;

                        AddIndexingPerformance(stats);

                        var batchCompleted = false;

                        try
                        {
                            using (var scope = stats.CreateScope())
                            {
                                try
                                {
                                    _indexingProcessCancellationTokenSource.Token.ThrowIfCancellationRequested();
                                    _batchProcessCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(_indexingProcessCancellationTokenSource.Token);

                                    bool didWork = false;
                                    try
                                    {
                                        TimeSpentIndexing.Start();
                                        var _lastAllocatedBytes = GC.GetAllocatedBytesForCurrentThread();

                                        didWork = DoIndexingWork(scope, _batchProcessCancellationTokenSource.Token);

                                        _lastAllocatedBytes = GC.GetAllocatedBytesForCurrentThread() - _lastAllocatedBytes;
                                        scope.AddAllocatedBytes(_lastAllocatedBytes);
                                    }
                                    catch (OperationCanceledException)
                                    {
                                        // We are here because the batch has been cancelled. This may happen
                                        // because the database is shutting down, or the batch has been
                                        // cancelled by some other internal process. If the database is
                                        // shutting down, then we throw again to handle shutdown.
                                        if (_indexingProcessCancellationTokenSource.IsCancellationRequested)
                                        {
                                            throw;
                                        }
                                    }
                                    finally
                                    {
                                        
                                        TimeSpentIndexing.Stop();

                                        // If we are here, then the previous block did not throw. There's two
                                        // possibilities: either the batch was not cancelled, and we really
                                        // did finish the work, or the batch was cancelled, but not because of
                                        // a database shutdown
                                        batchCompleted = !_batchProcessCancellationTokenSource.IsCancellationRequested;
                                        _batchProcessCancellationTokenSource = null;
                                    }

                                    _indexingBatchCompleted.SetAndResetAtomically();

                                    if (didWork)
                                        ResetErrors();

                                    if (_logger.IsInfoEnabled)
                                        _logger.Info($"Finished indexing for '{Name} ({Etag})'.'");
                                    _hadRealIndexingWorkToDo |= didWork;

                                    if (_logger.IsInfoEnabled)
                                        _logger.Info($"Finished indexing for '{Name} ({Etag})'.'");

                                    if (ShouldReplace())
                                    {
                                        var originalName = Name.Replace(Constants.Documents.Indexing.SideBySideIndexNamePrefix, string.Empty);

                                        // this can fail if the indexes lock is currently held, so we'll retry
                                        // however, we might be requested to shutdown, so we want to skip replacing
                                        // in this case, worst case scenario we'll handle this in the next batch
                                        while (_indexingProcessCancellationTokenSource.IsCancellationRequested == false)
                                        {
                                            if (DocumentDatabase.IndexStore.TryReplaceIndexes(originalName, Definition.Name))
                                                break;
                                        }
                                    }
                                }
                                catch (OutOfMemoryException oome)
                                {
                                    if (_logger.IsInfoEnabled)
                                        _logger.Info($"Out of memory occurred for '{Name} ({Etag})'.", oome);
                                    // TODO [ppekrol] GC?

                                    scope.AddMemoryError(oome);
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
                                catch (OperationCanceledException)
                                {
                                    // We are here only in the case of indexing process cancellation.
                                    scope.RecordMapCompletedReason("Operation canceled.");
                                    return;
                                }
                                catch (Exception e)
                                {
                                    if (_logger.IsOperationsEnabled)
                                        _logger.Operations($"Critical exception occurred for '{Name} ({Etag})'.", e);

                                    HandleUnexpectedErrors(scope, e);
                                }

                                try
                                {
                                    var failureInformation = _indexStorage.UpdateStats(stats.StartTime, stats.ToIndexingBatchStats());
                                    HandleIndexFailureInformation(failureInformation);
                                }
                                catch (VoronUnrecoverableErrorException vuee)
                                {
                                    HandleIndexCorruption(scope, vuee);
                                }
                                catch (Exception e)
                                {
                                    if (_logger.IsInfoEnabled)
                                        _logger.Info($"Could not update stats for '{Name} ({Etag})'.", e);
                                }
                            }
                        }
                        finally
                        {
                            stats.Complete();
                        }

                        if (batchCompleted)
                        {
                            DocumentDatabase.Changes.RaiseNotifications(new IndexChange { Name = Name, Type = IndexChangeTypes.BatchCompleted });
                        }

                        try
                        {
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
                                    new Size(NativeMemory.ThreadAllocations.Value.Allocations, SizeUnit.Bytes));
                            }

                            if (_mre.Wait(timeToWaitForMemoryCleanup, _indexingProcessCancellationTokenSource.Token) == false)
                            {
                                _allocationCleanupNeeded = false;

                                // there is no work to be done, and hasn't been for a while,
                                // so this is a good time to release resources we won't need 
                                // anytime soon
                                ReduceMemoryUsage();

                                var numberOfSetEvents =
                                    WaitHandle.WaitAny(new[]
                                        {_mre.WaitHandle, _logsAppliedEvent.WaitHandle, _indexingProcessCancellationTokenSource.Token.WaitHandle});

                                if (numberOfSetEvents == 1 && _logsAppliedEvent.IsSet)
                                {
                                    _hadRealIndexingWorkToDo = false;
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
                finally
                {
                    storageEnvironment.OnLogsApplied -= HandleLogsApplied;
                    if (DocumentDatabase != null)
                        DocumentDatabase.Changes.OnDocumentChange -= HandleDocumentChange;
                }
            }
        }

        protected virtual bool ShouldReplace()
        {
            return false;
        }

        private void ChangeIndexThreadPriorityIfNeeded()
        {
            if (_priorityChanged == false)
                return;

            _priorityChanged = false;

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

        private void HandleLogsApplied()
        {
            if (_hadRealIndexingWorkToDo)
                _logsAppliedEvent.Set();
        }

        private void ReduceMemoryUsage()
        {
            var beforeFree = NativeMemory.ThreadAllocations.Value.Allocations;
            if (_logger.IsInfoEnabled)
                _logger.Info(
                    $"{beforeFree / 1024:#,#} kb is used by '{Name} ({Etag})', reducing memory utilization.");

            DocumentDatabase.DocumentsStorage.ContextPool.Clean();
            _contextPool.Clean();
            ByteStringMemoryCache.CleanForCurrentThread();
            IndexPersistence.Clean();
            _currentMaximumAllowedMemory = DefaultMaximumMemoryAllocation;


            var afterFree = NativeMemory.ThreadAllocations.Value.Allocations;
            if (_logger.IsInfoEnabled)
                _logger.Info($"After cleanup, using {afterFree / 1024:#,#} Kb by '{Name} ({Etag})'.");
        }

        internal void ResetErrors()
        {
            Interlocked.Exchange(ref _writeErrors, 0);
            Interlocked.Exchange(ref _unexpectedErrors, 0);
            Interlocked.Exchange(ref _analyzerErrors, 0);
        }

        internal void HandleAnalyzerErrors(IndexingStatsScope stats, IndexAnalyzerException iae)
        {
            stats.AddAnalyzerError(iae);

            var analyzerErrors = Interlocked.Increment(ref _analyzerErrors);

            if (State == IndexState.Error || analyzerErrors < AnalyzerErrorLimit)
                return;

            // TODO we should create notification here?
            _errorStateReason = $"State was changed due to excessive number of analyzer errors ({analyzerErrors}).";
            SetState(IndexState.Error);
        }

        internal void HandleUnexpectedErrors(IndexingStatsScope stats, Exception e)
        {
            stats.AddUnexpectedError(e);

            var unexpectedErrors = Interlocked.Increment(ref _unexpectedErrors);

            if (State == IndexState.Error || unexpectedErrors < UnexpectedErrorsLimit)
                return;

            // TODO we should create notification here?
            _errorStateReason = $"State was changed due to excessive number of unexpected errors ({unexpectedErrors}).";
            SetState(IndexState.Error);
        }

        internal void HandleCriticalErrors(IndexingStatsScope stats, CriticalIndexingException e)
        {
            if (State == IndexState.Error)
                return;

            // TODO we should create notification here?
            _errorStateReason = "State was changed due to a critical error.";
            SetState(IndexState.Error);
        }

        internal void HandleWriteErrors(IndexingStatsScope stats, IndexWriteException iwe)
        {
            stats.AddWriteError(iwe);

            if (iwe.InnerException is SystemException) // Don't count transient errors
                return;

            var writeErrors = Interlocked.Increment(ref _writeErrors);

            if (State == IndexState.Error || writeErrors < WriteErrorsLimit)
                return;

            // TODO we should create notification here?
            _errorStateReason = $"State was changed due to excessive number of write errors ({writeErrors}).";
            SetState(IndexState.Error);
        }

        private void HandleIndexCorruption(IndexingStatsScope stats, Exception e)
        {
            stats.AddCorruptionError(e);

            if (_logger.IsOperationsEnabled)
                _logger.Operations($"Data corruption occurred for '{Name}' ({Etag}).", e);

            // TODO we should create notification here?

            _errorStateReason = $"State was changed due to data corruption with message '{e.Message}'";
            try
            {
                SetState(IndexState.Error);
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

            // TODO we should create notification here?

            _errorStateReason = message;
            SetState(IndexState.Error);
        }

        public void HandleError(Exception e)
        {
            var ide = e as VoronUnrecoverableErrorException;
            if (ide == null)
                return;

            throw new IndexCorruptionException(e);
        }

        protected abstract IIndexingWork[] CreateIndexWorkExecutors();

        public virtual IDisposable InitializeIndexingWork(TransactionOperationContext indexContext)
        {
            return null;
        }

        public bool DoIndexingWork(IndexingStatsScope stats, CancellationToken cancellationToken)
        {
            _threadAllocations = NativeMemory.ThreadAllocations.Value;

            bool mightBeMore = false;
            using (CultureHelper.EnsureInvariantCulture())
            using (DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext databaseContext))
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
            {
                indexContext.PersistentContext.LongLivedTransactions = true;
                databaseContext.PersistentContext.LongLivedTransactions = true;

                using (var tx = indexContext.OpenWriteTransaction())
                using (CurrentIndexingScope.Current = new CurrentIndexingScope(DocumentDatabase.DocumentsStorage, databaseContext, indexContext))
                {
                    var writeOperation = new Lazy<IndexWriteOperation>(() => IndexPersistence.OpenIndexWriter(indexContext.Transaction.InnerTransaction));

                    using (InitializeIndexingWork(indexContext))
                    {
                        try
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
                        }
                        finally
                        {
                            if (writeOperation.IsValueCreated)
                            {
                                using (var indexWriteOperation = writeOperation.Value)
                                {
                                    indexWriteOperation.Commit(stats);
                                }
                            }
                        }

                        _indexStorage.WriteReferences(CurrentIndexingScope.Current, tx);
                    }

                    using (stats.For(IndexingOperation.Storage.Commit))
                    {
                        tx.InnerTransaction.LowLevelTransaction.RetrieveCommitStats(out CommitStats commitStats);

                        tx.InnerTransaction.LowLevelTransaction.AfterCommitWhenNewReadTransactionsPrevented += () =>
                        {
                            if (writeOperation.IsValueCreated)
                            {
                                using (stats.For(IndexingOperation.Lucene.RecreateSearcher))
                                {
                                    // we need to recreate it after transaction commit to prevent it from seeing uncommitted changes
                                    // also we need this to be called when new read transaction are prevented in order to ensure
                                    // that queries won't get the searcher having 'old' state but see 'new' changes committed here
                                    // e.g. the old searcher could have a segment file in its in-memory state which has been removed in this tx
                                    IndexPersistence.RecreateSearcher(tx.InnerTransaction);
                                }
                            }
                        };

                        tx.Commit();

                        stats.RecordCommitStats(commitStats.NumberOfModifiedPages, commitStats.NumberOf4KbsWrittenToDisk);
                    }

                    return mightBeMore;
                }
            }
        }

        public abstract IIndexedDocumentsEnumerator GetMapEnumerator(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext, IndexingStatsScope stats);

        public abstract void HandleDelete(DocumentTombstone tombstone, string collection, IndexWriteOperation writer,
            TransactionOperationContext indexContext, IndexingStatsScope stats);

        public abstract int HandleMap(LazyStringValue key, IEnumerable mapResults, IndexWriteOperation writer,
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
            if (_isCompactionInProgress)
                return new List<IndexingError>();

            return _indexStorage.ReadErrors();
        }

        public long GetErrorCount()
        {
            if (_isCompactionInProgress)
                return 0;

            if (Type == IndexType.Faulty)
                return 1;

            return _indexStorage.ReadErrorsCount();
        }

        public DateTime? GetLastIndexingErrorTime()
        {
            if (_isCompactionInProgress || Type == IndexType.Faulty)
                return DateTime.MinValue;

            return _indexStorage.ReadLastIndexingErrorTime();
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
                    _logger.Info($"Changing priority for '{Name} ({Etag})' from '{Definition.Priority}' to '{priority}'.");

                _indexStorage.WritePriority(priority);

                Definition.Priority = priority;
                _priorityChanged = true;

                DocumentDatabase.Changes.RaiseNotifications(new IndexChange
                {
                    Name = Name,
                    Type = IndexChangeTypes.PriorityChanged
                });
            }
        }

        public virtual void SetState(IndexState state)
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
                    _logger.Info($"Changing state for '{Name} ({Etag})' from '{State}' to '{state}'.");


                var oldState = State;
                State = state;
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

            using (DrainRunningQueries())
            {
                AssertIndexState(assertState: false);

                if (Definition.LockMode == mode)
                    return;

                if (_logger.IsInfoEnabled)
                    _logger.Info(
                        $"Changing lock mode for '{Name} ({Etag})' from '{Definition.LockMode}' to '{mode}'.");

                _indexStorage.WriteLock(mode);

                DocumentDatabase.Changes.RaiseNotifications(new IndexChange
                {
                    Name = Name,
                    Type = IndexChangeTypes.LockModeChanged
                });
            }
        }

        public virtual void Enable()
        {
            if (State != IndexState.Disabled)
                return;

            using (DrainRunningQueries())
            {
                if (State != IndexState.Disabled)
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

                Stop();
                SetState(IndexState.Disabled);
            }
        }

        public void Rename(string name)
        {
            _indexStorage.Rename(name);
        }

        public virtual IndexProgress GetProgress(DocumentsOperationContext documentsContext)
        {
            if (_isCompactionInProgress)
            {
                return new IndexProgress
                {
                    Name = Name,
                    Etag = Etag,
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
                    Etag = Etag,
                    Name = Name,
                    Type = Type
                };

                progress.IsStale = IsStale(documentsContext, context);

                var stats = _indexStorage.ReadStats(tx);

                progress.Collections = new Dictionary<string, IndexProgress.CollectionStats>();
                foreach (var collection in Collections)
                {
                    var collectionStats = stats.Collections[collection];

                    var progressStats = progress.Collections[collection] = new IndexProgress.CollectionStats
                    {
                        LastProcessedDocumentEtag = collectionStats.LastProcessedDocumentEtag,
                        LastProcessedTombstoneEtag = collectionStats.LastProcessedTombstoneEtag
                    };

                    progressStats.NumberOfDocumentsToProcess = DocumentDatabase.DocumentsStorage.GetNumberOfDocumentsToProcess(documentsContext,
                        collection, progressStats.LastProcessedDocumentEtag, out long totalCount);
                    progressStats.TotalNumberOfDocuments = totalCount;

                    progressStats.NumberOfTombstonesToProcess =
                        DocumentDatabase.DocumentsStorage.GetNumberOfTombstonesToProcess(documentsContext, collection,
                            progressStats.LastProcessedTombstoneEtag, out totalCount);
                    progressStats.TotalNumberOfTombstones = totalCount;
                }

                return progress;
            }
        }

        public virtual IndexStats GetStats(bool calculateLag = false, bool calculateStaleness = false,
            DocumentsOperationContext documentsContext = null)
        {
            if (_isCompactionInProgress)
            {
                return new IndexStats
                {
                    Name = Name,
                    Etag = Etag,
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

                stats.Etag = Etag;
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

        private IndexStats.MemoryStats GetMemoryStats()
        {
            var stats = new IndexStats.MemoryStats();

            var name = IndexDefinitionBase.GetIndexNameSafeForFileSystem(Name);

            var indexPath = Configuration.StoragePath.Combine(name);

            var indexTempPath = Configuration.TempPath?.Combine(name);

            var journalPath = Configuration.JournalsStoragePath?.Combine(name);

            var totalSize = 0L;
            foreach (var mapping in NativeMemory.FileMapping)
            {
                var directory = Path.GetDirectoryName(mapping.Key);

                var isIndexPath = string.Equals(indexPath.FullPath, directory, StringComparison.OrdinalIgnoreCase);
                var isTempPath = indexTempPath != null && string.Equals(indexTempPath.FullPath, directory, StringComparison.OrdinalIgnoreCase);
                var isJournalPath = journalPath != null && string.Equals(journalPath.FullPath, directory, StringComparison.OrdinalIgnoreCase);

                if (isIndexPath || isTempPath || isJournalPath)
                {
                    foreach (var singleMapping in mapping.Value)
                        totalSize += singleMapping.Value;
                }
            }

            stats.DiskSize.SizeInBytes = totalSize;

            var indexingThread = _indexingThread;
            if (indexingThread != null)
            {
                foreach (var threadAllocationsValue in NativeMemory.ThreadAllocations.Values)
                {
                    if (indexingThread.ManagedThreadId == threadAllocationsValue.Id)
                    {
                        stats.ThreadAllocations.SizeInBytes = threadAllocationsValue.Allocations;
                        if (stats.ThreadAllocations.SizeInBytes < 0)
                            stats.ThreadAllocations.SizeInBytes = 0;
                        stats.MemoryBudget.SizeInBytes = _currentMaximumAllowedMemory.GetValue(SizeUnit.Bytes);
                        break;
                    }
                }
            }

            return stats;
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
            return Definition.ConvertToIndexDefinition(this);
        }

        public virtual async Task StreamQuery(HttpResponse response, BlittableJsonTextWriter writer,
            IndexQueryServerSide query, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            using (var result = new StreamDocumentQueryResult(response, writer, documentsContext))
            {
                await QueryInternal(result, query, documentsContext, token);
            }

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
                SetState(IndexState.Normal);

            MarkQueried(DocumentDatabase.Time.GetUtcNow());

            AssertQueryDoesNotContainFieldsThatAreNotIndexed(query.Metadata);


            if (resultToFill.SupportsInclude == false
                && (query.Metadata.Includes != null && query.Metadata.Includes.Length > 0))
                throw new NotSupportedException("Includes are not supported by this type of query.");

            using (var marker = MarkQueryAsRunning(query, token))

            {
                var queryDuration = Stopwatch.StartNew();
                AsyncWaitForIndexing wait = null;

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

                        if (query.WaitForNonStaleResultsAsOfNow && query.CutoffEtag == null)
                        {
                            query.CutoffEtag = 0;
                            foreach (var collection in Collections)
                            {
                                var etag = GetLastDocumentEtagInCollection(documentsContext, collection);

                                if (etag > query.CutoffEtag)
                                    query.CutoffEtag = etag;
                            }
                        }

                        var isStale = IsStale(documentsContext, indexContext, query.CutoffEtag);
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

                        FillQueryResult(resultToFill, isStale, documentsContext, indexContext);

                        if (Type.IsMapReduce() && (query.Metadata.Includes == null || query.Metadata.Includes.Length == 0))
                            documentsContext.CloseTransaction();
                        // map reduce don't need to access mapResults storage unless we have a transformer. Possible optimization: if we will know if transformer needs transaction then we may reset this here or not

                        using (var reader = IndexPersistence.OpenIndexReader(indexTx.InnerTransaction))
                        {
                            var totalResults = new Reference<int>();
                            var skippedResults = new Reference<int>();

                            var fieldsToFetch = new FieldsToFetch(query, Definition);
                            IEnumerable<Document> documents;

                            if (query.IsIntersect == false)
                            {
                                documents = reader.Query(query, fieldsToFetch, totalResults, skippedResults,
                                    GetQueryResultRetriever(query, documentsContext, fieldsToFetch), documentsContext, token.Token);
                            }
                            else
                            {
                                documents = reader.IntersectQuery(query, fieldsToFetch, totalResults, skippedResults,
                                    GetQueryResultRetriever(query, documentsContext, fieldsToFetch), documentsContext, token.Token);
                            }

                            var includeDocumentsCommand = new IncludeDocumentsCommand(
                                DocumentDatabase.DocumentsStorage, documentsContext, query.Metadata.Includes);

                            try
                            {
                                foreach (var document in documents)
                                {
                                    resultToFill.TotalResults = totalResults.Value;
                                    resultToFill.AddResult(document);

                                    includeDocumentsCommand.Gather(document);
                                }
                            }
                            catch (Exception e)
                            {
                                if (resultToFill.SupportsExceptionHandling == false)
                                    throw;

                                resultToFill.HandleException(e);
                            }

                            includeDocumentsCommand.Fill(resultToFill.Includes);
                            resultToFill.TotalResults = totalResults.Value;
                            resultToFill.SkippedResults = skippedResults.Value;
                            resultToFill.IncludedPaths = query.Metadata.Includes;
                        }

                        return;
                    }
                }
            }
        }

        public virtual async Task<FacetedQueryResult> FacetedQuery(FacetQueryServerSide query, long facetSetupEtag,
            DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            AssertIndexState();

            if (State == IndexState.Idle)
                SetState(IndexState.Normal);

            MarkQueried(DocumentDatabase.Time.GetUtcNow());
            AssertQueryDoesNotContainFieldsThatAreNotIndexed(query.Metadata);

            using (var marker = MarkQueryAsRunning(query, token))
            {
                var result = new FacetedQueryResult();

                var queryDuration = Stopwatch.StartNew();
                AsyncWaitForIndexing wait = null;

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

                            if (query.WaitForNonStaleResultsAsOfNow && query.CutoffEtag == null)
                                query.CutoffEtag =
                                    Collections.Max(
                                        x => DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(documentsContext, x));

                            var isStale = IsStale(documentsContext, indexContext, query.CutoffEtag);


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


                            FillFacetedQueryResult(result, IsStale(documentsContext, indexContext), facetSetupEtag,
                                documentsContext, indexContext);

                            documentsContext.CloseTransaction();

                            using (var reader = IndexPersistence.OpenFacetedIndexReader(indexTx.InnerTransaction))
                            {
                                result.Results = reader.FacetedQuery(query, indexContext, token.Token);
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
                        CalculateIndexEtag(IsStale(documentsContext, indexContext), documentsContext, indexContext)
                };

                using (var reader = IndexPersistence.OpenIndexReader(tx.InnerTransaction))
                {
                    result.Terms = reader.Terms(field, fromValue, pageSize, token.Token);
                }

                return result;
            }
        }

        public virtual SuggestionQueryResultServerSide SuggestionsQuery(SuggestionQueryServerSide query, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            AssertIndexState();

            using (var marker = MarkQueryAsRunning(query, token))
            {
                AssertIndexState();
                marker.HoldLock();

                using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
                using (var tx = indexContext.OpenReadTransaction())
                {
                    var result = new SuggestionQueryResultServerSide();

                    var isStale = IsStale(documentsContext, indexContext);

                    FillSuggestionQueryResult(result, isStale, documentsContext, indexContext);

                    using (var reader = IndexPersistence.OpenSuggestionIndexReader(tx.InnerTransaction, query.Field))
                    {
                        result.Suggestions = reader.Suggestions(query, token.Token);
                    }

                    return result;
                }
            }
        }

        public virtual MoreLikeThisQueryResultServerSide MoreLikeThisQuery(MoreLikeThisQueryServerSide query, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            AssertIndexState();

            HashSet<string> stopWords = null;
            if (string.IsNullOrWhiteSpace(query.StopWordsDocumentId) == false)
            {
                var stopWordsDoc = DocumentDatabase.DocumentsStorage.Get(documentsContext, query.StopWordsDocumentId);
                if (stopWordsDoc == null)
                    throw new InvalidOperationException("Stop words document " + query.StopWordsDocumentId +
                                                        " could not be found");

                if (stopWordsDoc.Data.TryGet(nameof(StopWordsSetup.StopWords), out BlittableJsonReaderArray value) && value != null)
                {
                    stopWords = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    for (var i = 0; i < value.Length; i++)
                        stopWords.Add(value.GetStringByIndex(i));
                }
            }

            using (var marker = MarkQueryAsRunning(query, token))
            {
                AssertIndexState();
                marker.HoldLock();

                using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
                using (var tx = indexContext.OpenReadTransaction())
                {

                    var result = new MoreLikeThisQueryResultServerSide();

                    var isStale = IsStale(documentsContext, indexContext);

                    FillQueryResult(result, isStale, documentsContext, indexContext);

                    if (Type.IsMapReduce() && (query.Includes == null || query.Includes.Length == 0))
                        documentsContext.CloseTransaction();
                    // map reduce don't need to access mapResults storage unless we have a transformer. Possible optimization: if we will know if transformer needs transaction then we may reset this here or not

                    using (var reader = IndexPersistence.OpenIndexReader(tx.InnerTransaction))
                    {
                        var includeDocumentsCommand = new IncludeDocumentsCommand(DocumentDatabase.DocumentsStorage,
                            documentsContext, query.Includes);

                        var documents = reader.MoreLikeThis(query, stopWords,
                            fieldsToFetch =>
                                GetQueryResultRetriever(null, documentsContext,
                                    new FieldsToFetch(fieldsToFetch, Definition)), documentsContext, token.Token);

                        foreach (var document in documents)
                        {
                            result.Results.Add(document);
                            includeDocumentsCommand.Gather(document);
                        }

                        includeDocumentsCommand.Fill(result.Includes);
                    }

                    return result;
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
                    var isStale = IsStale(documentsContext, indexContext, query.CutoffEtag);
                    FillQueryResult(result, isStale, documentsContext, indexContext);
                }

                var totalResults = new Reference<int>();
                foreach (var indexEntry in reader.IndexEntries(query, totalResults, documentsContext, token.Token))
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

            if (_isCompactionInProgress)
                ThrowCompactionInProgress();

            if (_initialized == false)
                ThrowNotIntialized();

            if (_disposed || _disposing)
                ThrowWasDisposed();

            if (assertState && State == IndexState.Error)
            {
                var errorStateReason = _errorStateReason;
                if (string.IsNullOrWhiteSpace(errorStateReason) == false)
                    ThrowMarkedAsError(errorStateReason);

                ThrowErrored();
            }
        }

        private void ThrowErrored()
        {
            throw new InvalidOperationException(
                $"Index '{Name} ({Etag})' is marked as errored. Please check index errors avaiable at '/databases/{DocumentDatabase.Name}/indexes/errors?name={Name}'.");
        }

        private void ThrowMarkedAsError(string errorStateReason)
        {
            throw new InvalidOperationException($"Index '{Name} ({Etag})' is marked as errored. {errorStateReason}");
        }

        private void ThrowWasDisposed()
        {
            throw new ObjectDisposedException($"Index '{Name} ({Etag})' was already disposed.");
        }

        private void ThrowNotIntialized()
        {
            throw new InvalidOperationException($"Index '{Name} ({Etag})' was not initialized.");
        }

        private void ThrowCompactionInProgress()
        {
            throw new InvalidOperationException($"Index '{Name} ({Etag})' is currently being compacted.");
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

                    if (f.StartsWith(Constants.Documents.Indexing.Fields.CustomSortFieldName))
                        continue;

                    AssertKnownField(f);
                }
            }
        }

        private void AssertKnownField(string f)
        {
            // the catch all field name means that we have dynamic fields names

            if (IndexPersistence.ContainsField(f) || 
                IndexPersistence.ContainsField("_"))
                return;

            ThrowInvalidField(f);
        }

        private static void ThrowInvalidField(string f)
        {
            throw new ArgumentException($"The field '{f}' is not indexed, cannot query/sort on fields that are not indexed");
        }

        private void FillFacetedQueryResult(FacetedQueryResult result, bool isStale, long facetSetupEtag,
            DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {
            result.IndexName = Name;
            result.IsStale = isStale;
            result.IndexTimestamp = LastIndexingTime ?? DateTime.MinValue;
            result.LastQueryTime = _lastQueryingTime ?? DateTime.MinValue;
            result.ResultEtag = CalculateIndexEtag(result.IsStale, documentsContext, indexContext) ^ facetSetupEtag;
        }

        private void FillSuggestionQueryResult(SuggestionQueryResult result, bool isStale,
            DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {
            result.IndexName = Name;
            result.IsStale = isStale;
            result.IndexTimestamp = LastIndexingTime ?? DateTime.MinValue;
            result.LastQueryTime = _lastQueryingTime ?? DateTime.MinValue;
            result.ResultEtag = CalculateIndexEtag(result.IsStale, documentsContext, indexContext);
        }

        private void FillQueryResult<T>(QueryResultBase<T> result, bool isStale,
            DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {
            result.IndexName = Name;
            result.IsStale = isStale;
            result.IndexTimestamp = LastIndexingTime ?? DateTime.MinValue;
            result.LastQueryTime = _lastQueryingTime ?? DateTime.MinValue;
            result.ResultEtag = CalculateIndexEtag(result.IsStale, documentsContext, indexContext);
        }

        private QueryDoneRunning MarkQueryAsRunning(IIndexQuery query, OperationCancelToken token)
        {
            var queryStartTime = DateTime.UtcNow;
            var queryId = Interlocked.Increment(ref _numberOfQueries);
            var executingQueryInfo = new ExecutingQueryInfo(queryStartTime, query, queryId, token);
            CurrentlyRunningQueries.Add(executingQueryInfo);

            return new QueryDoneRunning(this, executingQueryInfo);
        }

        private static bool WillResultBeAcceptable(bool isStale, IndexQueryBase<BlittableJsonReaderObject> query, AsyncWaitForIndexing wait)
        {
            if (isStale == false)
                return true;

            if (query.WaitForNonStaleResultsTimeout == null)
                return true;

            if (wait != null && wait.TimeoutExceeded)
                return true;

            return false;
        }

        protected virtual unsafe long CalculateIndexEtag(bool isStale, DocumentsOperationContext documentsContext,
            TransactionOperationContext indexContext)
        {
            var length = MinimumSizeForCalculateIndexEtagLength();

            var indexEtagBytes = stackalloc byte[length];

            CalculateIndexEtagInternal(indexEtagBytes, isStale, documentsContext, indexContext);

            unchecked
            {
                return (long)Hashing.XXHash64.Calculate(indexEtagBytes, (ulong)length);
            }
        }

        protected int MinimumSizeForCalculateIndexEtagLength()
        {
            var length = sizeof(long) * 4 * Collections.Count + // last document etag, last tombstone etag and last mapped etags per collection
                         sizeof(int) + // definition hash
                         1; // isStale
            return length;
        }

        protected unsafe void CalculateIndexEtagInternal(byte* indexEtagBytes, bool isStale,
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
        }

        public long GetIndexEtag()
        {
            if (_isCompactionInProgress)
                return -1;

            using (DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
            {
                using (indexContext.OpenReadTransaction())
                using (documentsContext.OpenReadTransaction())
                {
                    return CalculateIndexEtag(IsStale(documentsContext, indexContext), documentsContext, indexContext);
                }
            }
        }

        public virtual Dictionary<string, long> GetLastProcessedDocumentTombstonesPerCollection()
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (var tx = context.OpenReadTransaction())
                {
                    return GetLastProcessedDocumentTombstonesPerCollection(tx);
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

        public abstract IQueryResultRetriever GetQueryResultRetriever(IndexQueryServerSide query, DocumentsOperationContext documentsContext, FieldsToFetch fieldsToFetch);

        protected void HandleIndexOutputsPerDocument(string documentKey, int numberOfOutputs, IndexingStatsScope stats)
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
                _indexOutputsPerDocumentWarning.SampleDocumentId = documentKey;
            }

            if (_indexOutputsPerDocumentWarning.LastWarnedAt != null &&
                (SystemTime.UtcNow - _indexOutputsPerDocumentWarning.LastWarnedAt.Value).Minutes <= 5)
            {
                // save the hint every 5 minutes (at worst case)
                return;
            }

            _indexOutputsPerDocumentWarning.LastWarnedAt = SystemTime.UtcNow;

            var hint = PerformanceHint.Create("High indexing fanout ratio",
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

        public bool CanContinueBatch(
            IndexingStatsScope stats,
            DocumentsOperationContext documentsOperationContext,
            TransactionOperationContext indexingContext)
        {
            stats.RecordMapAllocations(_threadAllocations.Allocations);

            if (stats.ErrorsCount >= IndexStorage.MaxNumberOfKeptErrors)
            {
                stats.RecordMapCompletedReason($"Number of errors ({stats.ErrorsCount}) reached maximum number of allowed errors per batch ({IndexStorage.MaxNumberOfKeptErrors})");
                return false;
            }

            if (sizeof(int) == IntPtr.Size || DocumentDatabase.Configuration.Storage.ForceUsing32BitsPager)
            {
                IPagerLevelTransactionState pagerLevelTransactionState = documentsOperationContext.Transaction?.InnerTransaction?.LowLevelTransaction;
                var total32BitsMappedSize = pagerLevelTransactionState?.GetTotal32BitsMappedSize();
                if (total32BitsMappedSize > 8 * Voron.Global.Constants.Size.Megabyte)
                {
                    stats.RecordMapCompletedReason($"Running in 32 bits and have {total32BitsMappedSize / 1024:#,#} kb mapped in docs ctx");
                    return false;
                }

                pagerLevelTransactionState = indexingContext.Transaction?.InnerTransaction?.LowLevelTransaction;
                total32BitsMappedSize = pagerLevelTransactionState?.GetTotal32BitsMappedSize();
                if (total32BitsMappedSize > 8 * Voron.Global.Constants.Size.Megabyte)
                {
                    stats.RecordMapCompletedReason($"Running in 32 bits and have {total32BitsMappedSize / 1024:#,#} kb mapped in index ctx");
                    return false;
                }
            }

            var currentBudget = _currentMaximumAllowedMemory.GetValue(SizeUnit.Bytes);
            if (_threadAllocations.Allocations > currentBudget)
            {
                var canContinue = true;

                if (MemoryUsageGuard.TryIncreasingMemoryUsageForThread(_threadAllocations, ref _currentMaximumAllowedMemory,
                        _environment.Options.RunningOn32Bits, _logger, out ProcessMemoryUsage memoryUsage) == false)
                {
                    _allocationCleanupNeeded = true;

                    if (stats.MapAttempts >= Configuration.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)
                    {
                        stats.RecordMapCompletedReason("Cannot budget additional memory for batch");
                        canContinue = false;
                    }
                }

                if (memoryUsage != null)
                    stats.RecordMapMemoryStats(memoryUsage.WorkingSet, memoryUsage.PrivateMemory, currentBudget);

                return canContinue;
            }

            return true;
        }

        public IOperationResult Compact(Action<IOperationProgress> onProgress)
        {
            if (_isCompactionInProgress)
                throw new InvalidOperationException($"Index '{Name} ({Etag})' cannot be compacted because compaction is already in progress.");
            var progress = new IndexCompactionProgress
            {
                Message = "Draining queries for " + Name
            };
            onProgress?.Invoke(progress);

            using (DrainRunningQueries())
            {
                if (_environment.Options.IncrementalBackupEnabled)
                    throw new InvalidOperationException(
                        $"Index '{Name} ({Etag})' cannot be compacted because incremental backup is enabled.");

                if (Configuration.RunInMemory)
                    throw new InvalidOperationException(
                        $"Index '{Name} ({Etag})' cannot be compacted because it runs in memory.");

                _isCompactionInProgress = true;
                progress.Message = null;

                PathSetting compactPath = null;

                try
                {
                    var environmentOptions =
                        (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)_environment.Options;
                    var srcOptions = StorageEnvironmentOptions.ForPath(environmentOptions.BasePath.FullPath, null, null, DocumentDatabase.IoChanges,
                        DocumentDatabase.CatastrophicFailureNotification);
                    srcOptions.ForceUsing32BitsPager = DocumentDatabase.Configuration.Storage.ForceUsing32BitsPager;
                    srcOptions.OnNonDurableFileSystemError += DocumentDatabase.HandleNonDurableFileSystemError;
                    srcOptions.OnRecoveryError += DocumentDatabase.HandleOnRecoveryError;
                    srcOptions.CompressTxAboveSizeInBytes = DocumentDatabase.Configuration.Storage.CompressTxAboveSize.GetValue(SizeUnit.Bytes);
                    srcOptions.TimeToSyncAfterFlashInSec = (int)DocumentDatabase.Configuration.Storage.TimeToSyncAfterFlash.AsTimeSpan.TotalSeconds;
                    srcOptions.NumOfConcurrentSyncsPerPhysDrive = DocumentDatabase.Configuration.Storage.NumberOfConcurrentSyncsPerPhysicalDrive;
                    Sodium.CloneKey(out srcOptions.MasterKey, DocumentDatabase.MasterKey);

                    var wasRunning = _indexingThread != null;

                    Dispose();

                    compactPath = Configuration.StoragePath.Combine(IndexDefinitionBase.GetIndexNameSafeForFileSystem(Name) + "_Compact");

                    using (var compactOptions = (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)
                        StorageEnvironmentOptions.ForPath(compactPath.FullPath, null, null, DocumentDatabase.IoChanges, DocumentDatabase.CatastrophicFailureNotification))
                    {
                        compactOptions.OnNonDurableFileSystemError += DocumentDatabase.HandleNonDurableFileSystemError;
                        compactOptions.OnRecoveryError += DocumentDatabase.HandleOnRecoveryError;
                        compactOptions.CompressTxAboveSizeInBytes = DocumentDatabase.Configuration.Storage.CompressTxAboveSize.GetValue(SizeUnit.Bytes);
                        compactOptions.ForceUsing32BitsPager = DocumentDatabase.Configuration.Storage.ForceUsing32BitsPager;
                        compactOptions.TimeToSyncAfterFlashInSec = (int)DocumentDatabase.Configuration.Storage.TimeToSyncAfterFlash.AsTimeSpan.TotalSeconds;
                        compactOptions.NumOfConcurrentSyncsPerPhysDrive = DocumentDatabase.Configuration.Storage.NumberOfConcurrentSyncsPerPhysicalDrive;
                        Sodium.CloneKey(out srcOptions.MasterKey, DocumentDatabase.MasterKey);

                        StorageCompaction.Execute(srcOptions, compactOptions, progressReport =>
                        {
                            progress.Processed = progressReport.GlobalProgress;
                            progress.Total = progressReport.GlobalTotal;

                            onProgress?.Invoke(progress);
                        });
                    }

                    IOExtensions.DeleteDirectory(environmentOptions.BasePath.FullPath);
                    IOExtensions.MoveDirectory(compactPath.FullPath, environmentOptions.BasePath.FullPath);

                    _initialized = false;
                    _disposed = false;

                    Initialize(DocumentDatabase, Configuration, DocumentDatabase.Configuration.PerformanceHints);

                    if (wasRunning)
                        Start();

                    return IndexCompactionResult.Instance;
                }
                finally
                {
                    if (compactPath != null)
                        IOExtensions.DeleteDirectory(compactPath.FullPath);

                    _isCompactionInProgress = false;
                }
            }
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

        private struct QueryDoneRunning : IDisposable
        {
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
                if (_parent._currentlyRunningQueriesLock.TryEnterReadLock(TimeSpan.FromSeconds(3)) == false)
                    ThrowLockTimeoutException();

                _hasLock = true;
            }

            private void ThrowLockTimeoutException()
            {
                throw new TimeoutException($"Could not get the index read lock in a reasonable time, {_parent.Name} is probably undergoing maintenance now, try again later");
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
                _parent.CurrentlyRunningQueries.TryRemove(_queryInfo);
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
            _batchProcessCancellationTokenSource?.Cancel();
        }

        /// <summary>
        /// We don't need to do anything when the low memory problem is over,
        /// the class will automatically raise memory usage.
        /// </summary>
        public void LowMemoryOver()
        {
        }
    }
}
