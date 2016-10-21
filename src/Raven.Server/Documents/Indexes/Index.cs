using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Client.Data.Queries;
using Raven.Client.Indexing;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.Documents.Queries.Parse;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Sorting;
using Raven.Server.Documents.Transformers;
using Raven.Server.Exceptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.LowMemoryNotification;
using Raven.Server.Utils;
using Raven.Server.Utils.Metrics;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Json;
using Voron;
using Sparrow.Logging;
using Sparrow.Utils;
using Size = Raven.Server.Config.Settings.Size;
using Voron.Debugging;

namespace Raven.Server.Documents.Indexes
{
    public abstract class Index<TIndexDefinition> : Index
        where TIndexDefinition : IndexDefinitionBase
    {
        public new TIndexDefinition Definition => (TIndexDefinition)base.Definition;

        protected Index(int indexId, IndexType type, TIndexDefinition definition)
            : base(indexId, type, definition)
        {
        }
    }

    public abstract class Index : IDocumentTombstoneAware, IDisposable
    {
        private long _writeErrors;

        private const long WriteErrorsLimit = 10;

        protected Logger _logger;

        internal readonly LuceneIndexPersistence IndexPersistence;

        private readonly object _locker = new object();

        private readonly AsyncManualResetEvent _indexingBatchCompleted = new AsyncManualResetEvent();

        private CancellationTokenSource _cancellationTokenSource;

        protected DocumentDatabase DocumentDatabase;

        private Thread _indexingThread;

        private bool _initialized;

        protected UnmanagedBuffersPoolWithLowMemoryHandling _unmanagedBuffersPool;

        private StorageEnvironment _environment;

        internal TransactionContextPool _contextPool;

        private bool _disposed;

        protected readonly ManualResetEventSlim _mre = new ManualResetEventSlim();

        private DateTime? _lastQueryingTime;
        private DateTime? _lastIndexingTime;

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

        private bool _allocationCleanupNeeded;
        private Size _currentMaximumAllowedMemory = new Size(32, SizeUnit.Megabytes);
        private NativeMemory.ThreadStats _threadAllocations;

        protected Index(int indexId, IndexType type, IndexDefinitionBase definition)
        {
            if (indexId <= 0)
                throw new ArgumentException("IndexId must be greater than zero.", nameof(indexId));

            IndexId = indexId;
            Type = type;
            Definition = definition;
            IndexPersistence = new LuceneIndexPersistence(this);
            Collections = new HashSet<string>(Definition.Collections, StringComparer.OrdinalIgnoreCase);

            if (Collections.Contains(Constants.Indexing.AllDocumentsCollection))
                HandleAllDocs = true;
        }

        public static Index Open(int indexId, string path, DocumentDatabase documentDatabase)
        {
            StorageEnvironment environment = null;

            var options = StorageEnvironmentOptions.ForPath(path);
            try
            {
                options.SchemaVersion = 1;

                environment = new StorageEnvironment(options);

                IndexType type;
                try
                {
                    type = IndexStorage.ReadIndexType(indexId, environment);
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
                        return AutoMapIndex.Open(indexId, environment, documentDatabase);
                    case IndexType.AutoMapReduce:
                        return AutoMapReduceIndex.Open(indexId, environment, documentDatabase);
                    case IndexType.Map:
                        return StaticMapIndex.Open(indexId, environment, documentDatabase);
                    case IndexType.MapReduce:
                        return MapReduceIndex.Open(indexId, environment, documentDatabase);
                    default:
                        throw new ArgumentException($"Uknown index type {type} for index {indexId}");
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

        public int IndexId { get; }

        public IndexType Type { get; }

        public IndexingPriority Priority { get; protected set; }

        public IndexDefinitionBase Definition { get; }

        public string Name => Definition?.Name;

        public virtual IndexRunningStatus Status
        {
            get
            {
                if (_indexingThread != null)
                    return IndexRunningStatus.Running;

                if (DocumentDatabase.Configuration.Indexing.Disabled)
                    return IndexRunningStatus.Disabled;

                return IndexRunningStatus.Paused;
            }
        }

        public virtual bool HasBoostedFields => false;

        protected void Initialize(DocumentDatabase documentDatabase)
        {
            _logger = LoggingSource.Instance.GetLogger<Index>(documentDatabase.Name);
            lock (_locker)
            {
                if (_initialized)
                    throw new InvalidOperationException($"Index '{Name} ({IndexId})' was already initialized.");

                var indexPath = Path.Combine(documentDatabase.Configuration.Indexing.IndexStoragePath,
                    GetIndexNameSafeForFileSystem());
                var options = documentDatabase.Configuration.Indexing.RunInMemory
                    ? StorageEnvironmentOptions.CreateMemoryOnly()
                    : StorageEnvironmentOptions.ForPath(indexPath);

                options.SchemaVersion = 1;
                try
                {
                    Initialize(new StorageEnvironment(options), documentDatabase);
                }
                catch (Exception)
                {
                    options.Dispose();
                    throw;
                }
            }
        }

        public string GetIndexNameSafeForFileSystem()
        {
            var name = Name;
            foreach (var invalidPathChar in Path.GetInvalidFileNameChars())
            {
                name = name.Replace(invalidPathChar, '_');
            }
            if (name.Length < 64)
                return $"{IndexId:0000}-{name}";
            return $"{IndexId:0000}-{name.Substring(0, 64)}";
        }

        protected void Initialize(StorageEnvironment environment, DocumentDatabase documentDatabase)
        {
            if (_disposed)
                throw new ObjectDisposedException($"Index '{Name} ({IndexId})' was already disposed.");

            lock (_locker)
            {
                if (_initialized)
                    throw new InvalidOperationException($"Index '{Name} ({IndexId})' was already initialized.");

                try
                {
                    Debug.Assert(Definition != null);

                    DocumentDatabase = documentDatabase;
                    _environment = environment;
                    _unmanagedBuffersPool = new UnmanagedBuffersPoolWithLowMemoryHandling($"Indexes//{IndexId}");
                    _contextPool = new TransactionContextPool(_environment);
                    _indexStorage = new IndexStorage(this, _contextPool, documentDatabase);
                    _logger = LoggingSource.Instance.GetLogger<Index>(documentDatabase.Name);
                    _indexStorage.Initialize(_environment);
                    IndexPersistence.Initialize(_environment, DocumentDatabase.Configuration.Indexing);

                    LoadValues();

                    DocumentDatabase.DocumentTombstoneCleaner.Subscribe(this);

                    DocumentDatabase.Notifications.OnIndexChange += HandleIndexChange;

                    _indexWorkers = CreateIndexWorkExecutors();

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
        }

        protected virtual void LoadValues()
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenReadTransaction())
            {
                Priority = _indexStorage.ReadPriority(tx);
                _lastQueryingTime = DocumentDatabase.Time.GetUtcNow();
                _lastIndexingTime = _indexStorage.ReadLastIndexingTime(tx);
            }
        }

        public virtual void Start()
        {
            if (_disposed)
                throw new ObjectDisposedException($"Index '{Name} ({IndexId})' was already disposed.");

            if (_initialized == false)
                throw new InvalidOperationException($"Index '{Name} ({IndexId})' was not initialized.");

            lock (_locker)
            {
                if (_indexingThread != null)
                    throw new InvalidOperationException($"Index '{Name} ({IndexId})' is executing.");

                if (DocumentDatabase.Configuration.Indexing.Disabled)
                    return;

                _cancellationTokenSource = new CancellationTokenSource();

                _indexingThread = new Thread(ExecuteIndexing)
                {
                    Name = "Indexing of " + Name + " of " + _indexStorage.DocumentDatabase.Name,
                    IsBackground = true
                };

                _indexingThread.Start();
            }
        }

        public virtual void Stop()
        {
            if (_disposed)
                throw new ObjectDisposedException($"Index '{Name} ({IndexId})' was already disposed.");

            if (_initialized == false)
                throw new InvalidOperationException($"Index '{Name} ({IndexId})' was not initialized.");

            lock (_locker)
            {
                if (_indexingThread == null)
                    return;

                _cancellationTokenSource.Cancel();

                var indexingThread = _indexingThread;
                _indexingThread = null;
                //Cancelation was requested, the thread will exit the indexing loop and terminate.
                //If we invoke Thread.Join from the indexing thread itself it will cause a deadlock
                if (Thread.CurrentThread != indexingThread)
                    indexingThread.Join();
            }
        }

        public void Dispose()
        {
            lock (_locker)
            {
                if (_disposed)
                    return;

                _disposed = true;

                _cancellationTokenSource?.Cancel();

                DocumentDatabase.DocumentTombstoneCleaner.Unsubscribe(this);

                DocumentDatabase.Notifications.OnIndexChange -= HandleIndexChange;

                var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(Index)} '{Name}'");

                exceptionAggregator.Execute(() =>
                {
                    _indexingThread?.Join();
                    _indexingThread = null;
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

                exceptionAggregator.ThrowIfNeeded();
            }
        }

        public virtual bool IsStale(DocumentsOperationContext databaseContext)
        {
            Debug.Assert(databaseContext.Transaction != null);

            TransactionOperationContext indexContext;
            using (_contextPool.AllocateOperationContext(out indexContext))
            using (indexContext.OpenReadTransaction())
            {
                return IsStale(databaseContext, indexContext);
            }
        }

        protected virtual bool IsStale(DocumentsOperationContext databaseContext,
            TransactionOperationContext indexContext, long? cutoff = null)
        {
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
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
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
        internal Dictionary<string, long> GetLastMappedEtagsForDebug()
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
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
            // indexing threads should have lower priority than request processing threads
            // so we let the OS know that it can schedule them appropriately.
            Threading.TryLowerCurrentThreadPriority();

            using (CultureHelper.EnsureInvariantCulture())
            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(DocumentDatabase.DatabaseShutdown,
                    _cancellationTokenSource.Token))
            {
                try
                {
                    _contextPool.SetMostWorkInGoingToHappenonThisThread();

                    DocumentDatabase.Notifications.OnDocumentChange += HandleDocumentChange;

                    while (true)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Starting indexing for '{Name} ({IndexId})'.");

                        _mre.Reset();

                        var stats = _lastStats = new IndexingStatsAggregator(DocumentDatabase.IndexStore.Identities.GetNextIndexingStatsId());
                        _lastIndexingTime = stats.StartTime;

                        AddIndexingPerformance(stats);

                        using (var scope = stats.CreateScope())
                        {
                            try
                            {
                                cts.Token.ThrowIfCancellationRequested();

                                bool didWork;
                                try
                                {
                                    TimeSpentIndexing.Start();
                                    didWork = DoIndexingWork(scope, cts.Token);
                                }
                                finally
                                {
                                    TimeSpentIndexing.Stop();
                                }

                                _indexingBatchCompleted.SetAndResetAtomically();

                                DocumentDatabase.Notifications.RaiseNotifications(
                                    new IndexChangeNotification { Name = Name, Type = IndexChangeTypes.BatchCompleted });

                                if (didWork)
                                    ResetWriteErrors();

                                if (_logger.IsInfoEnabled)
                                    _logger.Info($"Finished indexing for '{Name} ({IndexId})'.'");
                            }
                            catch (OutOfMemoryException oome)
                            {
                                if (_logger.IsInfoEnabled)
                                    _logger.Info($"Out of memory occurred for '{Name} ({IndexId})'.", oome);
                                // TODO [ppekrol] GC?
                            }
                            catch (InvalidDataException ide)
                            {
                                HandleIndexCorruption(ide);
                                return;
                            }
                            catch (IndexCorruptionException ice)
                            {
                                HandleIndexCorruption(ice);
                                return;
                            }
                            catch (IndexWriteException iwe)
                            {
                                HandleWriteErrors(scope, iwe);
                            }
                            catch (IndexAnalyzerException iae)
                            {
                                scope.AddAnalyzerError(iae);
                            }
                            catch (OperationCanceledException)
                            {
                                return;
                            }
                            catch (Exception e)
                            {
                                if (_logger.IsInfoEnabled)
                                    _logger.Info($"Exception occurred for '{Name} ({IndexId})'.", e);
                            }

                            try
                            {
                                _indexStorage.UpdateStats(stats.StartTime, stats.ToIndexingBatchStats());
                            }
                            catch (Exception e)
                            {
                                if (_logger.IsInfoEnabled)
                                    _logger.Info($"Could not update stats for '{Name} ({IndexId})'.", e);
                            }
                        }

                        stats.Complete();

                        try
                        {
                            // the logic here is that if we hit the memory limit on the system, we want to retain our
                            // allocated memory as long as we still have work to do (since we will reuse it on the next batch)
                            // and it is probably better to avoid alloc/dealloc jitter.
                            // This is because faster indexes will tend to allocate the memory faster, and we want to give them
                            // all the available resources so they can complete faster.
                            var timeToWaitForCleanup = 5000;
                            if (_allocationCleanupNeeded)
                            {
                                timeToWaitForCleanup = 0; // if there is nothing to do, immediately cleanup everything

                                // at any rate, we'll reduce the budget for this index to what it currently has allocated to avoid
                                // the case where we freed memory at the end of the batch, but didn't adjust the budget accordingly
                                // so it will think that it can allocate more than it actually should
                                _currentMaximumAllowedMemory = Size.Min(_currentMaximumAllowedMemory,
                                    new Size(NativeMemory.ThreadAllocations.Value.Allocations, SizeUnit.Bytes));
                            }
                            if (_mre.Wait(timeToWaitForCleanup, cts.Token) == false)
                            {
                                _allocationCleanupNeeded = false;

                                // there is no work to be done, and hasn't been for a while,
                                // so this is a good time to release resources we won't need 
                                // anytime soon
                                ReduceMemoryUsage();
                                _mre.Wait(cts.Token);
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
                    DocumentDatabase.Notifications.OnDocumentChange -= HandleDocumentChange;
                }
            }
        }

        private void ReduceMemoryUsage()
        {
            var beforeFree = NativeMemory.ThreadAllocations.Value.Allocations;
            if (_logger.IsInfoEnabled)
                _logger.Info(
                    $"{beforeFree / 1024:#,#} kb is used by '{Name} ({IndexId})', reducing memory utilization.");

            DocumentDatabase.DocumentsStorage.ContextPool.Clean();
            _contextPool.Clean();
            ByteStringMemoryCache.Clean();
            IndexPersistence.Clean();
            _currentMaximumAllowedMemory = new Size(32, SizeUnit.Megabytes);


            var afterFree = NativeMemory.ThreadAllocations.Value.Allocations;
            if (_logger.IsInfoEnabled)
                _logger.Info($"After clenaup, using {afterFree / 1024:#,#} kb by '{Name} ({IndexId})'.");
        }

        internal void ResetWriteErrors()
        {
            Interlocked.Exchange(ref _writeErrors, 0);
        }

        internal void HandleWriteErrors(IndexingStatsScope stats, IndexWriteException iwe)
        {
            stats.AddWriteError(iwe);

            if (iwe.InnerException is SystemException) // Don't count transient errors
                return;

            var writeErrors = Interlocked.Increment(ref _writeErrors);

            if (Priority.HasFlag(IndexingPriority.Error) || writeErrors < WriteErrorsLimit)
                return;

            SetPriority(IndexingPriority.Error);
        }

        private void HandleIndexCorruption(Exception e)
        {
            if (_logger.IsOperationsEnabled)
                _logger.Operations($"Data corruption occured for '{Name}' ({IndexId}).", e);

            SetPriority(IndexingPriority.Error);
        }

        public void HandleError(Exception e)
        {
            var ide = e as InvalidDataException;
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
            DocumentsOperationContext databaseContext;
            TransactionOperationContext indexContext;
            using (CultureHelper.EnsureInvariantCulture())
            using (DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out databaseContext))
            using (_contextPool.AllocateOperationContext(out indexContext))
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
                            using (stats.For(IndexingOperation.Lucene.FlushToDisk))
                                writeOperation.Value.Dispose();
                        }
                    }

                    _indexStorage.WriteReferences(CurrentIndexingScope.Current, tx);
                }

                using (stats.For(IndexingOperation.Storage.Commit))
                {
                    CommitStats commitStats;
                    tx.InnerTransaction.LowLevelTransaction.RetrieveCommitStats(out commitStats);

                    tx.Commit();

                    stats.RecordCommitStats(commitStats.NumberOfModifiedPages, commitStats.NumberOfPagesWrittenToDisk);
                }

                if (writeOperation.IsValueCreated)
                {
                    using (stats.For(IndexingOperation.Lucene.RecreateSearcher))
                    {
                        IndexPersistence.RecreateSearcher();
                        // we need to recreate it after transaction commit to prevent it from seeing uncommitted changes
                    }
                }

                return mightBeMore;
            }
        }

        public abstract IIndexedDocumentsEnumerator GetMapEnumerator(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext, IndexingStatsScope stats);

        public abstract void HandleDelete(DocumentTombstone tombstone, string collection, IndexWriteOperation writer,
            TransactionOperationContext indexContext, IndexingStatsScope stats);

        public abstract int HandleMap(LazyStringValue key, IEnumerable mapResults, IndexWriteOperation writer,
            TransactionOperationContext indexContext, IndexingStatsScope stats);

        private void HandleIndexChange(IndexChangeNotification notification)
        {
            if (string.Equals(notification.Name, Name, StringComparison.OrdinalIgnoreCase) == false)
                return;

            if (notification.Type == IndexChangeTypes.IndexMarkedAsErrored)
                Stop();
        }

        protected virtual void HandleDocumentChange(DocumentChangeNotification notification)
        {
            if (HandleAllDocs == false && Collections.Contains(notification.CollectionName) == false)
                return;

            _mre.Set();
        }

        public virtual List<IndexingError> GetErrors()
        {
            return _indexStorage.ReadErrors();
        }

        public virtual void SetPriority(IndexingPriority priority)
        {
            if (Priority == priority)
                return;

            lock (_locker)
            {
                if (Priority == priority)
                    return;

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Changing priority for '{Name} ({IndexId})' from '{Priority}' to '{priority}'.");

                _indexStorage.WritePriority(priority);

                var oldPriority = Priority;
                Priority = priority;

                var notificationType = IndexChangeTypes.None;

                if (priority.HasFlag(IndexingPriority.Disabled))
                    notificationType = IndexChangeTypes.IndexDemotedToDisabled;
                else if (priority.HasFlag(IndexingPriority.Error))
                    notificationType = IndexChangeTypes.IndexMarkedAsErrored;
                else if (priority.HasFlag(IndexingPriority.Idle))
                    notificationType = IndexChangeTypes.IndexDemotedToIdle;
                else if (priority.HasFlag(IndexingPriority.Normal) && oldPriority.HasFlag(IndexingPriority.Idle))
                    notificationType = IndexChangeTypes.IndexPromotedFromIdle;

                if (notificationType != IndexChangeTypes.None)
                {
                    DocumentDatabase.Notifications.RaiseNotifications(new IndexChangeNotification
                    {
                        Name = Name,
                        Type = notificationType
                    });
                }
            }
        }

        public virtual void SetLock(IndexLockMode mode)
        {
            if (Definition.LockMode == mode)
                return;

            lock (_locker)
            {
                if (Definition.LockMode == mode)
                    return;

                if (_logger.IsInfoEnabled)
                    _logger.Info(
                        $"Changing lock mode for '{Name} ({IndexId})' from '{Definition.LockMode}' to '{mode}'.");

                _indexStorage.WriteLock(mode);
            }
        }

        public virtual IndexProgress GetProgress(DocumentsOperationContext documentsContext)
        {
            if (_contextPool == null)
                throw new ObjectDisposedException("Index " + Name);

            if (documentsContext.Transaction == null)
                throw new InvalidOperationException("Cannot calculate index progress without valid transaction.");

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenReadTransaction())
            {
                var progress = new IndexProgress
                {
                    Id = IndexId,
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

                    long totalCount;
                    progressStats.NumberOfDocumentsToProcess =
                        DocumentDatabase.DocumentsStorage.GetNumberOfDocumentsToProcess(documentsContext, collection,
                            progressStats.LastProcessedDocumentEtag, out totalCount);
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
            if (_contextPool == null)
                throw new ObjectDisposedException("Index " + Name);

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenReadTransaction())
            using (var reader = IndexPersistence.OpenIndexReader(tx.InnerTransaction))
            {
                var stats = _indexStorage.ReadStats(tx);

                stats.Id = IndexId;
                stats.Name = Name;
                stats.Type = Type;
                stats.EntriesCount = reader.EntriesCount();
                stats.LockMode = Definition.LockMode;
                stats.Priority = Priority;
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

            var indexPath = Path.Combine(DocumentDatabase.Configuration.Indexing.IndexStoragePath,
                GetIndexNameSafeForFileSystem());
            var totalSize = 0L;
            foreach (var mapping in NativeMemory.FileMapping)
            {
                var directory = Path.GetDirectoryName(mapping.Key);

                if (string.Equals(indexPath, directory, StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                foreach (var singleMapping in mapping.Value)
                {
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
            if (_disposed)
                throw new ObjectDisposedException($"Index '{Name} ({IndexId})' was already disposed.");

            if (Priority.HasFlag(IndexingPriority.Idle) && Priority.HasFlag(IndexingPriority.Forced) == false)
                SetPriority(IndexingPriority.Normal);

            MarkQueried(DocumentDatabase.Time.GetUtcNow());

            AssertQueryDoesNotContainFieldsThatAreNotIndexed(query, query.SortedFields);

            Transformer transformer = null;
            if (string.IsNullOrEmpty(query.Transformer) == false)
            {
                transformer = DocumentDatabase.TransformerStore.GetTransformer(query.Transformer);
                if (transformer == null)
                    throw new InvalidOperationException($"The transformer '{query.Transformer}' was not found.");
            }

            if (resultToFill.SupportsInclude == false &&
                ((query.Includes != null && query.Includes.Length > 0) ||
                 (transformer != null && transformer.HasInclude)))
                throw new NotSupportedException("Includes are not supported by this type of query.");

            TransactionOperationContext indexContext;

            using (MarkQueryAsRunning(query, token))
            using (_contextPool.AllocateOperationContext(out indexContext))
            {
                var queryDuration = Stopwatch.StartNew();
                AsyncWaitForIndexing wait = null;

                while (true)
                {
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
                            indexContext.ResetAndRenew();

                            Debug.Assert(query.WaitForNonStaleResultsTimeout != null);

                            if (wait == null)
                                wait = new AsyncWaitForIndexing(queryDuration, query.WaitForNonStaleResultsTimeout.Value,
                                    _indexingBatchCompleted);

                            await wait.WaitForIndexingAsync().ConfigureAwait(false);
                            continue;
                        }

                        FillQueryResult(resultToFill, isStale, documentsContext, indexContext);

                        if (Type.IsMapReduce() && (query.Includes == null || query.Includes.Length == 0) &&
                            (transformer == null || transformer.MightRequireTransaction == false))
                            documentsContext.CloseTransaction();
                        // map reduce don't need to access mapResults storage unless we have a transformer. Possible optimization: if we will know if transformer needs transaction then we may reset this here or not

                        using (var reader = IndexPersistence.OpenIndexReader(indexTx.InnerTransaction))
                        {
                            var totalResults = new Reference<int>();
                            var skippedResults = new Reference<int>();

                            var fieldsToFetch = new FieldsToFetch(query, Definition, transformer);
                            IEnumerable<Document> documents;

                            if (string.IsNullOrWhiteSpace(query.Query) ||
                                query.Query.Contains(Constants.IntersectSeparator) == false)
                            {
                                documents = reader.Query(query, fieldsToFetch, totalResults, skippedResults,
                                    GetQueryResultRetriever(documentsContext, indexContext, fieldsToFetch), token.Token);
                            }
                            else
                            {
                                documents = reader.IntersectQuery(query, fieldsToFetch, totalResults, skippedResults,
                                    GetQueryResultRetriever(documentsContext, indexContext, fieldsToFetch), token.Token);
                            }

                            var includeDocumentsCommand = new IncludeDocumentsCommand(
                                DocumentDatabase.DocumentsStorage, documentsContext, query.Includes);

                            using (
                                var scope = transformer?.OpenTransformationScope(query.TransformerParameters,
                                    includeDocumentsCommand, DocumentDatabase.DocumentsStorage,
                                    DocumentDatabase.TransformerStore, documentsContext))
                            {
                                var results = scope != null ? scope.Transform(documents) : documents;

                                try
                                {
                                    foreach (var document in results)
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
                            }

                            includeDocumentsCommand.Fill(resultToFill.Includes);
                            resultToFill.TotalResults = totalResults.Value;
                            resultToFill.SkippedResults = skippedResults.Value;
                        }

                        return;
                    }
                }
            }
        }

        public virtual async Task<FacetedQueryResult> FacetedQuery(FacetQuery query, long facetSetupEtag,
            DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            if (_disposed)
                throw new ObjectDisposedException($"Index '{Name} ({IndexId})' was already disposed.");

            if (Priority.HasFlag(IndexingPriority.Idle) && Priority.HasFlag(IndexingPriority.Forced) == false)
                SetPriority(IndexingPriority.Normal);

            MarkQueried(DocumentDatabase.Time.GetUtcNow());

            AssertQueryDoesNotContainFieldsThatAreNotIndexed(query, null);

            TransactionOperationContext indexContext;
            using (_contextPool.AllocateOperationContext(out indexContext))
            {
                var result = new FacetedQueryResult();

                var queryDuration = Stopwatch.StartNew();
                AsyncWaitForIndexing wait = null;

                while (true)
                {
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
                            indexContext.ResetAndRenew();

                            Debug.Assert(query.WaitForNonStaleResultsTimeout != null);

                            if (wait == null)
                                wait = new AsyncWaitForIndexing(queryDuration, query.WaitForNonStaleResultsTimeout.Value,
                                    _indexingBatchCompleted);

                            await wait.WaitForIndexingAsync().ConfigureAwait(false);
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

        public virtual TermsQueryResult GetTerms(string field, string fromValue, int pageSize,
            DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            TransactionOperationContext indexContext;
            using (_contextPool.AllocateOperationContext(out indexContext))
            using (var tx = indexContext.OpenReadTransaction())
            {
                var result = new TermsQueryResult
                {
                    IndexName = Name,
                    ResultEtag =
                        CalculateIndexEtag(IsStale(documentsContext, indexContext), documentsContext, indexContext)
                };

                using (var reader = IndexPersistence.OpenIndexReader(tx.InnerTransaction))
                {
                    result.Terms = reader.Terms(field, fromValue, pageSize);
                }

                return result;
            }
        }

        public virtual MoreLikeThisQueryResultServerSide MoreLikeThisQuery(MoreLikeThisQueryServerSide query,
            DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            Transformer transformer = null;
            if (string.IsNullOrEmpty(query.Transformer) == false)
            {
                transformer = DocumentDatabase.TransformerStore.GetTransformer(query.Transformer);
                if (transformer == null)
                    throw new InvalidOperationException($"The transformer '{query.Transformer}' was not found.");
            }

            HashSet<string> stopWords = null;
            if (string.IsNullOrWhiteSpace(query.StopWordsDocumentId) == false)
            {
                var stopWordsDoc = DocumentDatabase.DocumentsStorage.Get(documentsContext, query.StopWordsDocumentId);
                if (stopWordsDoc == null)
                    throw new InvalidOperationException("Stop words document " + query.StopWordsDocumentId +
                                                        " could not be found");

                BlittableJsonReaderArray value;
                if (stopWordsDoc.Data.TryGet(nameof(StopWordsSetup.StopWords), out value) && value != null)
                {
                    stopWords = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    for (var i = 0; i < value.Length; i++)
                        stopWords.Add(value.GetStringByIndex(i));
                }
            }

            TransactionOperationContext indexContext;
            using (_contextPool.AllocateOperationContext(out indexContext))
            using (var tx = indexContext.OpenReadTransaction())
            {
                var result = new MoreLikeThisQueryResultServerSide();

                var isStale = IsStale(documentsContext, indexContext);

                FillQueryResult(result, isStale, documentsContext, indexContext);

                if (Type.IsMapReduce() && (query.Includes == null || query.Includes.Length == 0) &&
                    (transformer == null || transformer.MightRequireTransaction == false))
                    documentsContext.CloseTransaction();
                // map reduce don't need to access mapResults storage unless we have a transformer. Possible optimization: if we will know if transformer needs transaction then we may reset this here or not

                using (var reader = IndexPersistence.OpenIndexReader(tx.InnerTransaction))
                {
                    var includeDocumentsCommand = new IncludeDocumentsCommand(DocumentDatabase.DocumentsStorage,
                        documentsContext, query.Includes);

                    using (
                        var scope = transformer?.OpenTransformationScope(query.TransformerParameters,
                            includeDocumentsCommand, DocumentDatabase.DocumentsStorage,
                            DocumentDatabase.TransformerStore, documentsContext))
                    {
                        var documents = reader.MoreLikeThis(query, stopWords,
                            fieldsToFetch =>
                                GetQueryResultRetriever(documentsContext, indexContext,
                                    new FieldsToFetch(fieldsToFetch, Definition, null)), token.Token);
                        var results = scope != null ? scope.Transform(documents) : documents;

                        foreach (var document in results)
                        {
                            result.Results.Add(document);
                            includeDocumentsCommand.Gather(document);
                        }
                    }

                    includeDocumentsCommand.Fill(result.Includes);
                }

                return result;
            }
        }

        private void AssertQueryDoesNotContainFieldsThatAreNotIndexed(IndexQueryBase query, SortedField[] sortedFields)
        {
            if (string.IsNullOrWhiteSpace(query.Query) == false)
            {
                var setOfFields = SimpleQueryParser.GetFields(query.Query, query.DefaultOperator, query.DefaultField);
                foreach (var field in setOfFields)
                {
                    var f = field;

                    if (IndexPersistence.ContainsField(f) == false &&
                        IndexPersistence.ContainsField("_") == false)
                        // the catch all field name means that we have dynamic fields names
                        throw new ArgumentException("The field '" + f +
                                                    "' is not indexed, cannot query on fields that are not indexed");
                }
            }
            if (sortedFields != null)
            {
                foreach (var sortedField in sortedFields)
                {
                    var f = sortedField.Field;
                    if (f == Constants.Indexing.Fields.IndexFieldScoreName)
                        continue;

                    if (f.StartsWith(Constants.Indexing.Fields.RandomFieldName) ||
                        f.StartsWith(Constants.Indexing.Fields.CustomSortFieldName))
                        continue;

                    if (f.StartsWith(Constants.Indexing.Fields.AlphaNumericFieldName))
                    {
                        f = SortFieldHelper.ExtractName(f);
                        if (string.IsNullOrEmpty(f))
                            throw new ArgumentException("Alpha numeric sorting requires a field name");
                    }

                    if (IndexPersistence.ContainsField(f) == false &&
                        f.StartsWith(Constants.Indexing.Fields.DistanceFieldName) == false &&
                        IndexPersistence.ContainsField("_") == false)
                        // the catch all field name means that we have dynamic fields names
                        throw new ArgumentException("The field '" + f +
                                                    "' is not indexed, cannot sort on fields that are not indexed");
                }
            }
        }

        private void FillFacetedQueryResult(FacetedQueryResult result, bool isStale, long facetSetupEtag,
            DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {
            result.IndexName = Name;
            result.IsStale = isStale;
            result.IndexTimestamp = _lastIndexingTime ?? DateTime.MinValue;
            result.LastQueryTime = _lastQueryingTime ?? DateTime.MinValue;
            result.ResultEtag = CalculateIndexEtag(result.IsStale, documentsContext, indexContext) ^ facetSetupEtag;
        }

        private void FillQueryResult<T>(QueryResultBase<T> result, bool isStale,
            DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {
            result.IndexName = Name;
            result.IsStale = isStale;
            result.IndexTimestamp = _lastIndexingTime ?? DateTime.MinValue;
            result.LastQueryTime = _lastQueryingTime ?? DateTime.MinValue;
            result.ResultEtag = CalculateIndexEtag(result.IsStale, documentsContext, indexContext);
        }

        private DisposableAction MarkQueryAsRunning(IndexQueryServerSide query, OperationCancelToken token)
        {
            var queryStartTime = DateTime.UtcNow;
            var queryId = Interlocked.Increment(ref _numberOfQueries);
            var executingQueryInfo = new ExecutingQueryInfo(queryStartTime, query, queryId, token);

            CurrentlyRunningQueries.Add(executingQueryInfo);

            return new DisposableAction(() => { CurrentlyRunningQueries.TryRemove(executingQueryInfo); });
        }

        private static bool WillResultBeAcceptable(bool isStale, IndexQueryBase query, AsyncWaitForIndexing wait)
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
            var indexEtagBytes = new long[
                1 + // definition hash
                1 + // isStale
                2 * Collections.Count // last document etags and last mapped etags per collection
                ];

            CalculateIndexEtagInternal(indexEtagBytes, isStale, documentsContext, indexContext);

            unchecked
            {
                fixed (long* buffer = indexEtagBytes)
                {
                    return
                        (long)Hashing.XXHash64.Calculate((byte*)buffer, (ulong)(indexEtagBytes.Length * sizeof(long)));
                }
            }
        }

        protected int CalculateIndexEtagInternal(long[] indexEtagBytes, bool isStale,
            DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {
            var index = 0;

            indexEtagBytes[index++] = Definition.GetHashCode();
            indexEtagBytes[index++] = isStale ? 0L : 1L;

            foreach (var collection in Collections)
            {
                var lastDocEtag = DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(documentsContext, collection);
                var lastMappedEtag = _indexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection);

                indexEtagBytes[index++] = lastDocEtag;
                indexEtagBytes[index++] = lastMappedEtag;
            }

            return index;
        }

        public long GetIndexEtag()
        {
            DocumentsOperationContext documentsContext;
            TransactionOperationContext indexContext;

            using (DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out documentsContext))
            using (_contextPool.AllocateOperationContext(out indexContext))
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
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
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

        public IndexingPerformanceStats[] GetIndexingPerformance(int fromId)
        {
            var lastStats = _lastStats;

            return _lastIndexingStats
                .Where(x => x.Id >= fromId)
                .Select(x => x == lastStats ? x.ToIndexingPerformanceLiveStatsWithDetails() : x.ToIndexingPerformanceStats())
                .ToArray();
        }

        public abstract IQueryResultRetriever GetQueryResultRetriever(DocumentsOperationContext documentsContext,
            TransactionOperationContext indexContext, FieldsToFetch fieldsToFetch);

        public abstract int? ActualMaxNumberOfIndexOutputs { get; }

        public abstract int MaxNumberOfIndexOutputs { get; }

        protected virtual bool EnsureValidNumberOfOutputsForDocument(int numberOfAlreadyProducedOutputs)
        {
            return numberOfAlreadyProducedOutputs <= MaxNumberOfIndexOutputs;
        }

        public virtual Dictionary<string, HashSet<CollectionName>> GetReferencedCollections()
        {
            return null;
        }

        public bool CanContinueBatch(IndexingStatsScope stats)
        {
            stats.RecordMapAllocations(_threadAllocations.Allocations);

            if (_threadAllocations.Allocations > _currentMaximumAllowedMemory.GetValue(SizeUnit.Bytes))
            {
                if (TryIncreasingMemoryUsageForIndex(new Size(_threadAllocations.Allocations, SizeUnit.Bytes), stats) == false)
                {
                    if (stats.MapAttempts < DocumentDatabase.Configuration.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)
                        return true;

                    stats.RecordMapCompletedReason("Cannot budget additional memory for batch");
                    return false;
                }
            }
            return true;
        }

        public long GetLastDocumentEtagInCollection(DocumentsOperationContext databaseContext, string collection)
        {
            return collection == Constants.Indexing.AllDocumentsCollection
                ? DocumentsStorage.ReadLastDocumentEtag(databaseContext.Transaction.InnerTransaction)
                : DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(databaseContext, collection);
        }

        public long GetLastTombstoneEtagInCollection(DocumentsOperationContext databaseContext, string collection)
        {
            return collection == Constants.Indexing.AllDocumentsCollection
                ? DocumentsStorage.ReadLastTombstoneEtag(databaseContext.Transaction.InnerTransaction)
                : DocumentDatabase.DocumentsStorage.GetLastTombstoneEtag(databaseContext, collection);
        }

        private bool TryIncreasingMemoryUsageForIndex(Size currentlyAllocated, IndexingStatsScope stats)
        {
            //TODO: This has to be exposed via debug endpoint

            // we run out our memory quota, so we need to see if we can increase it or break
            var memoryInfoResult = MemoryInformation.GetMemoryInfo();

            using (var currentProcess = Process.GetCurrentProcess())
            {
                // a lot of the memory that we use is actually from memory mapped files, as such, we can
                // rely on the OS to page it out (without needing to write, since it is read only in this case)
                // so we try to calculate how much such memory we can use with this assumption 
                var memoryMappedSize = new Size(currentProcess.WorkingSet64 - currentProcess.PrivateMemorySize64, SizeUnit.Bytes);

                stats.RecordMapMemoryStats(currentProcess.WorkingSet64, currentProcess.PrivateMemorySize64, _currentMaximumAllowedMemory.GetValue(SizeUnit.Bytes));

                if (memoryMappedSize < Size.Zero)
                {
                    // in this case, we are likely paging, our working set is smaller than the memory we allocated
                    // it isn't _neccesarily_ a bad thing, we might be paged on allocated memory we aren't using, but
                    // at any rate, we'll ignore that and just use the actual physical memory available
                    memoryMappedSize = Size.Zero;
                }
                var minMemoryToLeaveForMemoryMappedFiles = memoryInfoResult.TotalPhysicalMemory / 4;

                var memoryAssumedFreeOrCheapToFree = (memoryInfoResult.AvailableMemory + memoryMappedSize - minMemoryToLeaveForMemoryMappedFiles);

                // there isn't enough available memory to try, we want to leave some out for other things
                if (memoryAssumedFreeOrCheapToFree < memoryInfoResult.TotalPhysicalMemory / 10)
                {
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info(
                            $"{Name} ({IndexId}) which is already using {currentlyAllocated}/{_currentMaximumAllowedMemory} and the system has" +
                            $"{memoryInfoResult.AvailableMemory}/{memoryInfoResult.TotalPhysicalMemory} free RAM. Also have ~{memoryMappedSize} in mmap " +
                            $"files that can be cleanly released, not enough to proceed in batch.");
                    }
                    _allocationCleanupNeeded = true;
                    return false;
                }

                // If there isn't enough here to double our current allocation, we won't allocate any more
                // we do this check in this way to prevent multiple indexes of hitting this at the
                // same time and each thinking that they have enough space
                if (memoryAssumedFreeOrCheapToFree < _currentMaximumAllowedMemory)
                {
                    // TODO: We probably need to make a note of this in log & expose in stats
                    // TODO: to explain why we aren't increasing the memory in use
                    _allocationCleanupNeeded = true;
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info(
                            $"{Name} ({IndexId}) which is already using {currentlyAllocated}/{_currentMaximumAllowedMemory} and the system has" +
                            $"{memoryInfoResult.AvailableMemory}/{memoryInfoResult.TotalPhysicalMemory} free RAM. Also have ~{memoryMappedSize} in mmap " +
                            $"files that can be cleanly released, not enough to proceed in batch.");
                    }
                    return false;
                }

                // even though we have twice as much memory as we have current allocated, we will 
                // only increment by 16MB to avoid over allocation by multiple indexes. This way, 
                // we'll check often as we go along this
                var oldBudget = _currentMaximumAllowedMemory;
                _currentMaximumAllowedMemory = currentlyAllocated + new Size(16, SizeUnit.Megabytes);

                if (_logger.IsInfoEnabled)
                {
                    _logger.Info(
                        $"Increasing memory budget for {Name} ({IndexId}) which is using  {currentlyAllocated}/{oldBudget} and the system has" +
                        $"{memoryInfoResult.AvailableMemory}/{memoryInfoResult.TotalPhysicalMemory} free RAM with {memoryMappedSize} in mmap " +
                        $"files that can be cleanly released. Budget increased to {_currentMaximumAllowedMemory}");
                }
                return true;
            }
        }
    }
}