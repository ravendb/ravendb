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
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Client.Data.Queries;
using Raven.Client.Indexing;
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
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.Documents.Queries.Parse;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Sorting;
using Raven.Server.Documents.Transformers;
using Raven.Server.Exceptions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
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
using Voron.Exceptions;
using Voron.Impl.Compaction;

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

        private long _criticalErrors;

        private long _analyzerErrors;

        private const long WriteErrorsLimit = 10;

        private const long CriticalErrorsLimit = 3;

        private const long AnalyzerErrorLimit = 0;

        protected Logger _logger;

        internal LuceneIndexPersistence IndexPersistence;

        private readonly AsyncManualResetEvent _indexingBatchCompleted = new AsyncManualResetEvent();

        private CancellationTokenSource _cancellationTokenSource;

        protected DocumentDatabase DocumentDatabase;

        private Thread _indexingThread;

        private bool _initialized;

        protected UnmanagedBuffersPoolWithLowMemoryHandling _unmanagedBuffersPool;

        private StorageEnvironment _environment;

        internal TransactionContextPool _contextPool;

        internal bool _disposed;

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
        private Size _currentMaximumAllowedMemory = new Size(32, SizeUnit.Megabytes);
        private NativeMemory.ThreadStats _threadAllocations;
        private string _errorStateReason;
        private bool _isCompactionInProgress;
        private readonly ReaderWriterLockSlim _currentlyRunningQueriesLock = new ReaderWriterLockSlim();
        private volatile bool _priorityChanged;
        private volatile bool _hadRealIndexingWorkToDo;
        
        private readonly WarnIndexOutputsPerDocument _indexOutputsPerDocumentWarning = new WarnIndexOutputsPerDocument
        {
            MaxNumberOutputsPerDocument = int.MinValue,
            Suggestion = "Please verify this index definition and consider a re-design of your entities or index for better indexing performance"
        };

        protected Index(int indexId, IndexType type, IndexDefinitionBase definition)
        {
            if (indexId <= 0)
                throw new ArgumentException("IndexId must be greater than zero.", nameof(indexId));

            IndexId = indexId;
            Type = type;
            Definition = definition;
            Collections = new HashSet<string>(Definition.Collections, StringComparer.OrdinalIgnoreCase);

            if (Collections.Contains(Constants.Indexing.AllDocumentsCollection))
                HandleAllDocs = true;
        }

        public static Index Open(int indexId, string path, DocumentDatabase documentDatabase)
        {
            StorageEnvironment environment = null;

            var name = Path.GetDirectoryName(path);
            var indexPath = path;

            var indexTempPath =
                documentDatabase.Configuration.Indexing.TempPath?.Combine(name);

            var journalPath = documentDatabase.Configuration.Indexing.JournalsStoragePath?.Combine(name);

            var options = StorageEnvironmentOptions.ForPath(indexPath, indexTempPath?.FullPath, journalPath?.FullPath);
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
                        return MapIndex.Open(indexId, environment, documentDatabase);
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

        public IndexPriority Priority { get; private set; }

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
                    throw new InvalidOperationException($"Index '{Name} ({IndexId})' was already initialized.");

                var name = GetIndexNameSafeForFileSystem();

                var indexPath = configuration.StoragePath.Combine(name);

                var indexTempPath = configuration.TempPath?.Combine(name);

                var journalPath = configuration.JournalsStoragePath?.Combine(name);

                var options = configuration.RunInMemory
                    ? StorageEnvironmentOptions.CreateMemoryOnly(indexPath.FullPath, indexTempPath?.FullPath)
                    : StorageEnvironmentOptions.ForPath(indexPath.FullPath, indexTempPath?.FullPath, journalPath?.FullPath);

                options.SchemaVersion = 1;
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

        private ExitWriteLock DrainRunningQueries()
        {
            if (_currentlyRunningQueriesLock.IsWriteLockHeld)
                return new ExitWriteLock();

            if (_currentlyRunningQueriesLock.TryEnterWriteLock(TimeSpan.FromSeconds(10)) == false)
            {
                throw new InvalidOperationException("After waiting for 10 seconds for all running queries ");
            }
            return new ExitWriteLock(_currentlyRunningQueriesLock);
        }

        private struct ExitWriteLock : IDisposable
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

        protected void Initialize(StorageEnvironment environment, DocumentDatabase documentDatabase, IndexingConfiguration configuration, PerformanceHintsConfiguration performanceHints)
        {
            if (_disposed)
                throw new ObjectDisposedException($"Index '{Name} ({IndexId})' was already disposed.");

            using (DrainRunningQueries())
            {
                if (_initialized)
                    throw new InvalidOperationException($"Index '{Name} ({IndexId})' was already initialized.");

                try
                {
                    Debug.Assert(Definition != null);

                    DocumentDatabase = documentDatabase;
                    Configuration = configuration;
                    PerformanceHints = performanceHints;

                    _environment = environment;
                    _unmanagedBuffersPool = new UnmanagedBuffersPoolWithLowMemoryHandling($"Indexes//{IndexId}");
                    _contextPool = new TransactionContextPool(_environment);
                    _indexStorage = new IndexStorage(this, _contextPool, documentDatabase);
                    _logger = LoggingSource.Instance.GetLogger<Index>(documentDatabase.Name);
                    _indexStorage.Initialize(_environment);
                    IndexPersistence = new LuceneIndexPersistence(this);
                    IndexPersistence.Initialize(_environment);

                    LoadValues();

                    DocumentDatabase.DocumentTombstoneCleaner.Subscribe(this);

                    DocumentDatabase.Changes.OnIndexChange += HandleIndexChange;

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
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenReadTransaction())
            {
                Priority = _indexStorage.ReadPriority(tx);
                State = _indexStorage.ReadState(tx);
                _lastQueryingTime = DocumentDatabase.Time.GetUtcNow();
                LastIndexingTime = _indexStorage.ReadLastIndexingTime(tx);
                MaxNumberOfOutputsPerDocument = _indexStorage.ReadStats(tx).MaxNumberOfOutputsPerDocument;
            }
        }

        public virtual void Start()
        {
            if (_disposed)
                throw new ObjectDisposedException($"Index '{Name} ({IndexId})' was already disposed.");

            if (_initialized == false)
                throw new InvalidOperationException($"Index '{Name} ({IndexId})' was not initialized.");

            using (DrainRunningQueries())
            {
                if (_indexingThread != null)
                    throw new InvalidOperationException($"Index '{Name} ({IndexId})' is executing.");

                if (Configuration.Disabled)
                    return;

                if (State == IndexState.Disabled)
                    return;

                SetState(IndexState.Normal);

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

            using (DrainRunningQueries())
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

                if (status == IndexRunningStatus.Running)
                    Start();
            }
        }

        public virtual void Dispose()
        {
            var needToLock = _currentlyRunningQueriesLock.IsWriteLockHeld == false;
            if (needToLock)
                _currentlyRunningQueriesLock.EnterWriteLock();
            try
            {
                if (_disposed)
                    return;

                _disposed = true;

                _cancellationTokenSource?.Cancel();

                DocumentDatabase.DocumentTombstoneCleaner.Unsubscribe(this);

                DocumentDatabase.Changes.OnIndexChange -= HandleIndexChange;

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

            TransactionOperationContext indexContext;
            using (_contextPool.AllocateOperationContext(out indexContext))
            using (indexContext.OpenReadTransaction())
            {
                return IsStale(databaseContext, indexContext, cutoff);
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
        /// TODO iftah, change visibility of function back to internal when finished with new client
        public Dictionary<string, long> GetLastMappedEtagsForDebug()
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
            _priorityChanged = true;

            using (CultureHelper.EnsureInvariantCulture())
            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(DocumentDatabase.DatabaseShutdown,
                _cancellationTokenSource.Token))
            {
                // if we are starting indexing e.g. manually after failure
                // we need to reset errors to give it a chance
                ResetErrors();

                try
                {
                    _contextPool.SetMostWorkInGoingToHappenonThisThread();

                    DocumentDatabase.Changes.OnDocumentChange += HandleDocumentChange;
                    _environment.OnLogsApplied += HandleLogsApplied;

                    while (true)
                    {
                        ChangeIndexThreadPriorityIfNeeded();

                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Starting indexing for '{Name} ({IndexId})'.");

                        _mre.Reset();

                        var stats = _lastStats = new IndexingStatsAggregator(DocumentDatabase.IndexStore.Identities.GetNextIndexingStatsId(), _lastStats);
                        LastIndexingTime = stats.StartTime;

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

                                DocumentDatabase.Changes.RaiseNotifications(
                                    new IndexChange { Name = Name, Type = IndexChangeTypes.BatchCompleted });

                                if (didWork)
                                    ResetErrors();

                                _hadRealIndexingWorkToDo |= didWork;

                                if (_logger.IsInfoEnabled)
                                    _logger.Info($"Finished indexing for '{Name} ({IndexId})'.'");
                            }
                            catch (OutOfMemoryException oome)
                            {
                                if (_logger.IsInfoEnabled)
                                    _logger.Info($"Out of memory occurred for '{Name} ({IndexId})'.", oome);
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
                            catch (OperationCanceledException)
                            {
                                return;
                            }
                            catch (Exception e)
                            {
                                if (_logger.IsOperationsEnabled)
                                    _logger.Operations($"Critical exception occurred for '{Name} ({IndexId})'.", e);

                                HandleCriticalErrors(scope, e);
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
                                    _logger.Info($"Could not update stats for '{Name} ({IndexId})'.", e);
                            }
                        }

                        stats.Complete();

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

                            if (_mre.Wait(timeToWaitForMemoryCleanup, cts.Token) == false)
                            {
                                _allocationCleanupNeeded = false;

                                // there is no work to be done, and hasn't been for a while,
                                // so this is a good time to release resources we won't need 
                                // anytime soon
                                ReduceMemoryUsage();

                                var numberOfSetEvents =
                                    WaitHandle.WaitAny(new[]
                                        {_mre.WaitHandle, _logsAppliedEvent.WaitHandle, cts.Token.WaitHandle});

                                if (numberOfSetEvents == 1 && _logsAppliedEvent.IsSet)
                                {
                                    _hadRealIndexingWorkToDo = false;
                                    _environment.Cleanup();
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
                    _environment.OnLogsApplied -= HandleLogsApplied;
                    DocumentDatabase.Changes.OnDocumentChange -= HandleDocumentChange;
                }
            }
        }

        private void ChangeIndexThreadPriorityIfNeeded()
        {
            if (_priorityChanged == false)
                return;

            _priorityChanged = false;

            ThreadPriority newPriority;
            var priority = Priority;
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

            var currentPriority = Threading.GetCurrentThreadPriority();
            if (currentPriority == newPriority)
                return;

            Threading.TrySettingCurrentThreadPriority(newPriority);
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

        internal void ResetErrors()
        {
            Interlocked.Exchange(ref _writeErrors, 0);
            Interlocked.Exchange(ref _criticalErrors, 0);
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

        internal void HandleCriticalErrors(IndexingStatsScope stats, Exception e)
        {
            stats.AddCriticalError(e);

            var criticalErrors = Interlocked.Increment(ref _criticalErrors);

            if (State == IndexState.Error || criticalErrors < CriticalErrorsLimit)
                return;

            // TODO we should create notification here?
            _errorStateReason = $"State was changed due to excessive number of critical errors ({criticalErrors}).";
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
                _logger.Operations($"Data corruption occured for '{Name}' ({IndexId}).", e);

            // TODO we should create notification here?

            _errorStateReason = $"State was changed due to data corruption with message '{e.Message}'";
            SetState(IndexState.Error);
        }

        private void HandleIndexFailureInformation(IndexFailureInformation failureInformation)
        {
            if (failureInformation.IsInvalidIndex == false)
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
            DocumentsOperationContext databaseContext;
            TransactionOperationContext indexContext;
            using (CultureHelper.EnsureInvariantCulture())
            using (DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out databaseContext))
            using (_contextPool.AllocateOperationContext(out indexContext))
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

                        stats.RecordCommitStats(commitStats.NumberOfModifiedPages, commitStats.NumberOfPagesWrittenToDisk);
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

        public virtual void SetPriority(IndexPriority priority)
        {
            if (Priority == priority)
                return;

            using (DrainRunningQueries())
            {
                AssertIndexState(assertState: false);

                if (Priority == priority)
                    return;

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Changing priority for '{Name} ({IndexId})' from '{Priority}' to '{priority}'.");

                _indexStorage.WritePriority(priority);

                Priority = priority;
                _priorityChanged = true;
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
                    _logger.Info($"Changing state for '{Name} ({IndexId})' from '{State}' to '{state}'.");

                _indexStorage.WriteState(state);

                var oldState = State;
                State = state;

                var notificationType = IndexChangeTypes.None;

                if (state == IndexState.Disabled)
                    notificationType = IndexChangeTypes.IndexDemotedToDisabled;
                else if (state == IndexState.Error)
                    notificationType = IndexChangeTypes.IndexMarkedAsErrored;
                else if (state == IndexState.Idle)
                    notificationType = IndexChangeTypes.IndexDemotedToIdle;
                else if (state == IndexState.Normal && oldState == IndexState.Idle)
                    notificationType = IndexChangeTypes.IndexPromotedFromIdle;

                if (notificationType != IndexChangeTypes.None)
                {
                    DocumentDatabase.Changes.RaiseNotifications(new IndexChange
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


            using (DrainRunningQueries())
            {
                AssertIndexState(assertState: false);

                if (Definition.LockMode == mode)
                    return;

                if (_logger.IsInfoEnabled)
                    _logger.Info(
                        $"Changing lock mode for '{Name} ({IndexId})' from '{Definition.LockMode}' to '{mode}'.");

                _indexStorage.WriteLock(mode);
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

                SetState(IndexState.Disabled);
                Stop();
            }
        }

        public virtual IndexProgress GetProgress(DocumentsOperationContext documentsContext)
        {
            if (_isCompactionInProgress)
            {
                return new IndexProgress
                {
                    Name = Name,
                    Id = IndexId,
                    Type = Type
                };
            }

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
            if (_isCompactionInProgress)
            {
                return new IndexStats
                {
                    Name = Name,
                    Id = IndexId,
                    Type = Type
                };
            }

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

            var name = GetIndexNameSafeForFileSystem();

            var indexPath = Configuration.StoragePath.Combine(GetIndexNameSafeForFileSystem());

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
                    TransactionOperationContext indexContext;
                    using (_contextPool.AllocateOperationContext(out indexContext))
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
                                wait = new AsyncWaitForIndexing(queryDuration, query.WaitForNonStaleResultsTimeout.Value, this);


                            marker.ReleaseLock();

                            await wait.WaitForIndexingAsync(frozenAwaiter).ConfigureAwait(false);
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
                                    GetQueryResultRetriever(documentsContext, fieldsToFetch), token.Token);
                            }
                            else
                            {
                                documents = reader.IntersectQuery(query, fieldsToFetch, totalResults, skippedResults,
                                    GetQueryResultRetriever(documentsContext, fieldsToFetch), token.Token);
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
            AssertIndexState();

            if (State == IndexState.Idle)
                SetState(IndexState.Normal);

            MarkQueried(DocumentDatabase.Time.GetUtcNow());

            AssertQueryDoesNotContainFieldsThatAreNotIndexed(query, null);

            

            using (var marker = MarkQueryAsRunning(query, token))
            
            {
                var result = new FacetedQueryResult();

                var queryDuration = Stopwatch.StartNew();
                AsyncWaitForIndexing wait = null;

                while (true)
                {
                    TransactionOperationContext indexContext;
                    AssertIndexState();
                    marker.HoldLock();

                    using (_contextPool.AllocateOperationContext(out indexContext))
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

            TransactionOperationContext indexContext;
            using (_contextPool.AllocateOperationContext(out indexContext))
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

        public virtual MoreLikeThisQueryResultServerSide MoreLikeThisQuery(MoreLikeThisQueryServerSide query,
            DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            AssertIndexState();

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

            using (var marker = MarkQueryAsRunning(query, token))
            {
                AssertIndexState();
                marker.HoldLock();

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
                                    GetQueryResultRetriever(documentsContext,
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
        }

        public IndexEntriesQueryResult IndexEntries(IndexQueryServerSide query, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            AssertIndexState();

            if (State == IndexState.Idle)
                SetState(IndexState.Normal);

            MarkQueried(DocumentDatabase.Time.GetUtcNow());

            AssertQueryDoesNotContainFieldsThatAreNotIndexed(query, query.SortedFields);

            TransactionOperationContext indexContext;
            using (var marker = MarkQueryAsRunning(query, token))
            using (_contextPool.AllocateOperationContext(out indexContext))
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
            if (_isCompactionInProgress)
                throw new InvalidOperationException($"Index '{Name} ({IndexId})' is currently being compacted.");

            if (_initialized == false)
                throw new InvalidOperationException($"Index '{Name} ({IndexId})' was not initialized.");

            if (_disposed)
                throw new ObjectDisposedException($"Index '{Name} ({IndexId})' was already disposed.");

            if (assertState && State == IndexState.Error)
            {
                var errorStateReason = _errorStateReason;
                if (string.IsNullOrWhiteSpace(errorStateReason) == false)
                    throw new InvalidOperationException($"Index '{Name} ({IndexId})' is marked as errored. {errorStateReason}");

                throw new InvalidOperationException(
                    $"Index '{Name} ({IndexId})' is marked as errored. Please check index errors avaiable at '/databases/{DocumentDatabase.Name}/indexes/errors?name={Name}'.");
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
            result.IndexTimestamp = LastIndexingTime ?? DateTime.MinValue;
            result.LastQueryTime = _lastQueryingTime ?? DateTime.MinValue;
            result.ResultEtag = CalculateIndexEtag(result.IsStale, documentsContext, indexContext) ^ facetSetupEtag;
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

        public abstract IQueryResultRetriever GetQueryResultRetriever(DocumentsOperationContext documentsContext, FieldsToFetch fieldsToFetch);

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
                $"Index '{Name}' has produced more than {PerformanceHints.MaxWarnIndexOutputsPerDocument} map results from a single document",
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

        public bool CanContinueBatch(IndexingStatsScope stats)
        {
            stats.RecordMapAllocations(_threadAllocations.Allocations);

            if (stats.ErrorsCount >= IndexStorage.MaxNumberOfKeptErrors)
            {
                stats.RecordMapCompletedReason($"Number of errors ({stats.ErrorsCount}) reached maximum number of allowed errors per batch ({IndexStorage.MaxNumberOfKeptErrors})");
                return false;
            }

            if (_threadAllocations.Allocations > _currentMaximumAllowedMemory.GetValue(SizeUnit.Bytes))
            {
                if (TryIncreasingMemoryUsageForIndex(new Size(_threadAllocations.Allocations, SizeUnit.Bytes), stats) == false)
                {
                    if (stats.MapAttempts < Configuration.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)
                        return true;

                    stats.RecordMapCompletedReason("Cannot budget additional memory for batch");
                    return false;
                }
            }
            return true;
        }

        public IOperationResult Compact(Action<IOperationProgress> onProgress)
        {
            if (_isCompactionInProgress)
                throw new InvalidOperationException($"Index '{Name} ({IndexId})' cannot be compacted because compaction is already in progress.");
            var progress = new IndexCompactionProgress
            {
                Message = "Draining queries for " + Name
            };
            onProgress?.Invoke(progress);

            using (DrainRunningQueries())
            {
                if (_environment.Options.IncrementalBackupEnabled)
                    throw new InvalidOperationException(
                        $"Index '{Name} ({IndexId})' cannot be compacted because incremental backup is enabled.");

                if (Configuration.RunInMemory)
                    throw new InvalidOperationException(
                        $"Index '{Name} ({IndexId})' cannot be compacted because it runs in memory.");

                _isCompactionInProgress = true;
                progress.Message = null;

                PathSetting compactPath = null;

                try
                {
                    var environmentOptions =
                        (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)_environment.Options;
                    var srcOptions = StorageEnvironmentOptions.ForPath(environmentOptions.BasePath);

                    var wasRunning = _indexingThread != null;

                    Dispose();

                    compactPath = Configuration.StoragePath.Combine(GetIndexNameSafeForFileSystem() + "_Compact");

                    using (var compactOptions = (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(compactPath.FullPath))
                    {
                        StorageCompaction.Execute(srcOptions, compactOptions, progressReport =>
                        {
                            progress.Processed = progressReport.GlobalProgress;
                            progress.Total = progressReport.GlobalTotal;

                            onProgress?.Invoke(progress);
                        });
                    }

                    IOExtensions.DeleteDirectory(environmentOptions.BasePath);
                    IOExtensions.MoveDirectory(compactPath.FullPath, environmentOptions.BasePath);

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

        public virtual DetailedStorageReport GenerateStorageReport(bool calculateExactSizes)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenReadTransaction())
            {
                return _environment.GenerateDetailedReport(tx.InnerTransaction, calculateExactSizes);
            }
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
    }
}