using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Client.Data.Queries;
using Raven.Client.Indexing;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Transformers;
using Raven.Server.Exceptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

using Sparrow;
using Sparrow.Collections;
using Sparrow.Json;
using Voron;
using Sparrow.Logging;

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

        protected UnmanagedBuffersPool _unmanagedBuffersPool;

        private StorageEnvironment _environment;

        internal TransactionContextPool _contextPool;

        private bool _disposed;

        protected readonly ManualResetEventSlim _mre = new ManualResetEventSlim();

        private DateTime? _lastQueryingTime;

        public readonly HashSet<string> Collections;

        internal IndexStorage _indexStorage;

        private IIndexingWork[] _indexWorkers;

        public readonly ConcurrentSet<ExecutingQueryInfo> CurrentlyRunningQueries = new ConcurrentSet<ExecutingQueryInfo>();

        private readonly ConcurrentQueue<IndexingStatsAggregator> _lastIndexingStats = new ConcurrentQueue<IndexingStatsAggregator>();

        private int _numberOfQueries;

        protected Index(int indexId, IndexType type, IndexDefinitionBase definition)
        {
            if (indexId <= 0)
                throw new ArgumentException("IndexId must be greater than zero.", nameof(indexId));

            IndexId = indexId;
            Type = type;
            Definition = definition;
            IndexPersistence = new LuceneIndexPersistence(this);
            Collections = new HashSet<string>(Definition.Collections, StringComparer.OrdinalIgnoreCase);
       }

        public static Index Open(int indexId, DocumentDatabase documentDatabase)
        {
            StorageEnvironment environment = null;

            var options = StorageEnvironmentOptions.ForPath(Path.Combine(documentDatabase.Configuration.Indexing.IndexStoragePath, indexId.ToString()));
            try
            {
                options.SchemaVersion = 1;

                environment = new StorageEnvironment(options);
                var type = IndexStorage.ReadIndexType(indexId, environment);

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
                        throw new NotImplementedException();
                }
            }
            catch (Exception)
            {
                if (environment != null)
                    environment.Dispose();
                else
                    options.Dispose();

                throw;
            }
        }

        public int IndexId { get; }

        public IndexType Type { get; }

        public IndexingPriority Priority { get; protected set; }

        public IndexDefinitionBase Definition { get; }

        public string Name => Definition?.Name;

        public bool IsRunning => _indexingThread != null;

        protected void Initialize(DocumentDatabase documentDatabase)
        {
            _logger = LoggerSetup.Instance.GetLogger<Index>(documentDatabase.Name);
            lock (_locker)
            {
                if (_initialized)
                    throw new InvalidOperationException($"Index '{Name} ({IndexId})' was already initialized.");

                var options = documentDatabase.Configuration.Indexing.RunInMemory
                    ? StorageEnvironmentOptions.CreateMemoryOnly()
                    : StorageEnvironmentOptions.ForPath(Path.Combine(documentDatabase.Configuration.Indexing.IndexStoragePath, IndexId.ToString()));

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
                    _unmanagedBuffersPool = new UnmanagedBuffersPool($"Indexes//{IndexId}");
                    _contextPool = new TransactionContextPool(_environment);
                    _indexStorage = new IndexStorage(this, _contextPool, documentDatabase);
                    _logger = LoggerSetup.Instance.GetLogger<Index>(documentDatabase.Name);
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
                _lastQueryingTime = SystemTime.UtcNow;
            }
        }

        public void Start()
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
                    Name = "Indexing of " + Name,
                    IsBackground = true
                };

                _indexingThread.Start();
            }
        }

        public void Stop()
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

        public bool IsStale(DocumentsOperationContext databaseContext)
        {
            Debug.Assert(databaseContext.Transaction != null);

            TransactionOperationContext indexContext;
            using (_contextPool.AllocateOperationContext(out indexContext))
            using (indexContext.OpenReadTransaction())
            {
                return IsStale(databaseContext, indexContext);
            }
        }

        protected virtual bool IsStale(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, long? cutoff = null)
        {
            foreach (var collection in Collections)
            {
                var lastDocEtag = DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(databaseContext, collection);
                var lastProcessedDocEtag = _indexStorage.ReadLastIndexedEtag(indexContext.Transaction, collection);

                if (cutoff == null)
                {
                    if (lastDocEtag > lastProcessedDocEtag)
                        return true;

                    var lastTombstoneEtag = DocumentDatabase.DocumentsStorage.GetLastTombstoneEtag(databaseContext, collection);
                    var lastProcessedTombstoneEtag = _indexStorage.ReadLastProcessedTombstoneEtag(indexContext.Transaction, collection);

                    if (lastTombstoneEtag > lastProcessedTombstoneEtag)
                        return true;
                }
                else
                {
                    if (Math.Min(cutoff.Value, lastDocEtag) > lastProcessedDocEtag)
                        return true;

                    if (DocumentDatabase.DocumentsStorage.GetNumberOfTombstonesWithDocumentEtagLowerThan(databaseContext, collection, cutoff.Value) > 0)
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
            using (CultureHelper.EnsureInvariantCulture())
            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(DocumentDatabase.DatabaseShutdown, _cancellationTokenSource.Token))
            {
                try
                {
                    DocumentDatabase.Notifications.OnDocumentChange += HandleDocumentChange;

                    while (true)
                    {
                        if (_logger.IsInfoEnabled)
                           _logger.Info($"Starting indexing for '{Name} ({IndexId})'.");

                        _mre.Reset();

                        var stats = new IndexingStatsAggregator(DocumentDatabase.IndexStore.Identities.GetNextIndexingStatsId());
                        using (var scope = stats.CreateScope())
                        {
                            try
                            {
                                cts.Token.ThrowIfCancellationRequested();

                                var didWork = DoIndexingWork(scope, cts.Token);

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

                        AddIndexingPerformance(stats);

                        try
                        {
                            _mre.Wait(cts.Token);
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

        protected abstract IIndexingWork[] CreateIndexWorkExecutors();

        public virtual IDisposable InitializeIndexingWork(TransactionOperationContext indexContext)
        {
            return null;
        }

        public bool DoIndexingWork(IndexingStatsScope stats, CancellationToken cancellationToken)
        {
            DocumentsOperationContext databaseContext;
            TransactionOperationContext indexContext;

            bool mightBeMore = false; ;

            using (CultureHelper.EnsureInvariantCulture())
            using (DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out databaseContext))
            using (_contextPool.AllocateOperationContext(out indexContext))
            using (var tx = indexContext.OpenWriteTransaction())
            using (CurrentIndexingScope.Current = new CurrentIndexingScope(DocumentDatabase.DocumentsStorage, databaseContext))
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
                            using (stats.For("Lucene_Write"))
                                writeOperation.Value.Dispose();
                        }
                    }

                    _indexStorage.WriteReferences(CurrentIndexingScope.Current, tx);
                }

                using (stats.For("Storage_Commit"))
                {
                    tx.Commit();
                }

                if (writeOperation.IsValueCreated)
                {
                    using (stats.For("Lucene_RecreateSearcher"))
                    {
                        IndexPersistence.RecreateSearcher(); // we need to recreate it after transaction commit to prevent it from seeing uncommitted changes
                    }
                }

                return mightBeMore;
            }
        }

        public abstract IIndexedDocumentsEnumerator GetMapEnumerator(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext);

        public abstract void HandleDelete(DocumentTombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats);

        public abstract void HandleMap(LazyStringValue key, IEnumerable mapResults, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats);

        private void HandleIndexChange(IndexChangeNotification notification)
        {
            if (string.Equals(notification.Name, Name, StringComparison.OrdinalIgnoreCase) == false)
                return;

            if (notification.Type == IndexChangeTypes.IndexMarkedAsErrored)
                Stop();
        }

        protected virtual void HandleDocumentChange(DocumentChangeNotification notification)
        {
            if (Collections.Contains(notification.CollectionName) == false)
                return;

            _mre.Set();
        }

        public List<IndexingError> GetErrors()
        {
            return _indexStorage.ReadErrors();
        }

        public void SetPriority(IndexingPriority priority)
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

        public void SetLock(IndexLockMode mode)
        {
            if (Definition.LockMode == mode)
                return;

            lock (_locker)
            {
                if (Definition.LockMode == mode)
                    return;

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Changing lock mode for '{Name} ({IndexId})' from '{Definition.LockMode}' to '{mode}'.");

                _indexStorage.WriteLock(mode);
            }
        }

        public IndexStats GetStats()
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenReadTransaction())
            {
                return ReadStats(tx);
            }
        }

        private IndexStats ReadStats(RavenTransaction tx)
        {
            using (var reader = IndexPersistence.OpenIndexReader(tx.InnerTransaction))
            {
                var stats = _indexStorage.ReadStats(tx);

                stats.Id = IndexId;
                stats.Name = Name;
                stats.Type = Type;
                stats.ForCollections = Collections.ToArray();
                stats.EntriesCount = reader.EntriesCount();
                stats.LockMode = Definition.LockMode;
                stats.Priority = Priority;

                stats.LastQueryingTime = _lastQueryingTime;

                return stats;
            }
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

        public async Task<DocumentQueryResult> Query(IndexQueryServerSide query, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            if (_disposed)
                throw new ObjectDisposedException($"Index '{Name} ({IndexId})' was already disposed.");

            if (Priority.HasFlag(IndexingPriority.Idle) && Priority.HasFlag(IndexingPriority.Forced) == false)
                SetPriority(IndexingPriority.Normal);

            MarkQueried(SystemTime.UtcNow);

            Transformer transformer = null;
            if (string.IsNullOrEmpty(query.Transformer) == false)
            {
                transformer = DocumentDatabase.TransformerStore.GetTransformer(query.Transformer);
                if (transformer == null)
                    throw new InvalidOperationException($"The transformer '{query.Transformer}' was not found.");
            }

            TransactionOperationContext indexContext;

            using (MarkQueryAsRunning(query, token))
            using (_contextPool.AllocateOperationContext(out indexContext))
            {
                var result = new DocumentQueryResult();

                var queryDuration = Stopwatch.StartNew();
                AsyncWaitForIndexing wait = null;

                while (true)
                {
                    using (var indexTx = indexContext.OpenReadTransaction())
                    {
                        documentsContext.OpenReadTransaction(); // we have to open read tx for mapResults _after_ we open index tx

                        if (query.WaitForNonStaleResultsAsOfNow && query.CutoffEtag == null)
                            query.CutoffEtag = Collections.Max(x => DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(documentsContext, x));

                        var isStale = IsStale(documentsContext, indexContext, query.CutoffEtag);

                        if (WillResultBeAcceptable(isStale, query, wait) == false)
                        {
                            documentsContext.Reset();
                            indexContext.Reset();

                            Debug.Assert(query.WaitForNonStaleResultsTimeout != null);

                            if (wait == null)
                                wait = new AsyncWaitForIndexing(queryDuration, query.WaitForNonStaleResultsTimeout.Value, _indexingBatchCompleted);

                            await wait.WaitForIndexingAsync().ConfigureAwait(false);
                            continue;
                        }

                        FillQueryResult(result, isStale, documentsContext, indexContext);

                        if (Type.IsMapReduce() && transformer == null)
                            documentsContext.Reset(); // map reduce don't need to access mapResults storage unless we have a transformer. Possible optimization: if we will know if transformer needs transaction then we may reset this here or not

                        using (var reader = IndexPersistence.OpenIndexReader(indexTx.InnerTransaction))
                        {
                            var totalResults = new Reference<int>();
                            var skippedResults = new Reference<int>();

                            var fieldsToFetch = new FieldsToFetch(query, Definition, transformer);
                            IEnumerable<Document> documents;

                            if (string.IsNullOrWhiteSpace(query.Query) || query.Query.Contains(Constants.IntersectSeparator) == false)
                            {
                                documents = reader.Query(query, fieldsToFetch, totalResults, skippedResults, GetQueryResultRetriever(documentsContext, indexContext, fieldsToFetch), token.Token);
                            }
                            else
                            {
                                documents = reader.IntersectQuery(query, fieldsToFetch, totalResults, skippedResults, GetQueryResultRetriever(documentsContext, indexContext, fieldsToFetch), token.Token);
                            }

                            var includeDocumentsCommand = new IncludeDocumentsCommand(DocumentDatabase.DocumentsStorage, documentsContext, query.Includes);

                            using (var scope = transformer?.OpenTransformationScope(query.TransformerParameters, includeDocumentsCommand, DocumentDatabase.DocumentsStorage, DocumentDatabase.TransformerStore, documentsContext))
                            {
                                var results = scope != null ? scope.Transform(documents) : documents;

                                foreach (var document in results)
                                {
                                    result.Results.Add(document);
                                    includeDocumentsCommand.Gather(document);
                                }
                            }

                            includeDocumentsCommand.Fill(result.Includes);
                            result.TotalResults = totalResults.Value;
                            result.SkippedResults = skippedResults.Value;
                        }

                        return result;
                    }
                }
            }
        }

        public TermsQueryResult GetTerms(string field, string fromValue, int pageSize, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            TransactionOperationContext indexContext;
            using (_contextPool.AllocateOperationContext(out indexContext))
            using (var tx = indexContext.OpenReadTransaction())
            {
                var result = new TermsQueryResult
                {
                    IndexName = Name,
                    ResultEtag = CalculateIndexEtag(IsStale(documentsContext, indexContext), documentsContext, indexContext)
                };

                using (var reader = IndexPersistence.OpenIndexReader(tx.InnerTransaction))
                {
                    result.Terms = reader.Terms(field, fromValue, pageSize);
                }

                return result;
            }
        }

        public MoreLikeThisQueryResultServerSide MoreLikeThisQuery(MoreLikeThisQueryServerSide query, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            TransactionOperationContext indexContext;
            using (_contextPool.AllocateOperationContext(out indexContext))
            using (var tx = indexContext.OpenReadTransaction())
            {
                HashSet<string> stopWords = null;
                if (string.IsNullOrWhiteSpace(query.StopWordsDocumentId) == false)
                {
                    var stopWordsDoc = DocumentDatabase.DocumentsStorage.Get(documentsContext, query.StopWordsDocumentId);
                    if (stopWordsDoc == null)
                        throw new InvalidOperationException("Stop words document " + query.StopWordsDocumentId + " could not be found");

                    BlittableJsonReaderArray value;
                    if (stopWordsDoc.Data.TryGet(nameof(StopWordsSetup.StopWords), out value) && value != null)
                    {
                        stopWords = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                        for (var i = 0; i < value.Length; i++)
                            stopWords.Add(value.GetStringByIndex(i));
                    }
                }

                var result = new MoreLikeThisQueryResultServerSide();

                var isStale = IsStale(documentsContext, indexContext);

                FillQueryResult(result, isStale, documentsContext, indexContext);

                using (var reader = IndexPersistence.OpenIndexReader(tx.InnerTransaction))
                {
                    var includeDocumentsCommand = new IncludeDocumentsCommand(DocumentDatabase.DocumentsStorage, documentsContext, query.Includes);
                    foreach (var document in reader.MoreLikeThis(query, stopWords, fieldsToFetch => GetQueryResultRetriever(documentsContext, indexContext, new FieldsToFetch(fieldsToFetch, Definition, null)), token.Token))
                    {
                        result.Results.Add(document);
                        includeDocumentsCommand.Gather(document);
                    }

                    includeDocumentsCommand.Fill(result.Includes);
                }

                return result;
            }
        }

        private void FillQueryResult<T>(QueryResultBase<T> result, bool isStale, DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {
            var stats = ReadStats(indexContext.Transaction);

            result.IndexName = Name;
            result.IsStale = isStale;
            result.IndexTimestamp = stats.LastIndexingTime ?? DateTime.MinValue;
            result.LastQueryTime = stats.LastQueryingTime ?? DateTime.MinValue;
            result.ResultEtag = CalculateIndexEtag(result.IsStale, documentsContext, indexContext);
        }

        private DisposableAction MarkQueryAsRunning(IndexQueryServerSide query, OperationCancelToken token)
        {
            var queryStartTime = DateTime.UtcNow;
            var queryId = Interlocked.Increment(ref _numberOfQueries);
            var executingQueryInfo = new ExecutingQueryInfo(queryStartTime, query, queryId, token);

            CurrentlyRunningQueries.Add(executingQueryInfo);

            return new DisposableAction(() =>
            {
                CurrentlyRunningQueries.TryRemove(executingQueryInfo);
            });
        }

        private static bool WillResultBeAcceptable(bool isStale, IndexQueryServerSide query, AsyncWaitForIndexing wait)
        {
            if (isStale == false)
                return true;

            if (query.WaitForNonStaleResultsTimeout == null)
                return true;

            if (wait != null && wait.TimeoutExceeded)
                return true;

            return false;
        }

        protected virtual unsafe long CalculateIndexEtag(bool isStale, DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
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
                    return (long)Hashing.XXHash64.Calculate((byte*)buffer, indexEtagBytes.Length * sizeof(long));
                }
            }
        }

        protected int CalculateIndexEtagInternal(long[] indexEtagBytes, bool isStale, DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
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
            DocumentsOperationContext documentContext;
            TransactionOperationContext indexContext;

            using (DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out documentContext))
            using (_contextPool.AllocateOperationContext(out indexContext))
            {
                using (indexContext.OpenReadTransaction())
                using (documentContext.OpenReadTransaction())
                {
                    return CalculateIndexEtag(IsStale(documentContext, indexContext), documentContext, indexContext);
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
            return _lastIndexingStats
                .Where(x => x.Id >= fromId)
                .Select(x => x.ToIndexingPerformanceStats())
                .ToArray();
        }

        public abstract IQueryResultRetriever GetQueryResultRetriever(DocumentsOperationContext documentsContext, TransactionOperationContext indexContext, FieldsToFetch fieldsToFetch);

        public abstract int? ActualMaxNumberOfIndexOutputs { get; }

        public abstract int MaxNumberOfIndexOutputs { get; }

        protected virtual bool EnsureValidNumberOfOutputsForDocument(int numberOfAlreadyProducedOutputs)
        {
            return numberOfAlreadyProducedOutputs <= MaxNumberOfIndexOutputs;
        }
    }
}