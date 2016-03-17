using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.Persistance.Lucene;
using Raven.Server.Documents.Queries;
using Raven.Server.Exceptions;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

using Voron;
using Voron.Data.Tables;

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

    public abstract class Index : IDisposable
    {
        private class Schema
        {
            public static readonly string StatsTree = "Stats";

            public static readonly string EtagsMapTree = "Etags.Map";

            public static readonly string EtagsTombstoneTree = "Etags.Tombstone";

            public static readonly Slice TypeSlice = "Type";

            public static readonly Slice CreatedTimestampSlice = "CreatedTimestamp";

            public static readonly Slice IndexingAttemptsSlice = "IndexingAttempts";

            public static readonly Slice IndexingSuccessesSlice = "IndexingSuccesses";

            public static readonly Slice IndexingErrorsSlice = "IndexingErrors";

            public static readonly Slice LastIndexingTimeSlice = "LastIndexingTime";

            public static readonly Slice PrioritySlice = "Priority";
        }

        private readonly TableSchema _errorsSchema = new TableSchema();

        private long writeErrors;

        private const long WriteErrorsLimit = 10;

        public const int MaxNumberOfKeptErrors = 500;

        protected readonly ILog Log = LogManager.GetLogger(typeof(Index));

        protected readonly LuceneIndexPersistence IndexPersistence;

        private readonly object _locker = new object();

        private CancellationTokenSource _cancellationTokenSource;

        protected DocumentDatabase DocumentDatabase;

        private Thread _indexingThread;

        private bool _initialized;

        private UnmanagedBuffersPool _unmanagedBuffersPool;

        private StorageEnvironment _environment;

        protected TransactionContextPool _contextPool;

        private bool _disposed;

        protected readonly ManualResetEventSlim _mre = new ManualResetEventSlim();

        private DateTime? lastQueryingTime; // TODO [ppekrol] do we need to persist this?

        protected Index(int indexId, IndexType type, IndexDefinitionBase definition)
        {
            if (indexId <= 0)
                throw new ArgumentException("IndexId must be greater than zero.", nameof(indexId));

            IndexId = indexId;
            Type = type;
            Definition = definition;
            IndexPersistence = new LuceneIndexPersistence(indexId, definition);
            Collections = new HashSet<string>(Definition.Collections, StringComparer.OrdinalIgnoreCase);
        }

        public static Index Open(int indexId, DocumentDatabase documentDatabase)
        {
            var options = StorageEnvironmentOptions.ForPath(Path.Combine(documentDatabase.Configuration.Indexing.IndexStoragePath, indexId.ToString()));
            try
            {
                options.SchemaVersion = 1;

                var environment = new StorageEnvironment(options);
                using (var tx = environment.ReadTransaction())
                {
                    var statsTree = tx.ReadTree(Schema.StatsTree);
                    var result = statsTree.Read(Schema.TypeSlice);
                    if (result == null)
                        throw new InvalidOperationException($"Stats tree does not contain 'Type' entry in index '{indexId}'.");

                    var type = (IndexType)result.Reader.ReadLittleEndianInt32();

                    switch (type)
                    {
                        case IndexType.AutoMap:
                            return AutoMapIndex.Open(indexId, environment, documentDatabase);
                        default:
                            throw new NotImplementedException();
                    }
                }
            }
            catch (Exception)
            {
                options.Dispose();
                throw;
            }
        }

        public int IndexId { get; }

        public IndexType Type { get; }

        public IndexingPriority Priority { get; private set; }

        public IndexDefinitionBase Definition { get; }

        public string Name => Definition?.Name;

        public bool IsRunning => _indexingThread != null;

        protected void Initialize(DocumentDatabase documentDatabase)
        {
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
                    _contextPool = new TransactionContextPool(_unmanagedBuffersPool, _environment);

                    CreateSchema();

                    IndexPersistence.Initialize(_environment, DocumentDatabase.Configuration.Indexing);

                    DocumentDatabase.Notifications.OnIndexChange += HandleIndexChange;

                    _initialized = true;
                }
                catch (Exception)
                {
                    Dispose();
                    throw;
                }
            }
        }

        private unsafe void CreateSchema()
        {
            _errorsSchema.DefineIndex("ErrorTimestamps", new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                IsGlobal = true,
                Name = "ErrorTimestamps"
            });

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                _errorsSchema.Create(tx.InnerTransaction, "Errors");

                var typeInt = (int)Type;

                var statsTree = tx.InnerTransaction.CreateTree(Schema.StatsTree);
                statsTree.Add(Schema.TypeSlice, new Slice((byte*)&typeInt, sizeof(int)));

                if (statsTree.ReadVersion(Schema.CreatedTimestampSlice) == 0)
                {
                    var binaryDate = SystemTime.UtcNow.ToBinary();
                    statsTree.Add(Schema.CreatedTimestampSlice, new Slice((byte*)&binaryDate, sizeof(long)));
                }

                var priority = statsTree.Read(Schema.PrioritySlice);
                if (priority == null)
                    Priority = IndexingPriority.Normal;
                else
                    Priority = (IndexingPriority)priority.Reader.ReadLittleEndianInt32();

                tx.InnerTransaction.CreateTree(Schema.EtagsMapTree);
                tx.InnerTransaction.CreateTree(Schema.EtagsTombstoneTree);

                Definition.Persist(context);

                tx.Commit();
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

                DocumentDatabase.Notifications.OnIndexChange -= HandleIndexChange;

                _indexingThread?.Join();
                _indexingThread = null;

                _environment?.Dispose();
                _environment = null;

                _unmanagedBuffersPool?.Dispose();
                _unmanagedBuffersPool = null;

                _contextPool?.Dispose();
                _contextPool = null;
            }
        }

        protected HashSet<string> Collections;

        protected virtual bool IsStale(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext)
        {
            using (databaseContext.OpenReadTransaction())
            {
                foreach (var collection in Collections)
                {
                    var lastCollectionEtag = DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(databaseContext, collection);
                    var lastProcessedCollectionEtag = ReadLastMappedEtag(indexContext.Transaction, collection);

                    if (lastCollectionEtag > lastProcessedCollectionEtag)
                        return true;

                    var lastCollectionTombstoneEtag = DocumentDatabase.DocumentsStorage.GetLastTombstoneEtag(indexContext, collection);
                    var lastProcessedCollectionTombstoneEtag = ReadLastTombstoneEtag(indexContext.Transaction, collection);

                    if (lastCollectionTombstoneEtag > lastProcessedCollectionTombstoneEtag)
                        return true;
                }

                return false;
            }
        }

        public long GetLastMappedEtagFor(string collection)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            {
                using (var tx = context.OpenReadTransaction())
                {
                    return ReadLastMappedEtag(tx, collection);
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
                        etags[collection] = ReadLastMappedEtag(tx, collection);
                    }

                    return etags;
                }
            }
        }

        /// <summary>
        /// This should only be used for testing purposes.
        /// </summary>
        internal Dictionary<string, long> GetLastTombstoneEtagsForDebug()
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            {
                using (var tx = context.OpenReadTransaction())
                {
                    var etags = new Dictionary<string, long>();
                    foreach (var collection in Collections)
                    {
                        etags[collection] = ReadLastTombstoneEtag(tx, collection);
                    }

                    return etags;
                }
            }
        }

        protected long ReadLastTombstoneEtag(RavenTransaction tx, string collection)
        {
            return ReadLastEtag(tx, Schema.EtagsTombstoneTree, collection);
        }

        protected long ReadLastMappedEtag(RavenTransaction tx, string collection)
        {
            return ReadLastEtag(tx, Schema.EtagsMapTree, collection);
        }

        private static long ReadLastEtag(RavenTransaction tx, string tree, string collection)
        {
            var statsTree = tx.InnerTransaction.CreateTree(tree);
            var readResult = statsTree.Read(collection);
            long lastEtag = 0;
            if (readResult != null)
                lastEtag = readResult.Reader.ReadLittleEndianInt64();

            return lastEtag;
        }

        protected void WriteLastTombstoneEtag(RavenTransaction tx, string collection, long etag)
        {
            WriteLastEtag(tx, Schema.EtagsTombstoneTree, collection, etag);
        }

        protected void WriteLastMappedEtag(RavenTransaction tx, string collection, long etag)
        {
            WriteLastEtag(tx, Schema.EtagsMapTree, collection, etag);
        }

        private unsafe void WriteLastEtag(RavenTransaction tx, string tree, string collection, long etag)
        {
            if (Log.IsDebugEnabled)
                Log.Debug($"Writing last etag for '{Name} ({IndexId})'. Tree: {tree}. Collection: {collection}. Etag: {etag}.");

            var statsTree = tx.InnerTransaction.CreateTree(tree);
            statsTree.Add(collection, new Slice((byte*)&etag, sizeof(long)));
        }

        protected void ExecuteIndexing()
        {
            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(DocumentDatabase.DatabaseShutdown, _cancellationTokenSource.Token))
            {
                try
                {
                    DocumentDatabase.Notifications.OnDocumentChange += HandleDocumentChange;

                    while (true)
                    {
                        if (Log.IsDebugEnabled)
                            Log.Debug($"Starting indexing for '{Name} ({IndexId})'.'");

                        _mre.Reset();

                        var startTime = SystemTime.UtcNow;
                        var stats = new IndexingBatchStats();
                        try
                        {
                            cts.Token.ThrowIfCancellationRequested();

                            DoIndexingWork(stats, cts.Token);

                            DocumentDatabase.Notifications.RaiseNotifications(new IndexChangeNotification
                            {
                                Name = Name,
                                Type = IndexChangeTypes.BatchCompleted
                            });

                            ResetWriteErrors();

                            if (Log.IsDebugEnabled)
                                Log.Debug($"Finished indexing for '{Name} ({IndexId})'.'");
                        }
                        catch (OutOfMemoryException oome)
                        {
                            Log.WarnException($"Out of memory occured for '{Name} ({IndexId})'.", oome);
                            // TODO [ppekrol] GC?
                        }
                        catch (IndexWriteException iwe)
                        {
                            HandleWriteErrors(stats, iwe);
                        }
                        catch (IndexAnalyzerException iae)
                        {
                            stats.AddAnalyzerError(iae);
                        }
                        catch (OperationCanceledException)
                        {
                            return;
                        }
                        catch (Exception e)
                        {
                            Log.WarnException($"Exception occured for '{Name} ({IndexId})'.", e);
                        }

                        try
                        {
                            UpdateStats(startTime, stats);
                        }
                        catch (Exception e)
                        {
                            Log.ErrorException($"Could not update stats for '{Name} ({IndexId})'.", e);
                        }

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
            writeErrors = Interlocked.Exchange(ref writeErrors, 0);
        }

        internal void HandleWriteErrors(IndexingBatchStats stats, IndexWriteException iwe)
        {
            stats.AddWriteError(iwe);

            if (iwe.InnerException is SystemException) // Don't count transient errors
                return;

            writeErrors = Interlocked.Increment(ref writeErrors);

            if (Priority == IndexingPriority.Error || Interlocked.Read(ref writeErrors) < WriteErrorsLimit)
                return;

            SetPriority(IndexingPriority.Error);
        }

        public abstract void DoIndexingWork(IndexingBatchStats stats, CancellationToken cancellationToken);

        private void HandleIndexChange(IndexChangeNotification notification)
        {
            if (string.Equals(notification.Name, Name, StringComparison.OrdinalIgnoreCase) == false)
                return;

            if (notification.Type == IndexChangeTypes.IndexMarkedAsErrored)
                Stop();
        }

        private void HandleDocumentChange(DocumentChangeNotification notification)
        {
            if (Collections.Contains(notification.CollectionName) == false)
                return;

            _mre.Set();
        }

        public unsafe List<IndexingError> GetErrors()
        {
            var errors = new List<IndexingError>();

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenReadTransaction())
            {
                var table = new Table(_errorsSchema, "Errors", tx.InnerTransaction);

                foreach (var sr in table.SeekForwardFrom(_errorsSchema.Indexes["ErrorTimestamps"], Slice.BeforeAllKeys))
                {
                    foreach (var tvr in sr.Results)
                    {
                        int size;
                        var error = new IndexingError();

                        var ptr = tvr.Read(0, out size);
                        error.Timestamp = new DateTime(IPAddress.NetworkToHostOrder(*(long*)ptr), DateTimeKind.Utc);

                        ptr = tvr.Read(1, out size);
                        error.Document = new LazyStringValue(null, ptr, size, context);

                        ptr = tvr.Read(2, out size);
                        error.Action = new LazyStringValue(null, ptr, size, context);

                        ptr = tvr.Read(3, out size);
                        error.Error = new LazyStringValue(null, ptr, size, context);

                        errors.Add(error);
                    }
                }
            }

            return errors;
        }

        internal unsafe void UpdateStats(DateTime indexingTime, IndexingBatchStats stats)
        {
            if (Log.IsDebugEnabled)
                Log.Debug($"Updating statistics for '{Name} ({IndexId})'. Stats: {stats}.");

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var table = new Table(_errorsSchema, "Errors", tx.InnerTransaction);

                var statsTree = tx.InnerTransaction.ReadTree(Schema.StatsTree);

                statsTree.Increment(Schema.IndexingAttemptsSlice, stats.IndexingAttempts);
                statsTree.Increment(Schema.IndexingSuccessesSlice, stats.IndexingSuccesses);
                statsTree.Increment(Schema.IndexingErrorsSlice, stats.IndexingErrors);

                var binaryDate = indexingTime.ToBinary();
                statsTree.Add(Schema.LastIndexingTimeSlice, new Slice((byte*)&binaryDate, sizeof(long)));

                if (stats.Errors != null)
                {
                    foreach (var error in stats.Errors)
                    {
                        var ticksBigEndian = IPAddress.HostToNetworkOrder(error.Timestamp.Ticks);
                        var document = context.GetLazyString(error.Document);
                        var action = context.GetLazyString(error.Action);
                        var e = context.GetLazyString(error.Error);

                        var tvb = new TableValueBuilder
                                      {
                                          { (byte*)&ticksBigEndian, sizeof(long) },
                                          { document.Buffer, document.Size },
                                          { action.Buffer, action.Size },
                                          { e.Buffer, e.Size }
                                      };

                        table.Insert(tvb);
                    }

                    CleanupErrors(table);
                }

                tx.Commit();
            }
        }

        private void CleanupErrors(Table table)
        {
            if (table.NumberOfEntries <= MaxNumberOfKeptErrors)
                return;

            var take = table.NumberOfEntries - MaxNumberOfKeptErrors;
            foreach (var sr in table.SeekForwardFrom(_errorsSchema.Indexes["ErrorTimestamps"], Slice.BeforeAllKeys))
            {
                foreach (var tvr in sr.Results)
                {
                    if (take-- <= 0)
                        return;

                    table.Delete(tvr.Id);
                }
            }
        }

        public unsafe void SetPriority(IndexingPriority priority)
        {
            if (Priority == priority)
                return;

            lock (_locker)
            {
                if (Priority == priority)
                    return;

                if (Log.IsDebugEnabled)
                    Log.Debug($"Changing priority for '{Name} ({IndexId})' from '{Priority}' to '{priority}'.");

                TransactionOperationContext context;
                using (_contextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var statsTree = tx.InnerTransaction.ReadTree(Schema.StatsTree);
                    var priorityInt = (int)priority;
                    statsTree.Add(Schema.PrioritySlice, new Slice((byte*)&priorityInt, sizeof(int)));

                    tx.Commit();
                }

                Priority = priority;

                if (priority == IndexingPriority.Error)
                {
                    DocumentDatabase.Notifications.RaiseNotifications(new IndexChangeNotification
                    {
                        Name = Name,
                        Type = IndexChangeTypes.IndexMarkedAsErrored
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

                if (Log.IsDebugEnabled)
                    Log.Debug($"Changing lock mode for '{Name} ({IndexId})' from '{Definition.LockMode}' to '{mode}'.");

                TransactionOperationContext context;
                using (_contextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var oldLockMode = Definition.LockMode;
                    try
                    {
                        Definition.LockMode = mode;
                        Definition.Persist(context);

                        tx.Commit();
                    }
                    catch (Exception)
                    {
                        Definition.LockMode = oldLockMode;
                        throw;
                    }
                }
            }
        }

        public IndexStats GetStats()
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenReadTransaction())
            using (var reader = IndexPersistence.OpenIndexReader(tx.InnerTransaction))
            {
                var statsTree = tx.InnerTransaction.ReadTree(Schema.StatsTree);
                var table = new Table(_errorsSchema, "Errors", tx.InnerTransaction);

                var stats = new IndexStats();
                stats.Id = IndexId;
                stats.Name = Name;
                stats.Type = Type;
                stats.ForCollections = Collections.ToArray();
                stats.EntriesCount = reader.EntriesCount();
                stats.IsInMemory = _environment.Options is StorageEnvironmentOptions.PureMemoryStorageEnvironmentOptions;
                stats.CreatedTimestamp = DateTime.FromBinary(statsTree.Read(Schema.CreatedTimestampSlice).Reader.ReadLittleEndianInt64());
                stats.LockMode = Definition.LockMode;
                stats.Priority = Priority;
                stats.ErrorsCount = (int)table.NumberOfEntries;

                var lastIndexingTime = statsTree.Read(Schema.LastIndexingTimeSlice);
                if (lastIndexingTime != null)
                {
                    stats.LastIndexingTime = DateTime.FromBinary(lastIndexingTime.Reader.ReadLittleEndianInt64());
                    stats.IndexingAttempts = statsTree.Read(Schema.IndexingAttemptsSlice).Reader.ReadLittleEndianInt32();
                    stats.IndexingErrors = statsTree.Read(Schema.IndexingErrorsSlice).Reader.ReadLittleEndianInt32();
                    stats.IndexingSuccesses = statsTree.Read(Schema.IndexingAttemptsSlice).Reader.ReadLittleEndianInt32();

                    stats.LastIndexedEtags = new Dictionary<string, long>();
                    foreach (var collection in Collections)
                        stats.LastIndexedEtags[collection] = ReadLastMappedEtag(tx, collection);
                }

                stats.LastQueryingTime = lastQueryingTime;

                return stats;
            }
        }

        private void MarkQueried(DateTime time)
        {
            if (lastQueryingTime != null &&
                lastQueryingTime.Value >= time)
                return;

            lastQueryingTime = time;
        }

        public DocumentQueryResult Query(IndexQuery query, DocumentsOperationContext documentsContext, CancellationToken token)
        {
            if (_disposed)
                throw new ObjectDisposedException($"Index '{Name} ({IndexId})' was already disposed.");

            MarkQueried(SystemTime.UtcNow);

            TransactionOperationContext indexContext;
            var result = new DocumentQueryResult
            {
                IndexName = Name
            };

            using (_contextPool.AllocateOperationContext(out indexContext))
            {
                using (var tx = indexContext.OpenReadTransaction())
                {
                    result.IsStale = IsStale(documentsContext, indexContext);

                    Reference<int> totalResults = new Reference<int>();
                    List<string> documentIds;

                    using (var indexRead = IndexPersistence.OpenIndexReader(tx.InnerTransaction))
                    {
                        documentIds = indexRead.Query(query, token, totalResults).ToList();
                    }

                    result.TotalResults = totalResults.Value;

                    documentsContext.OpenReadTransaction();

                    foreach (var id in documentIds)
                    {
                        token.ThrowIfCancellationRequested();

                        var document = DocumentDatabase.DocumentsStorage.Get(documentsContext, id);

                        result.Results.Add(document);
                    }

                    return result;
                }
            }
        }
    }
}