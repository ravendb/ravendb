using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.Persistance.Lucene;
using Raven.Server.Documents.Indexes.Persistance.Lucene.Documents;
using Raven.Server.Documents.Queries;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

using Voron;

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
        private static readonly Slice TypeSlice = "Type";

        private static readonly Slice LastMappedEtagSlice = "LastMappedEtag";

        private static readonly Slice LastReducedEtagSlice = "LastReducedEtag";

        private static readonly Slice LastTombstoneEtagSlice = "LastTombstoneEtag";

        protected readonly LuceneIndexPersistance IndexPersistance;

        private readonly object _locker = new object();

        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();

        private DocumentDatabase _documentDatabase;

        private Task _indexingTask;

        private bool _initialized;

        private UnmanagedBuffersPool _unmanagedBuffersPool;

        private StorageEnvironment _environment;

        private TransactionContextPool _contextPool;

        private bool _disposed;

        private readonly ManualResetEventSlim _mre = new ManualResetEventSlim();

        protected Index(int indexId, IndexType type, IndexDefinitionBase definition)
        {
            if (indexId <= 0)
                throw new ArgumentException("IndexId must be greater than zero.", nameof(indexId));

            IndexId = indexId;
            Type = type;
            Definition = definition;
            IndexPersistance = new LuceneIndexPersistance(indexId, definition);
            Collections = new HashSet<string>(Definition.Collections, StringComparer.OrdinalIgnoreCase);
        }
        
        public static Index Open(int indexId, string path, DocumentDatabase documentDatabase)
        {
            var options = StorageEnvironmentOptions.ForPath(path);
            try
            {
                options.SchemaVersion = 1;

                var environment = new StorageEnvironment(options);
                using (var tx = environment.ReadTransaction())
                {
                    var statsTree = tx.ReadTree("Stats");
                    var result = statsTree.Read(TypeSlice);
                    if (result == null)
                        throw new InvalidOperationException($"Stats tree does not contain 'Type' entry in index '{indexId}'.");

                    var type = (IndexType)result.Reader.ReadLittleEndianInt32();

                    switch (type)
                    {
                        case IndexType.Auto:
                            return AutoIndex.Open(indexId, environment, documentDatabase);
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

        public IndexDefinitionBase Definition { get; }

        public string Name => Definition?.Name;

        public bool ShouldRun { get; private set; } = true;

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

        protected unsafe void Initialize(StorageEnvironment environment, DocumentDatabase documentDatabase)
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

                    _documentDatabase = documentDatabase;
                    _environment = environment;
                    _unmanagedBuffersPool = new UnmanagedBuffersPool($"Indexes//{IndexId}");
                    _contextPool = new TransactionContextPool(_unmanagedBuffersPool, _environment);

                    using (var tx = _environment.WriteTransaction())
                    {
                        var typeInt = (int)Type;

                        var statsTree = tx.CreateTree("Stats");
                        statsTree.Add(TypeSlice, new Slice((byte*)&typeInt, sizeof(int)));

                        tx.Commit();
                    }

                    IndexPersistance.Initialize(_documentDatabase.Configuration.Indexing);

                    _initialized = true;
                }
                catch (Exception)
                {
                    Dispose();
                    throw;
                }
            }
        }

        public void Execute(CancellationToken cancellationToken)
        {
            if (_disposed)
                throw new ObjectDisposedException($"Index '{Name} ({IndexId})' was already disposed.");

            if (_initialized == false)
                throw new InvalidOperationException($"Index '{Name} ({IndexId})' was not initialized.");

            lock (_locker)
            {
                if (_indexingTask != null)
                    throw new InvalidOperationException($"Index '{Name} ({IndexId})' is executing.");

                _indexingTask = Task.Factory.StartNew(() => ExecuteIndexing(cancellationToken), TaskCreationOptions.LongRunning);
            }
        }

        public void Dispose()
        {
            lock (_locker)
            {
                if (_disposed)
                    throw new ObjectDisposedException($"Index '{Name} ({IndexId})' was already disposed.");

                _disposed = true;

                _cancellationTokenSource.Cancel();

                _indexingTask?.Wait();
                _indexingTask = null;
                
                _environment?.Dispose();
                _environment = null;

                _unmanagedBuffersPool?.Dispose();
                _unmanagedBuffersPool = null;

                _contextPool?.Dispose();
                _contextPool = null;
            }
        }

        protected HashSet<string> Collections;

        protected abstract bool IsStale(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, out long lastEtag);

        public long GetLastMappedEtag()
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            {
                using (var tx = context.OpenReadTransaction())
                {
                    return ReadLastMappedEtag(tx);
                }
            }
        }

        public long GetLastTombstoneEtag()
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            {
                using (var tx = context.OpenReadTransaction())
                {
                    return ReadLastTombstoneEtag(tx);
                }
            }
        }

        protected long ReadLastTombstoneEtag(RavenTransaction tx)
        {
            return ReadLastEtag(tx, LastTombstoneEtagSlice);
        }

        protected long ReadLastMappedEtag(RavenTransaction tx)
        {
            return ReadLastEtag(tx, LastMappedEtagSlice);
        }

        protected long ReadLastReducedEtag(RavenTransaction tx)
        {
            return ReadLastEtag(tx, LastReducedEtagSlice);
        }

        private static long ReadLastEtag(RavenTransaction tx, Slice key)
        {
            var statsTree = tx.InnerTransaction.CreateTree("Stats");
            var readResult = statsTree.Read(key);
            long lastEtag = 0;
            if (readResult != null)
                lastEtag = readResult.Reader.ReadLittleEndianInt64();

            return lastEtag;
        }

        private static void WriteLastTombstoneEtag(RavenTransaction tx, long etag)
        {
            WriteLastEtag(tx, LastTombstoneEtagSlice, etag);
        }

        private static void WriteLastMappedEtag(RavenTransaction tx, long etag)
        {
            WriteLastEtag(tx, LastMappedEtagSlice, etag);
        }

        private static void WriteLastReducedEtag(RavenTransaction tx, long etag)
        {
            WriteLastEtag(tx, LastReducedEtagSlice, etag);
        }

        private static unsafe void WriteLastEtag(RavenTransaction tx, Slice key, long etag)
        {
            var statsTree = tx.InnerTransaction.CreateTree("Stats");
            statsTree.Add(key, new Slice((byte*)&etag, sizeof(long)));
        }

        private void ExecuteIndexing(CancellationToken cancellationToken)
        {
            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cancellationTokenSource.Token))
            {
                try
                {
                    _documentDatabase.Notifications.OnDocumentChange += HandleDocumentChange;

                    while (ShouldRun)
                    {
                        try
                        {
                            _mre.Reset();

                            cts.Token.ThrowIfCancellationRequested();

                            ExecuteCleanup(cts.Token);
                            ExecuteMap(cts.Token);

                            _mre.Wait(cts.Token);
                        }
                        catch (OutOfMemoryException oome)
                        {
                            // TODO
                        }
                        catch (AggregateException ae)
                        {
                            // TODO
                        }
                        catch (OperationCanceledException)
                        {
                            return;
                        }
                        catch (Exception e)
                        {
                            // TODO
                        }
                    }
                }
                finally
                {
                    _documentDatabase.Notifications.OnDocumentChange -= HandleDocumentChange;
                }
            }
        }

        private void ExecuteCleanup(CancellationToken token)
        {
            DocumentsOperationContext databaseContext;
            TransactionOperationContext indexContext;

            using (_documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out databaseContext))
            using (_contextPool.AllocateOperationContext(out indexContext))
            {
                databaseContext.OpenReadTransaction();
                indexContext.OpenReadTransaction();
                //TODO: This need to be done on a per collection basis
                var lastMappedEtag = ReadLastMappedEtag(indexContext.Transaction);
                var lastTombstoneEtag = ReadLastTombstoneEtag(indexContext.Transaction);

                var count = 0;
                var lastEtag = lastTombstoneEtag;
                var pageSize = _documentDatabase.Configuration.Indexing.MaxNumberOfTombstonesToFetch;

                using (var indexActions = IndexPersistance.Write())
                {
                    var sw = Stopwatch.StartNew();
                    foreach (var tombstone in _documentDatabase.DocumentsStorage.GetTombstonesAfter(databaseContext, lastEtag, 0, pageSize))
                    {
                        token.ThrowIfCancellationRequested();

                        count++;
                        lastEtag = tombstone.Etag;

                        if (tombstone.DeletedEtag > lastMappedEtag)
                            continue; // no-op, we have not yet indexed this document

                        indexActions.Delete(tombstone.Key);

                        if (sw.Elapsed > _documentDatabase.Configuration.Indexing.TombstoneProcessingTimeout.AsTimeSpan)
                        {
                            break;
                        }
                    }
                }

                if (count == 0)
                    return;

                _mre.Set(); // might be more

                using (var tx = indexContext.OpenWriteTransaction())
                {
                    WriteLastTombstoneEtag(tx, lastEtag);

                    tx.Commit();
                }
            }
        }

        private void HandleDocumentChange(DocumentChangeNotification notification)
        {
            if (Collections.Contains(notification.CollectionName) == false)
                return;

            _mre.Set();
        }

        private void ExecuteMap(CancellationToken cancellationToken)
        {
            DocumentsOperationContext databaseContext;
            TransactionOperationContext indexContext;

            using (_documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out databaseContext))
            using (_contextPool.AllocateOperationContext(out indexContext))
            {
                long lastMappedEtag;
                if (IsStale(databaseContext, indexContext, out lastMappedEtag) == false)
                    return;

                var startEtag = lastMappedEtag + 1;
                // TODO: need to avoid the lambda usage here, and have separate etag per collection
                var etags = Collections.ToDictionary(x => x, x => lastMappedEtag);
                var pageSize = _documentDatabase.Configuration.Indexing.MaxNumberOfDocumentsToFetchForMap;

                using (var indexActions = IndexPersistance.Write())
                {
                    foreach (var collection in Collections)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        var count = 0;
                        using (databaseContext.OpenReadTransaction())
                        {
                            var sw = Stopwatch.StartNew();
                            var fetchedTotalSizeInBytes = new Size(0, SizeUnit.Bytes);
                            foreach (var document in _documentDatabase.DocumentsStorage.GetDocumentsAfter(databaseContext, collection, startEtag, 0, pageSize))
                            {
                                cancellationToken.ThrowIfCancellationRequested();

                                count++;
                                fetchedTotalSizeInBytes.Add(document.Data.Size, SizeUnit.Bytes);
                                etags[collection] = document.Etag;
                                
                                try
                                {
                                    indexActions.Write(document);
                                }
                                catch (Exception)
                                {
                                    // TODO [ppekrol] log?
                                    continue;
                                }

                                if (sw.Elapsed > _documentDatabase.Configuration.Indexing.DocumentProcessingTimeout.AsTimeSpan || 
                                    //TODO: I don't think that this is needed now, we read from mmap, after all
                                    fetchedTotalSizeInBytes >= _documentDatabase.Configuration.Indexing.MaximumSizeAllowedToFetchFromStorageInMb)
                                {
                                    break;
                                }
                            }
                        }

                        if (count == 0)
                            return;

                        _mre.Set(); // might be more
                    }
                }
                // TODO: let us avoid using Linq here, it does a lot of allocations
                // TODO: that we can avoid
                var lastEtag = etags
                    .Select(x => x.Value)
                    .Where(x => x > lastMappedEtag)
                    .DefaultIfEmpty(0)
                    .Min();

                if (lastEtag == 0)
                    return;

                //TODO: This is wrong, we shouldn't be using a single etag value
                //TODO: we need to use an etag value per collection, and work on that
                using (var tx = indexContext.OpenWriteTransaction())
                {
                    WriteLastMappedEtag(tx, lastEtag);

                    tx.Commit();
                }
            }
        }

        private void ExecuteReduce()
        {
        }

        public DocumentQueryResult Query(IndexQuery query, DocumentsOperationContext context, CancellationToken token)
        {
            if (_disposed)
                throw new ObjectDisposedException($"Index '{Name} ({IndexId})' was already disposed.");

            TransactionOperationContext indexContext;
            var result = new DocumentQueryResult();

            using (_contextPool.AllocateOperationContext(out indexContext))
            {
                long lastEtag;
                result.IsStale = IsStale(context, indexContext, out lastEtag);
                result.IndexEtag = lastEtag;
            }

            List<string> documentIds;
            using (var indexRead = IndexPersistance.Read())
            {
                documentIds = indexRead.Query(query, token).ToList();
            }

            context.OpenReadTransaction();

            foreach (var id in documentIds)
            {
                var document = _documentDatabase.DocumentsStorage.Get(context, id);

                result.Results.Add(document);
            }


            return result;
        }
    }
}