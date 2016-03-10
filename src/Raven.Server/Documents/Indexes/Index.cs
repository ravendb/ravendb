using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.Persistance.Lucene;
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
        private static readonly string EtagsMap = "Etags.Map";

        private static readonly string EtagsTombstone = "Etags.Tombstone";

        private static readonly Slice TypeSlice = "Type";

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
                    var statsTree = tx.ReadTree("Stats");
                    var result = statsTree.Read(TypeSlice);
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

                    DocumentDatabase = documentDatabase;
                    _environment = environment;
                    _unmanagedBuffersPool = new UnmanagedBuffersPool($"Indexes//{IndexId}");
                    _contextPool = new TransactionContextPool(_unmanagedBuffersPool, _environment);

                    TransactionOperationContext context;
                    using (_contextPool.AllocateOperationContext(out context))
                    using (var tx = context.OpenWriteTransaction())
                    {
                        var typeInt = (int)Type;

                        var statsTree = tx.InnerTransaction.CreateTree("Stats");
                        statsTree.Add(TypeSlice, new Slice((byte*)&typeInt, sizeof(int)));

                        tx.InnerTransaction.CreateTree(EtagsMap);
                        tx.InnerTransaction.CreateTree(EtagsTombstone);

                        Definition.Persist(context);

                        tx.Commit();
                    }

                    IndexPersistence.Initialize(_environment, DocumentDatabase.Configuration.Indexing);

                    _initialized = true;
                }
                catch (Exception)
                {
                    Dispose();
                    throw;
                }
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
            return ReadLastEtag(tx, EtagsTombstone, collection);
        }

        protected long ReadLastMappedEtag(RavenTransaction tx, string collection)
        {
            return ReadLastEtag(tx, EtagsMap, collection);
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

        protected static void WriteLastTombstoneEtag(RavenTransaction tx, string collection, long etag)
        {
            WriteLastEtag(tx, EtagsTombstone, collection, etag);
        }

        protected static void WriteLastMappedEtag(RavenTransaction tx, string collection, long etag)
        {
            WriteLastEtag(tx, EtagsMap, collection, etag);
        }

        private static unsafe void WriteLastEtag(RavenTransaction tx, string tree, string collection, long etag)
        {
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
                        try
                        {
                            _mre.Reset();

                            cts.Token.ThrowIfCancellationRequested();

                            DoIndexingWork(cts.Token);

                            DocumentDatabase.Notifications.RaiseNotifications(new IndexChangeNotification
                            {
                                Name = Name,
                                Type = IndexChangeTypes.BatchCompleted
                            });

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
                    DocumentDatabase.Notifications.OnDocumentChange -= HandleDocumentChange;
                }
            }
        }

        public abstract void DoIndexingWork(CancellationToken cancellationToken);

        private void HandleDocumentChange(DocumentChangeNotification notification)
        {
            if (Collections.Contains(notification.CollectionName) == false)
                return;

            _mre.Set();
        }

        public DocumentQueryResult Query(IndexQuery query, DocumentsOperationContext documentsContext, CancellationToken token)
        {
            if (_disposed)
                throw new ObjectDisposedException($"Index '{Name} ({IndexId})' was already disposed.");

            TransactionOperationContext indexContext;
            var result = new DocumentQueryResult()
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
                        var document = DocumentDatabase.DocumentsStorage.Get(documentsContext, id);

                        result.Results.Add(document);
                    }

                    return result;
                }
            }
        }
    }
}