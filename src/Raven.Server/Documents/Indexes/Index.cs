using System;
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
using Raven.Server.Documents.Tasks;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;

using Voron;
using Voron.Impl;

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

        protected readonly LuceneIndexPersistance IndexPersistence;

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
            IndexPersistence = new LuceneIndexPersistance();

            DocumentConverter = new LuceneDocumentConverter(definition.MapFields);
        }

        public LuceneDocumentConverter DocumentConverter { get; }

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

                    IndexPersistence.Initialize(_documentDatabase.Configuration.Indexing);

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

                DocumentConverter?.Dispose();

                _environment?.Dispose();
                _environment = null;

                _unmanagedBuffersPool?.Dispose();
                _unmanagedBuffersPool = null;

                _contextPool?.Dispose();
                _contextPool = null;
            }
        }

        protected string[] Collections => Definition.Collections;

        protected abstract bool IsStale(TransactionOperationContext databaseContext, TransactionOperationContext indexContext, out long lastEtag);

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

        protected long ReadLastMappedEtag(Transaction tx)
        {
            return ReadLastEtag(tx, LastMappedEtagSlice);
        }

        protected long ReadLastReducedEtag(Transaction tx)
        {
            return ReadLastEtag(tx, LastReducedEtagSlice);
        }

        private static long ReadLastEtag(Transaction tx, Slice key)
        {
            var statsTree = tx.CreateTree("Stats");
            var readResult = statsTree.Read(key);
            long lastEtag = 0;
            if (readResult != null)
                lastEtag = readResult.Reader.ReadLittleEndianInt64();

            return lastEtag;
        }

        private void WriteLastMappedEtag(Transaction tx, long etag)
        {
            WriteLastEtag(tx, LastMappedEtagSlice, etag);
        }

        private void WriteLastReducedEtag(Transaction tx, long etag)
        {
            WriteLastEtag(tx, LastReducedEtagSlice, etag);
        }

        private static unsafe void WriteLastEtag(Transaction tx, Slice key, long etag)
        {
            var statsTree = tx.CreateTree("Stats");
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
            DocumentsOperationContext context;
            using (_documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenWriteTransaction();

                var task = _documentDatabase.TasksStorage.GetMergedTask(context, IndexId, DocumentsTask.DocumentsTaskType.RemoveFromIndex);
                if (task == null)
                    return;

                task.Execute(context, token);

                context.Transaction.Commit();
            }
        }

        private void HandleDocumentChange(DocumentChangeNotification notification)
        {
            if (_mre.IsSet)
                return;

            if (notification.Type != DocumentChangeTypes.Put && notification.Type != DocumentChangeTypes.Delete)
                return;

            if (Collections.Any(x => string.Equals(x, notification.CollectionName, StringComparison.OrdinalIgnoreCase)) == false)
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
                long lastEtag;
                if (IsStale(databaseContext, indexContext, out lastEtag) == false)
                    return;

                lastEtag++;
                var pageSize = _documentDatabase.Configuration.Indexing.MaxNumberOfItemsToFetchForMap;

                foreach (var collection in Collections)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var start = 0;

                    IndexPersistence.Write(addToIndex =>
                    {
                        while (true)
                        {
                            var count = 0;
                            var earlyExit = false;

                            using (databaseContext.OpenReadTransaction())
                            {
                                var sw = Stopwatch.StartNew();
                                var fetchedTotalSize = 0;
                                foreach (var document in _documentDatabase.DocumentsStorage.GetDocumentsAfter(databaseContext, collection, lastEtag, start, pageSize))
                                {
                                    cancellationToken.ThrowIfCancellationRequested();

                                    count++;
                                    fetchedTotalSize += document.Data.Size;
                                    lastEtag = document.Etag;

                                    Debug.Assert(document.Etag >= lastEtag);

                                    Lucene.Net.Documents.Document convertedDocument;

                                    try
                                    {
                                        convertedDocument = DocumentConverter.ConvertToCachedDocument(document);
                                    }
                                    catch (Exception)
                                    {
                                        // TODO [ppekrol] log that conversion failed, we need to keep going, add indexing errors
                                        continue;
                                    }

                                    if (convertedDocument == null)
                                        continue;

                                    try
                                    {
                                        addToIndex(convertedDocument);
                                    }
                                    catch (Exception)
                                    {
                                        // TODO [ppekrol] log?
                                        continue;
                                    }

                                    if (sw.Elapsed > _documentDatabase.Configuration.Indexing.DocumentProcessingTimeout.AsTimeSpan || fetchedTotalSize >= _documentDatabase.Configuration.Indexing.MaximumSizeAllowedToFetchFromStorageInMb.GetValue(SizeUnit.Bytes))
                                    {
                                        earlyExit = count != pageSize;
                                        break;
                                    }
                                }
                            }

                            if (count == 0)
                                break;

                            using (var tx = indexContext.OpenWriteTransaction())
                            {
                                WriteLastMappedEtag(tx, lastEtag);

                                tx.Commit();
                            }

                            if (earlyExit)
                            {
                                start += count;
                                continue;
                            }

                            if (count < pageSize)
                                break;

                            start += count;
                        }
                    });
                }
            }
        }

        private void ExecuteReduce()
        {
        }

        public QueryResult Query(IndexQuery query)
        {
            if (_disposed)
                throw new ObjectDisposedException($"Index '{Name} ({IndexId})' was already disposed.");

            throw new NotImplementedException();
        }
    }
}