using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

using Raven.Server.Documents;
using Raven.Server.Indexes.Persistance.Lucene;
using Raven.Server.Json;
using Raven.Server.ServerWide;

using Voron;
using Voron.Impl;

namespace Raven.Server.Indexes
{
    public abstract class Index : IDisposable
    {
        private static readonly Slice LastMappedEtagSlice = "LastMappedEtag";

        private static readonly Slice LastReducedEtagSlice = "LastReducedEtag";

        protected readonly LuceneIndexPersistance IndexPersistance;

        private readonly object _locker = new object();

        private readonly DocumentsStorage _documentsStorage;

        private readonly HashSet<string> _forCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        private Task _indexingTask;

        private bool _initialized;

        private UnmanagedBuffersPool _unmanagedBuffersPool;

        private StorageEnvironment _environment;

        private ContextPool _contextPool;

        protected Index(int indexId, DocumentsStorage documentsStorage)
        {
            if (indexId <= 0)
                throw new ArgumentException("IndexId must be greater than zero.", nameof(indexId));

            IndexId = indexId;
            _documentsStorage = documentsStorage;
            IndexPersistance = new LuceneIndexPersistance();
        }

        public int IndexId { get; }

        public string PublicName { get; private set; }

        public bool ShouldRun { get; private set; }

        public void Initialize()
        {
            if (_initialized)
                throw new InvalidOperationException();

            lock (_locker)
            {
                if (_initialized)
                    throw new InvalidOperationException();

                var options = _documentsStorage.Configuration.Core.RunInMemory
                    ? StorageEnvironmentOptions.CreateMemoryOnly()
                    : StorageEnvironmentOptions.ForPath(Path.Combine(_documentsStorage.Configuration.Core.IndexStoragePath, IndexId.ToString()));

                try
                {
                    Initialize(options);
                }
                catch (Exception)
                {
                    options.Dispose();
                    throw;
                }

                _initialized = true;
            }
        }

        private void Initialize(StorageEnvironmentOptions options)
        {
            options.SchemaVersion = 1;
            try
            {
                _environment = new StorageEnvironment(options);
                _unmanagedBuffersPool = new UnmanagedBuffersPool($"Indexes//{IndexId}");
                _contextPool = new ContextPool(_unmanagedBuffersPool, _environment);

                using (var tx = _environment.WriteTransaction())
                {
                    tx.Commit();
                }
            }
            catch (Exception)
            {
                options.Dispose();
                Dispose();
                throw;
            }
        }

        public void Execute()
        {
            if (_initialized == false)
                throw new InvalidOperationException();

            if (_indexingTask != null)
                throw new InvalidOperationException();

            lock (_locker)
            {
                if (_indexingTask != null)
                    throw new InvalidOperationException();

                _indexingTask = Task.Factory.StartNew(ExecuteIndexing, TaskCreationOptions.LongRunning);
            }
        }

        public void Dispose()
        {
            _indexingTask?.Wait();
            _indexingTask = null;

            _environment?.Dispose();
            _environment = null;

            _unmanagedBuffersPool?.Dispose();
            _unmanagedBuffersPool = null;

            _contextPool?.Dispose();
            _contextPool = null;
        }

        protected abstract bool IsStale(RavenOperationContext databaseContext, RavenOperationContext indexContext);

        protected abstract Lucene.Net.Documents.Document ConvertDocument(string collection, Document document);

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
            var tree = tx.CreateTree("Etags");
            var readResult = tree.Read(key);
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
            var tree = tx.CreateTree("Etags");
            tree.Add(key, new Slice((byte*)&etag, sizeof(long)));
        }

        private void ExecuteIndexing()
        {
            while (ShouldRun)
            {
                bool foundWork;
                try
                {
                    foundWork = ExecuteMap();
                }
                catch (OutOfMemoryException oome)
                {
                    foundWork = true;
                    // TODO
                }
                catch (AggregateException ae)
                {
                    foundWork = true;
                    // TODO
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception e)
                {
                    foundWork = true;
                    // TODO
                }

                if (foundWork == false && ShouldRun)
                {
                    // cleanup tasks here
                }
            }
        }

        private bool ExecuteMap()
        {
            RavenOperationContext databaseContext;
            RavenOperationContext indexContext;
            using (_documentsStorage.ContextPool.AllocateOperationContext(out databaseContext))
            using (_documentsStorage.ContextPool.AllocateOperationContext(out indexContext))
            {
                if (IsStale(databaseContext, indexContext) == false)
                    return false;

                var foundWork = false;
                foreach (var collection in _forCollections)
                {
                    var start = 0;
                    const int PageSize = 1024 * 10;

                    while (true)
                    {
                        var count = 0;
                        long lastEtag = 0;
                        var indexDocuments = new List<Lucene.Net.Documents.Document>();
                        using (var tx = databaseContext.Environment.ReadTransaction())
                        {
                            databaseContext.Transaction = tx;

                            lastEtag = ReadLastMappedEtag(tx);

                            foreach (var document in _documentsStorage.GetDocumentsAfter(databaseContext, collection, lastEtag, start, PageSize))
                            {
                                indexDocuments.Add(ConvertDocument(collection, document));
                                count++;

                                Debug.Assert(document.Etag > lastEtag);

                                lastEtag = document.Etag;
                            }
                        }

                        foundWork = foundWork || indexDocuments.Count > 0;

                        using (var tx = indexContext.Environment.WriteTransaction())
                        {
                            indexContext.Transaction = tx;

                            IndexPersistance.Write(indexContext, indexDocuments);
                            WriteLastMappedEtag(tx, lastEtag);

                            tx.Commit();
                        }

                        if (count < PageSize) break;

                        start += PageSize;
                    }
                }

                return foundWork;
            }
        }

        private void ExecuteReduce()
        {
        }
    }
}