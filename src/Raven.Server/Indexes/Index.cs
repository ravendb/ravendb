using System;
using System.Collections.Generic;

using Raven.Server.Documents;
using Raven.Server.Indexes.Storage.Lucene;
using Raven.Server.Json;

namespace Raven.Server.Indexes
{
    public abstract class Index
    {
        protected readonly LuceneIndexStorage IndexStorage;

        private readonly RavenOperationContext _context;

        private readonly DocumentsStorage _documentsStorage;

        private readonly HashSet<string> _forCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        protected Index(RavenOperationContext context, DocumentsStorage documentsStorage)
        {
            _context = context;
            _documentsStorage = documentsStorage;
            IndexStorage = new LuceneIndexStorage();
        }

        public int IndexId { get; private set; }

        public string PublicName { get; private set; }

        public void Execute()
        {
            while (IsStale())
            {
                ExecuteMap();
                ExecuteReduce();
            }
        }

        protected abstract bool IsStale();

        protected abstract Lucene.Net.Documents.Document ConvertDocument(string collection, Document document);

        private void ExecuteMap()
        {
            foreach (var collection in _forCollections)
            {
                var start = 0;
                const int PageSize = 1024 * 10;

                while (true)
                {
                    var count = 0;
                    var indexDocuments = new List<Lucene.Net.Documents.Document>();
                    using (var tx = _context.Environment.ReadTransaction())
                    {
                        _context.Transaction = tx;

                        foreach (var document in _documentsStorage.GetDocumentsAfter(_context, collection, 0, start, PageSize))
                        {
                            indexDocuments.Add(ConvertDocument(collection, document));
                            count++;
                        }
                    }

                    using (var tx = _context.Environment.WriteTransaction())
                    {
                        IndexStorage.Write(_context, indexDocuments);

                        tx.Commit();
                    }

                    if (count < PageSize)
                        break;

                    start += PageSize;
                }
            }
        }

        private void ExecuteReduce()
        {
        }
    }
}