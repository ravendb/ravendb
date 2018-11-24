using Lucene.Net.Store;
using Raven.Client;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries.Results
{
    public class MapReduceQueryResultRetriever : QueryResultRetrieverBase
    {
        private readonly JsonOperationContext _context;
        private QueryTimingsScope _storageScope;

        public MapReduceQueryResultRetriever(DocumentDatabase database, IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsStorage documentsStorage, JsonOperationContext context, FieldsToFetch fieldsToFetch, IncludeDocumentsCommand includeDocumentsCommand)
            : base(database, query, queryTimings, fieldsToFetch, documentsStorage, context, true, includeDocumentsCommand)
        {
            _context = context;
        }

        protected override Document LoadDocument(string id)
        {
            if (DocumentsStorage != null &&
                _context is DocumentsOperationContext ctx)
                return DocumentsStorage.Get(ctx, id);
            // can happen during some debug endpoints that should never load a document
            return null;
        }

        protected override long? GetCounter(string docId, string name)
        {
            if (DocumentsStorage != null &&
                _context is DocumentsOperationContext ctx)
                return DocumentsStorage.CountersStorage.GetCounterValue(ctx, docId, name);
            return null;
        }

        protected override DynamicJsonValue GetCounterRaw(string docId, string name)
        {
            if (DocumentsStorage == null || !(_context is DocumentsOperationContext ctx))
                return null;

            var djv = new DynamicJsonValue();

            foreach (var (cv, val) in DocumentsStorage.CountersStorage.GetCounterValues(ctx, docId, name))
            {
                djv[cv] = val;
            }

            return djv;
        }

        protected override unsafe Document DirectGet(Lucene.Net.Documents.Document input, string id, IState state)
        {
            var reduceValue = input.GetField(Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName).GetBinaryValue(state);

            var allocation = _context.GetMemory(reduceValue.Length);

            UnmanagedWriteBuffer buffer = new UnmanagedWriteBuffer(_context, allocation);
            buffer.Write(reduceValue, 0, reduceValue.Length);

            var result = new BlittableJsonReaderObject(allocation.Address, reduceValue.Length, _context, buffer);

            return new Document
            {
                Data = result
            };
        }

        public override Document Get(Lucene.Net.Documents.Document input, float score, IState state)
        {
            if (FieldsToFetch.IsProjection)
                return GetProjection(input, score, null, state);

            using (_storageScope = _storageScope?.Start() ?? RetrieverScope?.For(nameof(QueryTimingsScope.Names.Storage)))
            {
                var doc = DirectGet(input, null, state);

                if (doc != null)
                    doc.IndexScore = score;

                return doc;
            }
        }

        public override bool TryGetKey(Lucene.Net.Documents.Document document, IState state, out string key)
        {
            key = null;
            return false;
        }
    }
}
