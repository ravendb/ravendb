using Lucene.Net.Store;
using Raven.Client;
using Raven.Server.Documents.Includes;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Results
{
    public class MapReduceQueryResultRetriever : QueryResultRetrieverBase
    {
        private readonly JsonOperationContext _context;

        public MapReduceQueryResultRetriever(DocumentDatabase database, IndexQueryServerSide query, DocumentsStorage documentsStorage, JsonOperationContext context, FieldsToFetch fieldsToFetch, IncludeDocumentsCommand includeDocumentsCommand)
            : base(database, query, fieldsToFetch, documentsStorage, context, true, includeDocumentsCommand)
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

            return DirectGet(input, null, state);
        }

        public override bool TryGetKey(Lucene.Net.Documents.Document document, IState state, out string key)
        {
            key = null;
            return false;
        }
    }
}
