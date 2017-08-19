using Lucene.Net.Store;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Results
{
    public class MapReduceQueryResultRetriever : QueryResultRetrieverBase
    {
        private readonly JsonOperationContext _context;

        public MapReduceQueryResultRetriever(IndexQueryServerSide query, DocumentsStorage documentsStorage, JsonOperationContext context, FieldsToFetch fieldsToFetch)
            : base(query, fieldsToFetch, documentsStorage, context, true)
        {
            _context = context;
        }

        protected override Document LoadDocument(string id)
        {
            if(_documentsStorage != null && 
                _context is DocumentsOperationContext ctx)
                return _documentsStorage.Get(ctx, id);
            // can happen during some debug endpoints that should never load a document
            return null; 

        }

        protected override unsafe Document DirectGet(Lucene.Net.Documents.Document input, string id, IState state)
        {
            var reduceValue = input.GetField(Constants.Documents.Indexing.Fields.ReduceValueFieldName).GetBinaryValue(state);

            var result = new BlittableJsonReaderObject((byte*)_context.PinObjectAndGetAddress(reduceValue), reduceValue.Length, _context);

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
