using Lucene.Net.Store;
using Raven.Client;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Results
{
    public class MapReduceQueryResultRetriever : QueryResultRetrieverBase
    {
        private readonly JsonOperationContext _context;

        public MapReduceQueryResultRetriever(JsonOperationContext context, FieldsToFetch fieldsToFetch)
            : base(fieldsToFetch, context, true)
        {
            _context = context;
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
            if (FieldsToFetch.IsProjection || FieldsToFetch.IsTransformation)
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