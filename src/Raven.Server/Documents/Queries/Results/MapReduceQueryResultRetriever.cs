using Raven.Abstractions.Data;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Results
{
    public class MapReduceQueryResultRetriever : QueryResultRetrieverBase
    {
        private readonly JsonOperationContext _context;

        public MapReduceQueryResultRetriever(JsonOperationContext context, FieldsToFetch fieldsToFetch)
            : base(fieldsToFetch, context)
        {
            _context = context;
        }

        protected override unsafe Document DirectGet(Lucene.Net.Documents.Document input, string id)
        {
            var reduceValue = input.GetField(Constants.Indexing.Fields.ReduceValueFieldName).GetBinaryValue();

            var result = new BlittableJsonReaderObject((byte*)_context.PinObjectAndGetAddress(reduceValue), reduceValue.Length, _context);

            return new Document
            {
                Data = result
            };
        }

        public override Document Get(Lucene.Net.Documents.Document input, float score)
        {
            if (_fieldsToFetch.IsProjection || _fieldsToFetch.IsTransformation)
                return GetProjection(input, score, null);

            return DirectGet(input, null);
        }

        public override bool TryGetKey(Lucene.Net.Documents.Document document, out string key)
        {
            key = null;
            return false;
        }
    }
}