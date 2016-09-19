using Raven.Abstractions.Data;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries.Results
{
    public class MapReduceQueryResultRetriever : QueryResultRetrieverBase
    {
        private readonly TransactionOperationContext _indexContext;

        public MapReduceQueryResultRetriever(TransactionOperationContext indexContext, FieldsToFetch fieldsToFetch)
            : base(fieldsToFetch, indexContext)
        {
            _indexContext = indexContext;
        }

        protected unsafe override Document DirectGet(Lucene.Net.Documents.Document input, string id)
        {
            var reduceValue = input.GetField(Constants.Indexing.Fields.ReduceValueFieldName).GetBinaryValue();

            var result = new BlittableJsonReaderObject((byte*)_indexContext.PinObjectAndGetAddress(reduceValue),
                reduceValue.Length, _indexContext);

            return new Document
            {
                Data = result
            };
        }

        public unsafe override Document Get(Lucene.Net.Documents.Document input, float score)
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