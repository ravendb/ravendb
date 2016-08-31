using Raven.Abstractions.Data;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries.Results
{
    public class MapReduceQueryResultRetriever : IQueryResultRetriever
    {
        private readonly TransactionOperationContext _indexContext;

        private readonly FieldsToFetch _fieldsToFetch;

        public MapReduceQueryResultRetriever(TransactionOperationContext indexContext, FieldsToFetch fieldsToFetch)
        {
            _indexContext = indexContext;
            _fieldsToFetch = fieldsToFetch;
        }

        public unsafe Document Get(Lucene.Net.Documents.Document input, float score)
        {
            // TODO [ppekrol] handle IsDistinct, no Id then

            var reduceValue = input.GetField(Constants.Indexing.Fields.ReduceValueFieldName).GetBinaryValue();

            var result = new BlittableJsonReaderObject((byte*)_indexContext.PinObjectAndGetAddress(reduceValue),
                reduceValue.Length, _indexContext);

            if (_fieldsToFetch.IsProjection)
            {
                foreach (var name in result.GetPropertyNames())
                {
                    if (_fieldsToFetch.ContainsField(name))
                        continue;

                    if (result.Modifications == null)
                        result.Modifications = new DynamicJsonValue(result);

                    result.Modifications.Remove(name);
                }

                result = _indexContext.ReadObject(result, "map-reduce result document");
            }

            return new Document
            {
                Data = result
            };
        }

        public bool TryGetKey(Lucene.Net.Documents.Document document, out string key)
        {
            key = null;
            return false;
        }
    }
}