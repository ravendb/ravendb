using System.Globalization;

using Raven.Server.ServerWide.Context;
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

        public Document Get(Lucene.Net.Documents.Document input)
        {
            var djv = new DynamicJsonValue();

            // TODO [ppekrol] handle IsDistinct, no Id then

            foreach (var field in input.GetFields())
            {
                if (field.Name.EndsWith("_Range"))
                {
                    var fieldName = field.Name.Substring(0, field.Name.Length - 6);
                    if (_fieldsToFetch.ContainsField(fieldName) == false)
                        continue;

                    djv[fieldName] = double.Parse(field.StringValue, CultureInfo.InvariantCulture);

                    continue;
                }

                if (_fieldsToFetch.ContainsField(field.Name) == false)
                    continue;

                djv[field.Name] = field.StringValue;
            }

            return new Document
            {
                Data = _indexContext.ReadObject(djv, "map-reduce result document")
            };
        }
    }
}