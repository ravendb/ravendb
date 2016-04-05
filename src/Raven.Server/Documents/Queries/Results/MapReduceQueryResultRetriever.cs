using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries.Results
{
    public class MapReduceQueryResultRetriever : IQueryResultRetriever
    {
        private readonly TransactionOperationContext _indexContext;

        public MapReduceQueryResultRetriever(TransactionOperationContext indexContext)
        {
            _indexContext = indexContext;
        }

        public Document Get(Lucene.Net.Documents.Document input)
        {
            var djv = new DynamicJsonValue();

            foreach (var field in input.GetFields())
            {
                djv[field.Name] = field.StringValue;
            }

            return new Document
            {
                Data = _indexContext.ReadObject(djv, "map-reduce result document")
            };
        }
    }
}