using System;
using System.Globalization;
using System.Linq;

using Raven.Client.Data;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries.Results
{
    public class MapReduceQueryResultRetriever : IQueryResultRetriever
    {
        private readonly TransactionOperationContext _indexContext;

        private readonly string[] _fieldsToFetch;

        public MapReduceQueryResultRetriever(TransactionOperationContext indexContext, string[] fieldsToFetch)
        {
            _indexContext = indexContext;
            _fieldsToFetch = fieldsToFetch;
        }

        public Document Get(Lucene.Net.Documents.Document input)
        {
            var djv = new DynamicJsonValue();

            foreach (var field in input.GetFields())
            {
                if (field.Name.EndsWith("_Range"))
                {
                    var fieldName = field.Name.Substring(0, field.Name.Length - 6);
                    if (IncludeField(fieldName) == false)
                        continue;

                    djv[fieldName] = double.Parse(field.StringValue, CultureInfo.InvariantCulture);

                    continue;
                }

                if (IncludeField(field.Name) == false)
                    continue;

                djv[field.Name] = field.StringValue;
            }

            return new Document
            {
                Data = _indexContext.ReadObject(djv, "map-reduce result document")
            };
        }

        private bool IncludeField(string name)
        {
            if (_fieldsToFetch != null && _fieldsToFetch.Contains(name, StringComparer.OrdinalIgnoreCase) == false)
                return false;

            return true;
        }
    }
}