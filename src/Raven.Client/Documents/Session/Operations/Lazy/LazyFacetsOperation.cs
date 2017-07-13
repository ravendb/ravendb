using System;
using System.Net.Http;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.Operations.Lazy
{
    internal class LazyFacetsOperation : ILazyOperation
    {
        private readonly FacetQuery _query;

        public LazyFacetsOperation(FacetQuery query)
        {
            _query = query;
        }

        public GetRequest CreateRequest()
        {
            throw new NotImplementedException();

            /*
            var method = _query.CalculateHttpMethod();
            return new GetRequest
            {
                Url = "/queries/" + _query.IndexName,
                Query = "?" + _query.GetQueryString(method),
                Method = method.Method,
                Content = method == HttpMethod.Post ? _query.GetFacetsAsJson() : null
            };
            */
        }

        public object Result { get; private set; }
        public QueryResult QueryResult { get; set; }
        public bool RequiresRetry { get; private set; }

        public void HandleResponse(GetResponse response)
        {
            if (response.ForceRetry)
            {
                Result = null;
                RequiresRetry = true;
                return;
            }

            Result = JsonDeserializationClient.FacetedQueryResult((BlittableJsonReaderObject)response.Result);
        }
    }
}
