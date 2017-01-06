using System;
using System.Linq;
using System.Net.Http;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Shard;
using Sparrow.Json;


namespace Raven.NewClient.Client.Document.Batches
{
    public class LazyFacetsOperation : ILazyOperation
    {
        private readonly FacetQuery _query;

        public LazyFacetsOperation(FacetQuery query)
        {
            _query = query;
        }

        public GetRequest CreateRequest()
        {
            var method = _query.CalculateHttpMethod();
            return new GetRequest
            {
                Url = "/queries/" + _query.IndexName,
                Query = _query.GetQueryString(method),
                Method = method.Method,
                Content = method == HttpMethod.Post ? _query.GetFacetsAsJson() : null
            };
        }

        public object Result { get; private set; }
        public QueryResult QueryResult { get; set; }
        public bool RequiresRetry { get; private set; }

        public void HandleResponse(BlittableJsonReaderObject response)
        {
            throw new NotImplementedException();
            /*if (response.RequestHasErrors())
            {
                throw new InvalidOperationException("Got an unexpected response code for the request: " + response.Status + "\r\n" + response.Result);
            }

            var result = (RavenJObject)response.Result;
            Result = result.JsonDeserialization<FacetedQueryResult>();*/
        }
    }
}
