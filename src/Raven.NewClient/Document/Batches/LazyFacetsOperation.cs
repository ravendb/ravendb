using System.Net.Http;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Json;
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
                Query = "?" + _query.GetQueryString(method),
                Method = method.Method,
                Content = method == HttpMethod.Post ? _query.GetFacetsAsJson() : null
            };
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
