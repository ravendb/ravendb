using System;
using System.Linq;
using System.Net.Http;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Shard;
using Raven.NewClient.Json.Linq;

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

        public void HandleResponse(GetResponse response)
        {
            if (response.RequestHasErrors())
            {
                throw new InvalidOperationException("Got an unexpected response code for the request: " + response.Status + "\r\n" + response.Result);
            }

            var result = (RavenJObject)response.Result;
            Result = result.JsonDeserialization<FacetedQueryResult>();
        }

        public void HandleResponses(GetResponse[] responses, ShardStrategy shardStrategy)
        {
            var result = new FacetedQueryResult();

            foreach (var response in responses.Select(response => (RavenJObject)response.Result))
            {
                var facet = response.JsonDeserialization<FacetedQueryResult>();
                foreach (var facetResult in facet.Results)
                {
                    if (!result.Results.ContainsKey(facetResult.Key))
                        result.Results[facetResult.Key] = new FacetResult();

                    var newFacetResult = result.Results[facetResult.Key];
                    foreach (var facetValue in facetResult.Value.Values)
                    {
                        var existingFacetValueRange = newFacetResult.Values.Find((x) => x.Range == facetValue.Range);
                        if (existingFacetValueRange != null)
                            existingFacetValueRange.Hits += facetValue.Hits;
                        else
                            newFacetResult.Values.Add(new FacetValue() { Hits = facetValue.Hits, Range = facetValue.Range });
                    }

                    foreach (var facetTerm in facetResult.Value.RemainingTerms)
                    {
                        if (!newFacetResult.RemainingTerms.Contains(facetTerm))
                            newFacetResult.RemainingTerms.Add(facetTerm);
                    }
                }
            }

            Result = result;
        }

        public IDisposable EnterContext()
        {
            return null;
        }
    }
}
