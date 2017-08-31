using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Queries.Facets;
using Raven.Server.Json;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Faceted
{
    public class FacetQueryServerSide : FacetQuery<BlittableJsonReaderObject>
    {
        [JsonIgnore]
        public QueryMetadata Metadata { get; private set; }

        public static async Task<(FacetQueryServerSide FacetQuery, long? FacetsEtag)> Create(HttpContext httpContext, int start, int pageSize, JsonOperationContext context)
        {
            var result = new FacetQueryServerSide
            {
                Start = start,
                PageSize = pageSize
            };

            if (httpContext.Request.Query.TryGetValue("facetDoc", out StringValues values))
                result.FacetSetupDoc = values.First();

            if (httpContext.Request.Query.TryGetValue("query", out values))
                result.Query = values.First();

            if (httpContext.Request.Query.TryGetValue("cutOffEtag", out values))
                result.CutoffEtag = long.Parse(values.First());

            if (httpContext.Request.Query.TryGetValue("waitForNonStaleResultsAsOfNow", out values))
                result.WaitForNonStaleResultsAsOfNow = bool.Parse(values.First());

            if (httpContext.Request.Query.TryGetValue("waitForNonStaleResultsTimeoutInMs", out values))
                result.WaitForNonStaleResultsTimeout = TimeSpan.FromMilliseconds(long.Parse(values.First()));

            long? facetsEtag = null;
            if (httpContext.Request.Query.TryGetValue("facets", out values) && values.Count > 0)
            {
                var facets = await FacetedQueryParser.ParseFromStringAsync(values[0], context);
                result.Facets = facets.Facets;
                facetsEtag = facets.FacetsEtag;
            }

            result.Metadata = new QueryMetadata(result.Query, result.QueryParameters, 0);
            return (result, facetsEtag);
        }

        public static unsafe (FacetQueryServerSide FacetQuery, long? FacetsEtag) Create(BlittableJsonReaderObject json, JsonOperationContext context, QueryMetadataCache cache)
        {
            var result = JsonDeserializationServer.FacetQuery(json);

            if (result.PageSize == 0 && json.TryGet(nameof(PageSize), out int _) == false)
                result.PageSize = int.MaxValue;

            if (string.IsNullOrWhiteSpace(result.Query))
                throw new InvalidOperationException($"Facet query does not contain '{nameof(Query)}' field.");

            long? facetsEtag = null;

            if (json.TryGet(nameof(Facets), out BlittableJsonReaderArray facetsArray) && facetsArray != null)
                facetsEtag = Hashing.XXHash32.Calculate(facetsArray.Parent.BasePointer, facetsArray.Parent.Size);

            result.Metadata = cache.TryGetMetadata(result, context, out var metadataHash, out var metadata)
                ? metadata
                : new QueryMetadata(result.Query, result.QueryParameters, metadataHash);
            return (result, facetsEtag);
        }
    }
}
