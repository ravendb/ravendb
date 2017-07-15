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
        public static async Task<(FacetQueryServerSide FacetQuery, long? FacetsEtag)> Parse(HttpContext httpContext, int start, int pageSize, JsonOperationContext context)
        {
            var result = new FacetQueryServerSide
            {
                Start = start,
                PageSize = pageSize
            };

            StringValues values;
            if (httpContext.Request.Query.TryGetValue("facetDoc", out values))
                result.FacetSetupDoc = values.First();

            if (httpContext.Request.Query.TryGetValue("query", out values))
                result.Query = values.First();

            if (httpContext.Request.Query.TryGetValue("cutOffEtag", out values))
                result.CutoffEtag = long.Parse(values.First());

            if (httpContext.Request.Query.TryGetValue("waitForNonStaleResultsAsOfNow", out values))
                result.WaitForNonStaleResultsAsOfNow = bool.Parse(values.First());

            if (httpContext.Request.Query.TryGetValue("waitForNonStaleResultsTimeout", out values))
                result.WaitForNonStaleResultsTimeout = TimeSpan.Parse(values.First());

            long? facetsEtag = null;
            if (httpContext.Request.Query.TryGetValue("facets", out values) && values.Count > 0)
            {
                var facets = await FacetedQueryParser.ParseFromStringAsync(values[0], context);
                result.Facets = facets.Facets;
                facetsEtag = facets.FacetsEtag;
            }

            return (result, facetsEtag);
        }

        public static unsafe (FacetQueryServerSide FacetQuery, long? FacetsEtag) Create(BlittableJsonReaderObject json)
        {
            var result = JsonDeserializationServer.FacetQuery(json);

            if (string.IsNullOrWhiteSpace(result.Query))
                throw new InvalidOperationException($"Facet query does not contain '{nameof(Query)}' field.");

            long? facetsEtag = null;

            if (json.TryGet(nameof(Facets), out BlittableJsonReaderArray facetsArray) && facetsArray != null)
                facetsEtag = Hashing.XXHash32.Calculate(facetsArray.Parent.BasePointer, facetsArray.Parent.Size);

            return (result, facetsEtag);
        }
    }
}