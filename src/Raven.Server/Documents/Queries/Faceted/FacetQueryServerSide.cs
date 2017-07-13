using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Queries.Facets;
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

        public static (FacetQueryServerSide FacetQuery, long? FacetsEtag) Create(BlittableJsonReaderObject json)
        {
            if (json.TryGet(nameof(Query), out string query) == false || string.IsNullOrEmpty(query))
                throw new InvalidOperationException($"Index query does not contain '{nameof(Query)}' field.");

            long? facetsEtag = null;
            var result = new FacetQueryServerSide(); // put 'query' here

            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
            foreach (var propertyIndex in json.GetPropertiesByInsertionOrder())
            {
                json.GetPropertyByIndex(propertyIndex, ref propertyDetails);

                switch (propertyDetails.Name)
                {
                    case nameof(Query):
                        continue;
                    case nameof(IndexName):
                        result.IndexName = propertyDetails.Value?.ToString();
                        break;
                    case nameof(FacetSetupDoc):
                        result.FacetSetupDoc = propertyDetails.Value?.ToString();
                        break;
                    case nameof(Facets):
                        var facetsArray = propertyDetails.Value as BlittableJsonReaderArray;
                        if (facetsArray == null || facetsArray.Length == 0)
                            continue;

                        var facets = FacetedQueryParser.ParseFromJson(facetsArray);
                        result.Facets = facets.Facets;
                        facetsEtag = facets.FacetsEtag;
                        break;
                    case nameof(CutoffEtag):
                        result.CutoffEtag = (long?)propertyDetails.Value;
                        break;
                    case nameof(PageSize):
                        result.PageSize = (int)propertyDetails.Value;
                        break;
                    case nameof(Start):
                        result.Start = (int)propertyDetails.Value;
                        break;
                    case nameof(WaitForNonStaleResultsTimeout):
                        if (propertyDetails.Value != null)
                            result.WaitForNonStaleResultsTimeout = TimeSpan.Parse(propertyDetails.Value.ToString());
                        break;
                    case nameof(WaitForNonStaleResults):
                        result.WaitForNonStaleResults = (bool)propertyDetails.Value;
                        break;
                    case nameof(WaitForNonStaleResultsAsOfNow):
                        result.WaitForNonStaleResultsAsOfNow = (bool)propertyDetails.Value;
                        break;
                    case nameof(QueryParameters):
                        result.QueryParameters = (BlittableJsonReaderObject)propertyDetails.Value;
                        break;
                }
            }

            return (result, facetsEtag);
        }
    }
}