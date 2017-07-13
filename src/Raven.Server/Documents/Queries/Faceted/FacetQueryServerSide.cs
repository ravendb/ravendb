using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Queries.Facets;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Faceted
{
    public class FacetQueryServerSide : FacetQuery<BlittableJsonReaderObject>
    {
        public static FacetQueryServerSide Parse(HttpContext httpContext, int start, int pageSize, JsonOperationContext context)
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

            return result;
        }

        public static FacetQueryServerSide Create(BlittableJsonReaderObject json)
        {
            throw new NotImplementedException();
        }
    }
}