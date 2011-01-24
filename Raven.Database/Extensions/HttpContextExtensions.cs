//-----------------------------------------------------------------------
// <copyright file="HttpContextExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using Raven.Database.Data;
using Raven.Database.Server.Responders;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;

namespace Raven.Database.Extensions
{
    public static class HttpContextExtensions
    {
        public static IndexQuery GetIndexQueryFromHttpContext(this IHttpContext context, int maxPageSize)
        {
            var query = new IndexQuery
            {
                Query = context.Request.QueryString["query"] ?? "",
                Start = context.GetStart(),
                Cutoff = context.GetCutOff(),
                PageSize = context.GetPageSize(maxPageSize),
                SkipTransformResults = context.GetSkipTransformResults(),
                FieldsToFetch = context.Request.QueryString.GetValues("fetch"),
				GroupBy = context.Request.QueryString.GetValues("groupBy"),
				AggregationOperation = context.GetAggregationOperation(),
                SortedFields = context.Request.QueryString.GetValues("sort")
                    .EmptyIfNull()
                    .Select(x => new SortedField(x))
                    .ToArray()
            };

            double lat = context.GetLat(), lng = context.GetLng(), radius = context.GetRadius();
            if (lat != 0 || lng != 0 || radius != 0)
            {
                return new SpatialIndexQuery(query)
                {
                    Latitude = lat,
                    Longitude = lng,
                    Radius = radius,
                    SortByDistance = context.SortByDistance()
                };
            }
            return query;
        }
        
    }
}