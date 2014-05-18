//-----------------------------------------------------------------------
// <copyright file="HttpContextExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Database.Server.Abstractions;
using Raven.Json.Linq;

namespace Raven.Database.Extensions
{
	public static class HttpContextExtensions
	{
		public static Facet[] GetFacetsFromHttpContext(this IHttpContext context)
		{
			var dictionary = new Dictionary<string, Facet>();

			foreach (var facetString in context.Request.QueryString.AllKeys
				.Where(x=>x.StartsWith("facet.", StringComparison.OrdinalIgnoreCase))
				.ToArray())
			{
				var parts = facetString.Split(new[] { '.' }, StringSplitOptions.RemoveEmptyEntries);
				if (parts.Length != 3)
					throw new InvalidOperationException("Could not parse query parameter: " + facetString);

				var fieldName = parts[1];

				Facet facet;
				if (dictionary.TryGetValue(fieldName, out facet) == false)
					dictionary[fieldName] = facet = new Facet { Name = fieldName };

				foreach (var value in context.Request.QueryString.GetValues(facetString) ?? Enumerable.Empty<string>())
				{
					switch (parts[2].ToLowerInvariant())
					{
						case "mode":
							FacetMode mode;
							if (Enum.TryParse(value, true, out mode) == false)
								throw new InvalidOperationException("Could not parse " + facetString + "=" + value);

							facet.Mode = mode;
							break;
						case "range":
							facet.Ranges.Add(value);
							break;
					}
				}
			}
			return dictionary.Values.ToArray();
		}

		public static string GetFacetSetupDocFromHttpContext(this IHttpContext context)
		{
			return context.Request.QueryString["facetDoc"] ?? "";
		}

		public static int GetFacetStartFromHttpContext(this IHttpContext context)
		{
			int start;
			return int.TryParse(context.Request.QueryString["facetStart"], out start) ? start : 0;
		}

		public static int? GetFacetPageSizeFromHttpContext(this IHttpContext context)
		{
			int pageSize;
			if (int.TryParse(context.Request.QueryString["facetPageSize"], out pageSize))
				return pageSize;
			return null;
		}

        public static Dictionary<string, RavenJToken> ExtractQueryInputs(this IHttpContext context)
        {
            var result = new Dictionary<string, RavenJToken>();
            foreach (var key in context.Request.QueryString.AllKeys)
            {
                if (string.IsNullOrEmpty(key)) continue;
                if (key.StartsWith("qp-"))
                {
                    var realkey = key.Substring(3);
                    result[realkey] = context.Request.QueryString[key];
                }
            }
            return result;
        }

		public static IndexQuery GetIndexQueryFromHttpContext(this IHttpContext context, int maxPageSize)
		{
			var query = new IndexQuery
			{
				Query = context.Request.QueryString["query"] ?? "",
				Start = context.GetStart(),
				Cutoff = context.GetCutOff(),
				CutoffEtag = context.GetCutOffEtag(),
				PageSize = context.GetPageSize(maxPageSize),
				SkipTransformResults = context.GetSkipTransformResults(),
				FieldsToFetch = context.Request.QueryString.GetValues("fetch"),
				DefaultField = context.Request.QueryString["defaultField"],
                WaitForNonStaleResultsAsOfNow = context.GetWaitForNonStaleResultsAsOfNow(),

				DefaultOperator =
					string.Equals(context.Request.QueryString["operator"], "AND", StringComparison.OrdinalIgnoreCase) ?
						QueryOperator.And :
						QueryOperator.Or,

				IsDistinct = context.IsDistinct(),
				SortedFields = context.Request.QueryString.GetValues("sort")
					.EmptyIfNull()
					.Select(x => new SortedField(x))
					.ToArray(),
				HighlightedFields = context.GetHighlightedFields().ToArray(),
				HighlighterPreTags = context.Request.QueryString.GetValues("preTags"),
				HighlighterPostTags = context.Request.QueryString.GetValues("postTags"),
                ResultsTransformer = context.Request.QueryString["resultsTransformer"],
                QueryInputs = context.ExtractQueryInputs(),
				ExplainScores = context.GetExplainScores()
            };

	
			var spatialFieldName = context.Request.QueryString["spatialField"] ?? Constants.DefaultSpatialFieldName;
			var queryShape = context.Request.QueryString["queryShape"];
			SpatialUnits units;
			bool unitsSpecified = Enum.TryParse(context.Request.QueryString["spatialUnits"], out units);
			double distanceErrorPct;
			if (!double.TryParse(context.Request.QueryString["distErrPrc"], NumberStyles.Any, CultureInfo.InvariantCulture, out distanceErrorPct))
				distanceErrorPct = Constants.DefaultSpatialDistanceErrorPct;
			SpatialRelation spatialRelation;
			if (Enum.TryParse(context.Request.QueryString["spatialRelation"], false, out spatialRelation)
				&& !string.IsNullOrWhiteSpace(queryShape))
			{
				return new SpatialIndexQuery(query)
				{
					SpatialFieldName = spatialFieldName,
					QueryShape = queryShape,
					RadiusUnitOverride = unitsSpecified ? units : (SpatialUnits?) null,
					SpatialRelation = spatialRelation,
					DistanceErrorPercentage = distanceErrorPct,
				};
			}
			return query;
		}

	}
}
