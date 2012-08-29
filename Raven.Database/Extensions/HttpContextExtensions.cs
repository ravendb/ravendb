//-----------------------------------------------------------------------
// <copyright file="HttpContextExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Extensions
{
	public static class HttpContextExtensions
	{
		public static Facet[] GetFacetsFromHttpContext(this IHttpContext context)
		{
			var dictionary = new Dictionary<string, Facet>();

			foreach (var facetString in context.Request.QueryString.AllKeys
				.Where(x=>x.StartsWith("facet.", StringComparison.InvariantCultureIgnoreCase))
				.ToArray())
			{
				var parts = facetString.Split(new[] {'.'}, StringSplitOptions.RemoveEmptyEntries);
				if(parts.Length != 3)
					throw new InvalidOperationException("Could not parse query parameter: " + facetString);

				var fieldName = parts[1];

				Facet facet;
				if (dictionary.TryGetValue(fieldName, out facet) == false)
					dictionary[fieldName] = facet = new Facet{Name = fieldName};

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
				GroupBy = context.Request.QueryString.GetValues("groupBy"),
				DefaultField = context.Request.QueryString["defaultField"],

				DefaultOperator = 
					string.Equals(context.Request.QueryString["operator"], "AND", StringComparison.InvariantCultureIgnoreCase) ?
						QueryOperator.And : 
						QueryOperator.Or,

				AggregationOperation = context.GetAggregationOperation(),
				SortedFields = context.Request.QueryString.GetValues("sort")
					.EmptyIfNull()
					.Select(x => new SortedField(x))
					.ToArray()
			};

			var spatialFieldName = context.Request.QueryString["spatialField"] ?? Constants.DefaultSpatialFieldName;
			var queryShape = context.Request.QueryString["queryShape"];
			double distanceErrorPct;
			if (!double.TryParse(context.Request.QueryString["distErrPrc"], out distanceErrorPct))
				distanceErrorPct = Constants.DefaultSpatialDistanceErrorPct;
			SpatialRelation spatialRelation;
			if (Enum.TryParse(context.Request.QueryString["spatialRelation"], false, out spatialRelation)
				&& !string.IsNullOrWhiteSpace(queryShape))
			{
				return new SpatialIndexQuery(query)
				{
					SpatialFieldName = spatialFieldName,
					QueryShape = queryShape,
					SpatialRelation = spatialRelation,
					DistanceErrorPercentage = distanceErrorPct,
				};
			}
			return query;
		}
		
	}
}
