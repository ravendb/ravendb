//-----------------------------------------------------------------------
// <copyright file="SpatialIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Documents;
using Lucene.Net.Search;
using Lucene.Net.Spatial;
using Lucene.Net.Spatial.Prefix;
using Lucene.Net.Spatial.Prefix.Tree;
using Lucene.Net.Spatial.Queries;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Spatial4n.Core.Context;
using Spatial4n.Core.Shapes;


namespace Raven.Database.Indexing
{
	public static class SpatialIndex
	{
		// TODO: Support new SpatialContext(DistanceUnits.MILES) for backward compatibility through config
		internal static readonly SpatialContext Context = SpatialContext.GEO_KM;

		static SpatialIndex()
		{
		}

		public static IEnumerable<IFieldable> Generate(double? lat, double? lng)
		{
			var maxLength = GeohashPrefixTree.GetMaxLevelsPossible();
			var strategy = new RecursivePrefixTreeStrategy(new GeohashPrefixTree(Context, maxLength), Constants.SpatialFieldName);

			Shape shape = Context.MakePoint(lng ?? 0, lat ?? 0);
			return strategy.CreateIndexableFields(shape)
				.Concat(new[] { new Field(Constants.SpatialShapeFieldName, Context.ToString(shape), Field.Store.YES, Field.Index.NO), });
		}

		public static IEnumerable<IFieldable> Generate(string shapeWKT, SpatialSearchStrategy spatialSearchStrategy, int maxTreeLevel, double distanceErrorPct = 0.025)
		{
			var strategy = CreateStrategy(spatialSearchStrategy, maxTreeLevel);

			var shape = Context.ReadShape(shapeWKT);
			return strategy.CreateIndexableFields(shape)
				.Concat(new[] {new Field(Constants.SpatialShapeFieldName, Context.ToString(shape), Field.Store.YES, Field.Index.NO),});
		}

		public static SpatialStrategy CreateStrategy(SpatialSearchStrategy spatialSearchStrategy, int maxTreeLevel)
		{
			switch (spatialSearchStrategy)
			{
				case SpatialSearchStrategy.GeohashPrefixTree:
					return new RecursivePrefixTreeStrategy(new GeohashPrefixTree(Context, maxTreeLevel), Constants.SpatialFieldName);
				case SpatialSearchStrategy.QuadPrefixTree:
					return new RecursivePrefixTreeStrategy(new QuadPrefixTree(Context, maxTreeLevel), Constants.SpatialFieldName);
			}
			return null;
		}

		/// <summary>
		/// Make a spatial query
		/// </summary>
		/// <param name="spatialStrategy"> </param>
		/// <param name="lat"></param>
		/// <param name="lng"></param>
		/// <param name="radius">Radius, in miles</param>
		/// <returns></returns>
		public static Query MakeQuery(SpatialStrategy spatialStrategy, double lat, double lng, double radius)
		{
			return spatialStrategy.MakeQuery(new SpatialArgs(SpatialOperation.IsWithin, Context.MakeCircle(lng, lat, radius)));
		}

		public static Query MakeQuery(SpatialStrategy spatialStrategy, string shapeWKT, SpatialRelation relation)
		{
			return spatialStrategy.MakeQuery(new SpatialArgs(SpatialOperation.IsWithin, Context.ReadShape(shapeWKT)));
		}

		public static Filter MakeFilter(SpatialStrategy spatialStrategy, IndexQuery indexQuery)
		{
			var spatialQry = indexQuery as SpatialIndexQuery;
			if (spatialQry == null) return null;

			var args = new SpatialArgs(SpatialOperation.IsWithin, Context.MakeCircle(spatialQry.Longitude, spatialQry.Latitude, spatialQry.Radius));
			return spatialStrategy.MakeFilter(args);
		}

		public static double GetDistance(double fromLat, double fromLng, double toLat, double toLng)
		{
			var ptFrom = Context.MakePoint(fromLng, fromLat);
			var ptTo = Context.MakePoint(toLng, toLat);
			return Context.GetDistCalc().Distance(ptFrom, ptTo);
		}
	}
}
