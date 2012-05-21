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
using Raven.Abstractions.Data;
using Spatial4n.Core.Context;
using Spatial4n.Core.Distance;
using Spatial4n.Core.Query;
using Spatial4n.Core.Shapes;


namespace Raven.Database.Indexing
{
	public static class SpatialIndex
	{
		internal static readonly SpatialContext RavenSpatialContext = new SpatialContext(DistanceUnits.MILES);
		private static readonly SpatialStrategy<SimpleSpatialFieldInfo> strategy;
		private static readonly SimpleSpatialFieldInfo fieldInfo;
		private static readonly int maxLength;

		static SpatialIndex()
		{
			maxLength = GeohashPrefixTree.GetMaxLevelsPossible();
			fieldInfo = new SimpleSpatialFieldInfo(Constants.SpatialFieldName);
			strategy = new RecursivePrefixTreeStrategy(new GeohashPrefixTree(RavenSpatialContext, maxLength));
		}

		public static IEnumerable<Fieldable> Generate(double? lat, double? lng)
		{
			Shape shape = RavenSpatialContext.MakePoint(lng ?? 0, lat ?? 0);
			return strategy.CreateFields(fieldInfo, shape, true, false).Where(f => f != null)
				.Concat(new[] { new Field(Constants.SpatialShapeFieldName, RavenSpatialContext.ToString(shape), Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS), });
		}

		/// <summary>
		/// Make a spatial query
		/// </summary>
		/// <param name="lat"></param>
		/// <param name="lng"></param>
		/// <param name="radius">Radius, in miles</param>
		/// <returns></returns>
		public static Query MakeQuery(double lat, double lng, double radius)
		{
			return strategy.MakeQuery(new SpatialArgs(SpatialOperation.IsWithin, RavenSpatialContext.MakeCircle(lng, lat, radius)), fieldInfo);
		}

		public static Filter MakeFilter(IndexQuery indexQuery)
		{
			var spatialQry = indexQuery as SpatialIndexQuery;
			if (spatialQry == null) return null;

			var args = new SpatialArgs(SpatialOperation.IsWithin, RavenSpatialContext.MakeCircle(spatialQry.Longitude, spatialQry.Latitude, spatialQry.Radius));
			return strategy.MakeFilter(args, fieldInfo);
		}

		public static double GetDistance(double fromLat, double fromLng, double toLat, double toLng)
		{
			var ptFrom = RavenSpatialContext.MakePoint(fromLng, fromLat);
			var ptTo = RavenSpatialContext.MakePoint(toLng, toLat);
			return RavenSpatialContext.GetDistCalc().Distance(ptFrom, ptTo);
		}
	}
}
