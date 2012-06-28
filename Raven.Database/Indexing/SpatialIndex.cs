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
		internal static readonly SpatialContext Context = new SpatialContext(DistanceUnits.MILES);
		internal static readonly SpatialStrategy<SimpleSpatialFieldInfo> Strategy;
		private static readonly SimpleSpatialFieldInfo fieldInfo;
		private static readonly int maxLength;

		static SpatialIndex()
		{
			maxLength = GeohashPrefixTree.GetMaxLevelsPossible();
			fieldInfo = new SimpleSpatialFieldInfo(Constants.SpatialFieldName);
			Strategy = new RecursivePrefixTreeStrategy(new GeohashPrefixTree(Context, maxLength));
		}

		public static IEnumerable<Fieldable> Generate(double? lat, double? lng)
		{
			Shape shape = Context.MakePoint(lng ?? 0, lat ?? 0);
			return Strategy.CreateFields(fieldInfo, shape, true, false).Where(f => f != null)
				.Concat(new[] { new Field(Constants.SpatialShapeFieldName, Context.ToString(shape), Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS), });
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
			return Strategy.MakeQuery(new SpatialArgs(SpatialOperation.IsWithin, Context.MakeCircle(lng, lat, radius)), fieldInfo);
		}

		public static Filter MakeFilter(IndexQuery indexQuery)
		{
			var spatialQry = indexQuery as SpatialIndexQuery;
			if (spatialQry == null) return null;

			var args = new SpatialArgs(SpatialOperation.IsWithin, Context.MakeCircle(spatialQry.Longitude, spatialQry.Latitude, spatialQry.Radius));
			return Strategy.MakeFilter(args, fieldInfo);
		}

		public static double GetDistance(double fromLat, double fromLng, double toLat, double toLng)
		{
			var ptFrom = Context.MakePoint(fromLng, fromLat);
			var ptTo = Context.MakePoint(toLng, toLat);
			return Context.GetDistCalc().Distance(ptFrom, ptTo);
		}
	}
}
