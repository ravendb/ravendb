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
			fieldInfo = new SimpleSpatialFieldInfo("RavenDBSpatial");
			strategy = new RecursivePrefixTreeStrategy(new GeohashPrefixTree(RavenSpatialContext, maxLength));
		}

		public static IEnumerable<Fieldable> Generate(double? lat, double? lng)
		{
			Shape shape = RavenSpatialContext.MakePoint(lng ?? 0, lat ?? 0);
			return strategy.CreateFields(fieldInfo, shape, true, false).Where(f => f != null);
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
	}
}
