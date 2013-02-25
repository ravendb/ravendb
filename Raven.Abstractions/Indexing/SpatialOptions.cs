namespace Raven.Abstractions.Indexing
{
	public class SpatialOptions
	{
		public SpatialOptions()
		{
			Type = SpatialFieldType.Geography;
			Strategy = SpatialSearchStrategy.GeohashPrefixTree;
			MaxTreeLevel = 9;
		}

		public SpatialFieldType Type { get; set; }
		public SpatialSearchStrategy Strategy { get; set; }
		public int MaxTreeLevel { get; set; }
		public double MinX { get; set; }
		public double MaxX { get; set; }
		public double MinY { get; set; }
		public double MaxY { get; set; }
	}

	public enum SpatialFieldType
	{
		Geography,
		Cartesian
	}

	public enum SpatialSearchStrategy
	{
		GeohashPrefixTree,
		QuadPrefixTree,
	}

	public enum SpatialRelation
	{
		Within,
		Contains,
		Disjoint,
		Intersects,

		/// <summary>
		/// Does not filter the query, merely sort by the distance
		/// </summary>
		Nearby
	}
}
