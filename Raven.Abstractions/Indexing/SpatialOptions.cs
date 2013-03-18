namespace Raven.Abstractions.Indexing
{
	public class SpatialOptions
	{
		public SpatialOptions()
		{
			Type = SpatialFieldType.Geography;
			Strategy = SpatialSearchStrategy.GeohashPrefixTree;
			MaxTreeLevel = DefaultGeohashLevel;
		}

		public SpatialFieldType Type { get; set; }
		public SpatialSearchStrategy Strategy { get; set; }
		public int MaxTreeLevel { get; set; }
		public double MinX { get; set; }
		public double MaxX { get; set; }
		public double MinY { get; set; }
		public double MaxY { get; set; }

		// about 2 meters, should be good enough (see: http://unterbahn.com/2009/11/metric-dimensions-of-geohash-partitions-at-the-equator/)
		public const int DefaultGeohashLevel = 9;
		// about 1 meter, should be good enough
		public const int DefaultQuadTreeLevel = 25;
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

    public enum SpatialUnits
    {
        Kilometers,
        Miles
    }
}
