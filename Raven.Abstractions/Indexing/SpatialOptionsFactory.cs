using System;

namespace Raven.Abstractions.Indexing
{
	public class SpatialOptionsFactory
	{
		public SpatialOptions Geography()
		{
			return Geography(0);
		}

		public SpatialOptions Geography(int maxTreeLevel)
		{
			return Geography(0, SpatialSearchStrategy.GeohashPrefixTree);
		}

		public SpatialOptions Geography(int maxTreeLevel, SpatialSearchStrategy strategy)
		{
			if (maxTreeLevel == 0)
			{
				switch (strategy)
				{
					case SpatialSearchStrategy.GeohashPrefixTree:
						maxTreeLevel = 9; // about 2 meters, should be good enough (see: http://unterbahn.com/2009/11/metric-dimensions-of-geohash-partitions-at-the-equator/)
						break;
					case SpatialSearchStrategy.QuadPrefixTree:
						maxTreeLevel = 25; // about 1 meter, should be good enough
						break;
					default:
						throw new ArgumentOutOfRangeException("strategy");
				}
			}
			return new SpatialOptions
				   {
					   Type = SpatialFieldType.Geography,
					   MaxTreeLevel = maxTreeLevel,
					   Strategy = strategy
				   };
		}

		public  SpatialOptions Cartesian(double minX, double maxX, double minY, double maxY, int maxTreeLevel)
		{
			return new SpatialOptions
				   {
					   Type = SpatialFieldType.Cartesian,
					   Strategy = SpatialSearchStrategy.QuadPrefixTree,
					   MaxTreeLevel = maxTreeLevel,
					   MinX = minX,
					   MaxX = maxX,
					   MinY = minY,
					   MaxY = maxY
				   };
		}
	}
}