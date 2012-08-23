namespace Raven.Abstractions.Indexing
{
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
	}
}
