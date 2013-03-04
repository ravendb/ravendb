using Raven.Abstractions.Indexing;

namespace Raven.Client
{
	public class SpatialCriteria
	{
		public SpatialRelation Relation { get; set; }
		public string Shape { get; set; }
	}
}