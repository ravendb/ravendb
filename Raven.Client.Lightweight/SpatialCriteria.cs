using Raven.Abstractions.Indexing;

namespace Raven.Client
{
	public class SpatialCriteria
	{
		public SpatialRelation Relation { get; set; }
		public object Shape { get; set; }
	}
}