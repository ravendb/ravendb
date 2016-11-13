using Raven.Abstractions.Indexing;

namespace Raven.NewClient.Client.Spatial
{
    public class SpatialCriteria
    {
        public SpatialRelation Relation { get; set; }
        public object Shape { get; set; }
        public double DistanceErrorPct { get; set; }
    }
}
