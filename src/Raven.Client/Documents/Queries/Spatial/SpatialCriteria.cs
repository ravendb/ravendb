using Raven.Client.Documents.Indexes.Spatial;

namespace Raven.Client.Documents.Queries.Spatial
{
    public class SpatialCriteria
    {
        public SpatialRelation Relation { get; set; }
        public object Shape { get; set; }
        public double DistanceErrorPct { get; set; }
    }
}
