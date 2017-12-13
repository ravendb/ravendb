using Raven.Client.Documents.Indexes.Spatial;

namespace Raven.Client.Documents.Queries.Spatial
{
    public class SpatialCriteriaFactory
    {
        public static SpatialCriteriaFactory Instance = new SpatialCriteriaFactory();

        private SpatialCriteriaFactory()
        {
        }

        public SpatialCriteria RelatesToShape(string shapeWkt, SpatialRelation relation, double distErrorPercent = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct)
        {
            return new WktCriteria(shapeWkt, relation, distErrorPercent);
        }

        public SpatialCriteria Intersects(string shapeWkt, double distErrorPercent = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct)
        {
            return RelatesToShape(shapeWkt, SpatialRelation.Intersects, distErrorPercent);
        }

        public SpatialCriteria Contains(string shapeWkt, double distErrorPercent = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct)
        {
            return RelatesToShape(shapeWkt, SpatialRelation.Contains, distErrorPercent);
        }

        public SpatialCriteria Disjoint(string shapeWkt, double distErrorPercent = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct)
        {
            return RelatesToShape(shapeWkt, SpatialRelation.Disjoint, distErrorPercent);
        }

        public SpatialCriteria Within(string shapeWkt, double distErrorPercent = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct)
        {
            return RelatesToShape(shapeWkt, SpatialRelation.Within, distErrorPercent);
        }

        public SpatialCriteria WithinRadius(double radius, double latitude, double longitude, SpatialUnits? radiusUnits = null, double distErrorPercent = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct)
        {
            return new CircleCriteria(radius, latitude, longitude, radiusUnits, SpatialRelation.Within, distErrorPercent);
        }
    }
}
