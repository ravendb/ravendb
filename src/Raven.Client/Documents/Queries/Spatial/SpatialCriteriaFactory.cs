using Raven.Client.Documents.Indexes.Spatial;

namespace Raven.Client.Documents.Queries.Spatial
{
    public class SpatialCriteriaFactory
    {
        public static SpatialCriteriaFactory Instance = new SpatialCriteriaFactory();

        private SpatialCriteriaFactory()
        {
        }

        public SpatialCriteria RelatesToShape(string shapeWKT, SpatialRelation relation, double distErrorPercent = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct)
        {
            return new WktCriteria(shapeWKT, relation, distErrorPercent);
        }

        public SpatialCriteria Intersects(string shapeWKT, double distErrorPercent = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct)
        {
            return RelatesToShape(shapeWKT, SpatialRelation.Intersects, distErrorPercent);
        }

        public SpatialCriteria Contains(string shapeWKT, double distErrorPercent = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct)
        {
            return RelatesToShape(shapeWKT, SpatialRelation.Contains, distErrorPercent);
        }

        public SpatialCriteria Disjoint(string shapeWKT, double distErrorPercent = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct)
        {
            return RelatesToShape(shapeWKT, SpatialRelation.Disjoint, distErrorPercent);
        }

        public SpatialCriteria Within(string shapeWKT, double distErrorPercent = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct)
        {
            return RelatesToShape(shapeWKT, SpatialRelation.Within, distErrorPercent);
        }

        public SpatialCriteria WithinRadius(double radius, double latitude, double longitude, SpatialUnits? radiusUnits = null, double distErrorPercent = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct)
        {
            return new CircleCriteria(radius, latitude, longitude, radiusUnits, SpatialRelation.Within, distErrorPercent);
        }
    }
}
