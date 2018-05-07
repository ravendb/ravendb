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
            return new WktCriteria(shapeWkt, relation, null, distErrorPercent);
        }

        public SpatialCriteria RelatesToShape(string shapeWkt, SpatialRelation relation, SpatialUnits units, double distErrorPercent = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct)
        {
            return new WktCriteria(shapeWkt, relation, units, distErrorPercent);
        }

        public SpatialCriteria Intersects(string shapeWkt, double distErrorPercent = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct)
        {
            return RelatesToShape(shapeWkt, SpatialRelation.Intersects, distErrorPercent);
        }

        public SpatialCriteria Intersects(string shapeWkt, SpatialUnits units, double distErrorPercent = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct)
        {
            return RelatesToShape(shapeWkt, SpatialRelation.Intersects, units, distErrorPercent);
        }

        public SpatialCriteria Contains(string shapeWkt, double distErrorPercent = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct)
        {
            return RelatesToShape(shapeWkt, SpatialRelation.Contains, distErrorPercent);
        }

        public SpatialCriteria Contains(string shapeWkt, SpatialUnits units, double distErrorPercent = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct)
        {
            return RelatesToShape(shapeWkt, SpatialRelation.Contains, units, distErrorPercent);
        }

        public SpatialCriteria Disjoint(string shapeWkt, double distErrorPercent = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct)
        {
            return RelatesToShape(shapeWkt, SpatialRelation.Disjoint, distErrorPercent);
        }

        public SpatialCriteria Disjoint(string shapeWkt, SpatialUnits units, double distErrorPercent = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct)
        {
            return RelatesToShape(shapeWkt, SpatialRelation.Disjoint, units, distErrorPercent);
        }

        public SpatialCriteria Within(string shapeWkt, double distErrorPercent = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct)
        {
            return RelatesToShape(shapeWkt, SpatialRelation.Within, distErrorPercent);
        }

        public SpatialCriteria Within(string shapeWkt, SpatialUnits units, double distErrorPercent = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct)
        {
            return RelatesToShape(shapeWkt, SpatialRelation.Within, units, distErrorPercent);
        }

        public SpatialCriteria WithinRadius(double radius, double latitude, double longitude, SpatialUnits? radiusUnits = null, double distErrorPercent = Constants.Documents.Indexing.Spatial.DefaultDistanceErrorPct)
        {
            return new CircleCriteria(radius, latitude, longitude, radiusUnits, SpatialRelation.Within, distErrorPercent);
        }
    }
}
