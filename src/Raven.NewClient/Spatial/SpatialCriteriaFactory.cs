using System;
using System.Globalization;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;

namespace Raven.NewClient.Client.Spatial
{
    public class SpatialCriteriaFactory
    {
        public SpatialCriteria RelatesToShape(object shape, SpatialRelation relation, double distErrorPercent = 0.025)
        {
            return new SpatialCriteria
                   {
                       Relation = relation,
                       Shape = shape,
                       DistanceErrorPct = distErrorPercent
                   };
        }

        public SpatialCriteria Intersects(object shape, double distErrorPercent = 0.025)
        {
            return RelatesToShape(shape, SpatialRelation.Intersects, distErrorPercent);
        }

        public SpatialCriteria Contains(object shape, double distErrorPercent = 0.025)
        {
            return RelatesToShape(shape, SpatialRelation.Contains, distErrorPercent);
        }

        public SpatialCriteria Disjoint(object shape, double distErrorPercent = 0.025)
        {
            return RelatesToShape(shape, SpatialRelation.Disjoint, distErrorPercent);
        }

        public SpatialCriteria Within(object shape, double distErrorPercent = 0.025)
        {
            return RelatesToShape(shape, SpatialRelation.Within, distErrorPercent);
        }

        [Obsolete("Order of parameters in this method is inconsistent with the rest of the API (x = longitude, y = latitude). Please use 'WithinRadius'.")]
        public SpatialCriteria WithinRadiusOf(double radius, double x, double y, double distErrorPercent = 0.025)
        {
            return RelatesToShape(SpatialIndexQuery.GetQueryShapeFromLatLon(y, x, radius), SpatialRelation.Within, distErrorPercent);
        }

        public SpatialCriteria WithinRadius(double radius, double latitude, double longitude, double distErrorPercent=0.025)
        {
            return RelatesToShape(SpatialIndexQuery.GetQueryShapeFromLatLon(latitude, longitude, radius), SpatialRelation.Within,distErrorPercent);
        }
    }
}
