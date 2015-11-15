using System;
using System.Globalization;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;

namespace Raven.Client.Spatial
{
    public class SpatialCriteriaFactory
    {
        public SpatialCriteria RelatesToShape(object shape, SpatialRelation relation)
        {
            return new SpatialCriteria
                   {
                       Relation = relation,
                       Shape = shape
                   };
        }

        public SpatialCriteria Intersects(object shape)
        {
            return RelatesToShape(shape, SpatialRelation.Intersects);
        }

        public SpatialCriteria Contains(object shape)
        {
            return RelatesToShape(shape, SpatialRelation.Contains);
        }

        public SpatialCriteria Disjoint(object shape)
        {
            return RelatesToShape(shape, SpatialRelation.Disjoint);
        }

        public SpatialCriteria Within(object shape)
        {
            return RelatesToShape(shape, SpatialRelation.Within);
        }

        [Obsolete("Order of parameters in this method is inconsistent with the rest of the API (x = longitude, y = latitude). Please use 'WithinRadius'.")]
        public SpatialCriteria WithinRadiusOf(double radius, double x, double y)
        {
            return RelatesToShape(SpatialIndexQuery.GetQueryShapeFromLatLon(y, x, radius), SpatialRelation.Within);
        }

        public SpatialCriteria WithinRadius(double radius, double latitude, double longitude)
        {
            return RelatesToShape(SpatialIndexQuery.GetQueryShapeFromLatLon(latitude, longitude, radius), SpatialRelation.Within);
        }
    }
}
