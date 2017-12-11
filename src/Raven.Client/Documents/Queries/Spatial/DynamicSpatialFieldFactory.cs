using System;
using System.Linq.Expressions;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Queries.Spatial
{
    public class DynamicSpatialFieldFactory<TEntity>
    {
        public static readonly DynamicSpatialFieldFactory<TEntity> Instance = new DynamicSpatialFieldFactory<TEntity>();

        private DynamicSpatialFieldFactory()
        {
        }

        public PointField Point(Expression<Func<TEntity, object>> latitudePath, Expression<Func<TEntity, object>> longitudePath)
        {
            var latitude = latitudePath.ToPropertyPath();
            var longitude = longitudePath.ToPropertyPath();

            return new PointField(latitude, longitude);
        }

        public WktField Wkt(Expression<Func<TEntity, object>> wktPath)
        {
            var wkt = wktPath.ToPropertyPath();

            return new WktField(wkt);
        }
    }
}
