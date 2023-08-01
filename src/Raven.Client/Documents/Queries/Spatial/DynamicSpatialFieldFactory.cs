using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Queries.Spatial
{
    public sealed class DynamicSpatialFieldFactory<TEntity>
    {
        private readonly DocumentConventions _conventions;
        public static readonly DynamicSpatialFieldFactory<TEntity> Instance = new(DocumentConventions.Default);

        internal DynamicSpatialFieldFactory(DocumentConventions conventions)
        {
            _conventions = conventions;
        }

        public PointField Point(Expression<Func<TEntity, object>> latitudePath, Expression<Func<TEntity, object>> longitudePath)
        {
            var latitude = latitudePath.ToPropertyPath(_conventions);
            var longitude = longitudePath.ToPropertyPath(_conventions);

            return new PointField(latitude, longitude);
        }

        public WktField Wkt(Expression<Func<TEntity, object>> wktPath)
        {
            var wkt = wktPath.ToPropertyPath(_conventions);

            return new WktField(wkt);
        }
    }
}
