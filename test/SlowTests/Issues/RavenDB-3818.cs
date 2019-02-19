using System.Globalization;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3818 : RavenTestBase
    {
        [Fact]
        public void SparialSearchWithDistanceErrorPercent()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    //Point(12.556675672531128 55.675285554217), corner of the bounding rectangle below
                    var nearbyPoints1 = session.Query<EntitySpatialIndex.Result, EntitySpatialIndex>()
                        .Spatial(x => x.Coordinates, x => x.WithinRadius(1, 55.675285554217, 12.556675672531128, SpatialUnits.Kilometers, 0.025));
                    var queryUrl1 = RavenTestHelper.GetIndexQuery(nearbyPoints1);
                    Assert.NotNull(queryUrl1.Query.Contains("within(Coordinates, circle(1, 55.675285554217, 12.556675672531128))"));

                    var nearbyPoints2 = session.Query<EntitySpatialIndex.Result, EntitySpatialIndex>()
                        .Spatial(x => x.Coordinates, x => x.WithinRadius(1, 55.675285554217, 12.556675672531128, SpatialUnits.Kilometers, 0.01));
                    var queryUrl2 = RavenTestHelper.GetIndexQuery(nearbyPoints2);
                    Assert.NotNull(queryUrl2.Query.Contains("within(Coordinates, circle(1, 55.675285554217, 12.556675672531128), 0.01)"));
                }
            }
        }

        private class Entity
        {
            public string Id { get; set; }
            public Geolocation Geolocation { get; set; }
        }

        private class Geolocation
        {
            public double Lon { get; set; }
            public double Lat { get; set; }
            public string WKT
            {
                get
                {
                    return string.Format("POINT({0} {1})",
                        Lon.ToString(CultureInfo.InvariantCulture),
                        Lat.ToString(CultureInfo.InvariantCulture));
                }
            }
        }

        private class EntitySpatialIndex : AbstractIndexCreationTask<Entity>
        {
            public class Result
            {
                public string Coordinates { get; set; }
            }

            public EntitySpatialIndex()
            {
                Map = entities => entities.Select(entity => new
                {
                    entity.Id,
                    Coordinates = entity.Geolocation.WKT
                });

                Spatial("Coordinates", x => x.Cartesian.BoundingBoxIndex());
            }
        }
    }
}
