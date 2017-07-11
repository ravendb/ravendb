using System.Globalization;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3818 : RavenTestBase
    {
        [Fact(Skip = "RavenDB-5988")]
        public void SparialSearchWithDistanceErrorPercent()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    //Point(12.556675672531128 55.675285554217), corner of the bounding rectangle below
                    var nearbyPoints1 = (RavenQueryInspector<Entity>)session.Query<Entity, EntitySpatialIndex>()
                        .Customize(x =>
                            x.WithinRadiusOf(fieldName: "Coordinates", radius: 1, latitude: 55.675285554217, longitude: 12.556675672531128, distErrorPercent: 0.025));

                    //var queryUrl1 = nearbyPoints1.GetIndexQuery(false).GetQueryString(DocumentConventions.Default);
                    //Assert.NotNull(queryUrl1.Contains("distErrorPercent=0.025"));

                    var nearbyPoints2 = (RavenQueryInspector<Entity>)session.Query<Entity, EntitySpatialIndex>()
                        .Customize(x =>
                            x.WithinRadiusOf(fieldName: "Coordinates", radius: 1, latitude: 55.675285554217, longitude: 12.556675672531128, distErrorPercent: 0.01));
                    //var queryUrl2 = nearbyPoints2.GetIndexQuery(false).GetQueryString(DocumentConventions.Default);
                    //Assert.NotNull(queryUrl2.Contains("distErrorPercent=0.01"));
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
