using System.Globalization;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Spatial
{
    public class Nick : RavenTestBase
    {
        public Nick(ITestOutputHelper output) : base(output)
        {
        }

        private class MySpatialDocument
        {
            public string Id { get; set; }
            public double Latitude { get; set; }
            public double Longitude { get; set; }
        }

        private class MySpatialIndex : AbstractIndexCreationTask<MySpatialDocument>
        {
            public MySpatialIndex()
            {
                Map = entities => from entity in entities
                                  select new
                                  {
                                      Coordinates = CreateSpatialField(entity.Latitude, entity.Longitude)
                                  };
            }
        }

        [Fact]
        public void Test()
        {
            using (IDocumentStore store = GetDocumentStore())
            {
                using (IDocumentSession session = store.OpenSession())
                {
                    session.Store(new MySpatialDocument
                    {
                        Id = "spatials/1",
                        Latitude = 48.3708044,
                        Longitude = 2.8028712999999925
                    });
                    session.SaveChanges();
                }

                new MySpatialIndex().Execute(store);

                Indexes.WaitForIndexing(store);

                // Distance between the 2 tested points is 35.75km
                // (can be checked here: http://www.movable-type.co.uk/scripts/latlong.html

                using (IDocumentSession session = store.OpenSession())
                {
                    var result = session.Advanced.DocumentQuery<MySpatialDocument, MySpatialIndex>()
                        // 1.025 is for the 2.5% uncertainty at the circle circumference
                        .WithinRadiusOf("Coordinates", radius: 35.75 * 1.025, latitude: 48.6003516, longitude: 2.4632387000000335)
                        .SingleOrDefault();

                    Assert.NotNull(result); // A location should be returned.

                    result = session.Advanced.DocumentQuery<MySpatialDocument, MySpatialIndex>()
                        .WithinRadiusOf("Coordinates", radius: 30, latitude: 48.6003516, longitude: 2.4632387000000335)
                        .SingleOrDefault();

                    Assert.Null(result); // No result should be returned.

                    result = session.Advanced.DocumentQuery<MySpatialDocument, MySpatialIndex>()
                        .WithinRadiusOf("Coordinates", radius: 33, latitude: 48.6003516, longitude: 2.4632387000000335)
                        .SingleOrDefault();

                    Assert.Null(result); // No result should be returned.

                    var shape = GetQueryShapeFromLatLon(48.6003516, 2.4632387000000335, 33);
                    result = session.Advanced.DocumentQuery<MySpatialDocument, MySpatialIndex>()
                        .RelatesToShape("Coordinates", shape, SpatialRelation.Intersects, 0)
                        .SingleOrDefault();

                    Assert.Null(result); // No result should be returned.
                }
            }
        }

        private static string GetQueryShapeFromLatLon(double lat, double lng, double radius)
        {
            return "Circle(" +
                   lng.ToString("F6", CultureInfo.InvariantCulture) + " " +
                   lat.ToString("F6", CultureInfo.InvariantCulture) + " " +
                   "d=" + radius.ToString("F6", CultureInfo.InvariantCulture) +
                   ")";
        }
    }
}
