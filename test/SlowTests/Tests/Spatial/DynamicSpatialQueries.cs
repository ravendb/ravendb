using FastTests;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Queries;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Spatial
{
    public class DynamicSpatialQueries : RavenTestBase
    {
        public DynamicSpatialQueries(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void VerifySingleSpatialPropertyInResultsTest()
        {
            var house1 = new GeoDoc(44.75, -93.35);
            var house2 = new GeoDoc(44.751, -93.351);
            var house3 = new GeoDoc(44.752, -93.352);
            var house4 = new GeoDoc(45.75, -94.35);

            using (var store = GetDocumentStore())
            {
                store.Initialize();

                using (var session = store.OpenSession())
                {
                    session.Store(house1);
                    session.Store(house2);
                    session.Store(house3);
                    session.Store(house4);
                    session.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    var json = commands.RawGetJson<BlittableJsonReaderObject>("/queries?query=from GeoDocs where spatial.within(spatial.point(Location1.Latitude, Location1.Longitude), spatial.circle(50, 44.75, -93.35))&addSpatialProperties=true");

                    Assert.True(json.TryGet(nameof(QueryResult.TotalResults), out int results));
                    Assert.Equal(3, results);
                    
                    Assert.True(json.TryGet(nameof(DocumentQueryResult.SpatialProperties), out BlittableJsonReaderArray spatialProperties));
                    Assert.Equal(1, spatialProperties.Length);
                    
                    (spatialProperties[0] as BlittableJsonReaderObject).TryGet(nameof(SpatialProperty.LatitudeProperty), out string lat);
                    Assert.Equal(lat, "Location1.Latitude");
                    (spatialProperties[0] as BlittableJsonReaderObject).TryGet(nameof(SpatialProperty.LongitudeProperty), out string lng);
                    Assert.Equal(lng, "Location1.Longitude");
                }
            }
        }

        [Fact]
        public void VerifyMultipleSpatialPropertiesInResultsTest()
        {
            var house1 = new GeoDoc(44.75, -93.35);
            var house2 = new GeoDoc(44.751, -93.351);
            var house3 = new GeoDoc(44.752, -93.352);
            var house4 = new GeoDoc(45.75, -94.35);

            using (var store = GetDocumentStore())
            {
                store.Initialize();

                using (var session = store.OpenSession())
                {
                    session.Store(house1);
                    session.Store(house2);
                    session.Store(house3);
                    session.Store(house4);
                    session.SaveChanges();
                }

                var boundingPolygon =
                    "POLYGON((12.556675672531128 55.675285554217,12.56213665008545 55.675285554217,12.56213665008545 55.67261750095371,12.556675672531128 55.67261750095371,12.556675672531128 55.675285554217))";
                
                using (var commands = store.Commands())
                {
                    var json = commands.RawGetJson<BlittableJsonReaderObject>("/queries?query=from GeoDocs " +
                        "where spatial.within(spatial.point(Location1.Latitude, Location1.Longitude), spatial.circle(50, 44.75, -93.35)) " +
                        "or spatial.within(spatial.point(Location2.Latitude, Location2.Longitude), spatial.circle(50, 45.75, -94.35)) " +
                        $"or spatial.within(spatial.point(Location3.Latitude, Location3.Longitude), spatial.wkt('{boundingPolygon}'))&addSpatialProperties=true");

                    Assert.True(json.TryGet(nameof(QueryResult.TotalResults), out int results));
                    Assert.Equal(4, results);
                    
                    Assert.True(json.TryGet(nameof(DocumentQueryResult.SpatialProperties), out BlittableJsonReaderArray spatialProperties));
                    Assert.Equal(3, spatialProperties.Length);

                    for (var i = 0; i < spatialProperties.Length; i++)
                    {
                        Assert.True((spatialProperties[i] as BlittableJsonReaderObject).TryGet(nameof(SpatialProperty.LatitudeProperty), out string lat));
                        Assert.Equal(lat, $"Location{i+1}.Latitude");
                        Assert.True((spatialProperties[i] as BlittableJsonReaderObject).TryGet(nameof(SpatialProperty.LongitudeProperty), out string lng));
                        Assert.Equal(lng, $"Location{i+1}.Longitude");
                    } 
                }
            }
        }
        
        [Fact]
        public void VerifySelectedSpatialPropertiesInResultsTest()
        {
            var house1 = new GeoDoc(44.75, -93.35);
            var house2 = new GeoDoc(44.751, -93.351);
            var house3 = new GeoDoc(44.752, -93.352);
            var house4 = new GeoDoc(45.75, -94.35);

            using (var store = GetDocumentStore())
            {
                store.Initialize();

                using (var session = store.OpenSession())
                {
                    session.Store(house1);
                    session.Store(house2);
                    session.Store(house3);
                    session.Store(house4);
                    session.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    var json = commands.RawGetJson<BlittableJsonReaderObject>("/queries?query=from GeoDocs " +
                                                                              "where spatial.within(spatial.point(Location1.Latitude, Location1.Longitude), spatial.circle(50, 44.75, -93.35)) " +
                                                                              "select Location1 as SomeAlias&addSpatialProperties=true");

                    Assert.True(json.TryGet(nameof(QueryResult.TotalResults), out int results));
                    Assert.Equal(3, results);
                    
                    Assert.True(json.TryGet(nameof(DocumentQueryResult.SpatialProperties), out BlittableJsonReaderArray spatialProperties));
                    Assert.Equal(1, spatialProperties.Length);

                    (spatialProperties[0] as BlittableJsonReaderObject).TryGet(nameof(SpatialProperty.LatitudeProperty), out string lat);
                    Assert.Equal(lat, "SomeAlias.Latitude");
                    (spatialProperties[0] as BlittableJsonReaderObject).TryGet(nameof(SpatialProperty.LongitudeProperty), out string lng);
                    Assert.Equal(lng, "SomeAlias.Longitude");
                }
            }
        }

        private class GeoPoint
        {
            public double Latitude { get; set; }
            public double Longitude { get; set; }
            
            public GeoPoint(double lat, double lng)
            {
                Latitude = lat;
                Longitude = lng;
            }
        }

        private class GeoDoc
        {
            public string Id { get; set; }
            public GeoPoint Location1 { get; set; }
            public GeoPoint Location2 { get; set; }
            public GeoPoint Location3 { get; set; }

            public GeoDoc(double lat, double lng)
            {
                Location1 = new GeoPoint(lat, lng);
                Location2 = new GeoPoint(lat + 0.0001, lng + 0.0001);
                Location3 = new GeoPoint(lat + 0.0002, lng + 0.0002);
            }
        }
    }
}
