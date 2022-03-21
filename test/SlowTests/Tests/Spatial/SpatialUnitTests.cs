using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Spatial
{
    public class SpatialUnitTests : RavenTestBase
    {
        public SpatialUnitTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Test()
        {
            var myHouse = new DummyGeoDoc(44.757767, -93.355322);
            // The gym is about 7.32 miles (11.79 kilometers) from my house.
            var gym = new DummyGeoDoc(44.682861, -93.25);

            using (var store = GetDocumentStore())
            {
                store.Initialize();
                store.ExecuteIndex(new KmGeoIndex());
                store.ExecuteIndex(new MilesGeoIndex());

                using (var session = store.OpenSession())
                {
                    session.Store(myHouse);
                    session.Store(gym);
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var km = session.Query<DummyGeoDoc, KmGeoIndex>()
                                         .Spatial(x => x.Location, x => x.WithinRadius(8, myHouse.Latitude, myHouse.Longitude))
                                         .Count();
                    Assert.Equal(1, km);

                    var miles = session.Query<DummyGeoDoc, MilesGeoIndex>()
                                         .Spatial(x => x.Location, x => x.WithinRadius(8, myHouse.Latitude, myHouse.Longitude))
                                         .Count();
                    Assert.Equal(2, miles);
                }

                using (var session = store.OpenSession())
                {
                    var km = session.Query<DummyGeoDoc, KmGeoIndex>()
                        .Spatial("Location", factory => factory.WithinRadius(8, myHouse.Latitude, myHouse.Longitude))
                        .Count();
                    Assert.Equal(1, km);

                    var miles = session.Query<DummyGeoDoc, MilesGeoIndex>()
                        .Spatial("Location", factory => factory.WithinRadius(8, myHouse.Latitude, myHouse.Longitude))
                        .Count();
                    Assert.Equal(2, miles);
                }

                using (var session = store.OpenSession())
                {
                    var miles = session.Query<DummyGeoDoc, MilesGeoIndex>()
                        .Spatial("Location", factory => factory.WithinRadius(8, myHouse.Latitude, myHouse.Longitude, SpatialUnits.Kilometers))
                        .Count();
                    Assert.Equal(1, miles);

                    var km = session.Query<DummyGeoDoc, KmGeoIndex>()
                        .Spatial("Location", factory => factory.WithinRadius(8, myHouse.Latitude, myHouse.Longitude, SpatialUnits.Miles))
                        .Count();
                    Assert.Equal(2, km);
                }
            }
        }

        private class KmGeoIndex : AbstractIndexCreationTask<DummyGeoDoc>
        {
            public KmGeoIndex()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  Location = CreateSpatialField(doc.Location[1], doc.Location[0])
                              };

                Spatial(x => x.Location, x => x.Geography.Default(SpatialUnits.Kilometers));
            }
        }

        private class MilesGeoIndex : AbstractIndexCreationTask<DummyGeoDoc>
        {
            public MilesGeoIndex()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  Location = CreateSpatialField(doc.Location[1], doc.Location[0])
                              };

                Spatial(x => x.Location, x => x.Geography.Default(SpatialUnits.Miles));
            }
        }

        private class DummyGeoDoc
        {
            public string Id { get; set; }
            public double[] Location { get; set; }
            public double Latitude { get; set; }
            public double Longitude { get; set; }

            public DummyGeoDoc(double lat, double lng)
            {
                Latitude = lat;
                Longitude = lng;
                Location = new[] { lng, lat };
            }
        }
    }
}
