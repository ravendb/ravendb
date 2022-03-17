using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Nberardi : RavenTestBase
    {
        public Nberardi(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Spatial_Search_Should_Integrate_Distance_As_A_Boost_Factor()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new SpatialIndex());

                using (var session = store.OpenSession())
                {
                    session.Store(new SpatialEntity(45.70955, -73.569131) // 22.23 Kb
                    {
                        Id = "se/1",
                    });

                    session.Store(new SpatialEntity(45.50955, -73.569131) // 0 Km
                    {
                        Id = "se/2",
                    });

                    session.Store(new SpatialEntity(45.60955, -73.569131) // 11.11 KM 
                    {
                        Id = "se/3",
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.DocumentQuery<SpatialEntity>("SpatialIndex")
                        .WithinRadiusOf("Coordinates", 500, 45.50955, -73.569133)
                        .OrderByDistance("Coordinates", 45.50955, -73.569133)
                        .ToList();

                    Assert.Equal(results[0].Id, "se/2");
                    Assert.Equal(results[1].Id, "se/3");
                    Assert.Equal(results[2].Id, "se/1");
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.DocumentQuery<SpatialEntity>("SpatialIndex")
                        .WithinRadiusOf("Coordinates", 500, 45.50955, -73.569133)
                        .OrderByDistance("Coordinates", "POINT (-73.569133 45.50955)")
                        .ToList();

                    Assert.Equal(results[0].Id, "se/2");
                    Assert.Equal(results[1].Id, "se/3");
                    Assert.Equal(results[2].Id, "se/1");
                }
            }
        }

        private class SpatialIndex : AbstractIndexCreationTask<SpatialEntity>
        {
            public SpatialIndex()
            {
                Map =
                    entities =>
                    from e in entities
                    select new
                    {
                        Coordinates = CreateSpatialField(e.Latitude, e.Longitude)
                    };
            }
        }

        private class SpatialEntity
        {
            public SpatialEntity() { }

            public SpatialEntity(double latitude, double longitude)
            {
                Latitude = latitude;
                Longitude = longitude;
            }

            public string Id { get; set; }
            public double Latitude { get; set; }
            public double Longitude { get; set; }
        }
    }
}
