using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Maxime2 : RavenTestBase
    {
        public Maxime2(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.Single)]
        public void Spatial_Search_Should_Integrate_Distance_As_A_Boost_Factor(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.ExecuteIndex(new SpatialIndex());

                using (var session = store.OpenSession())
                {
                    session.Store(new SpatialEntity(45.70955, -73.569131) // 22.23 Kb
                    {
                        Id = "se/1",
                        Name = "Universite du Quebec a Montreal",
                        Description = "UQAM",
                    });

                    session.Store(new SpatialEntity(45.50955, -73.569131) // 0 Km
                    {
                        Id = "se/2",
                        Name = "UQAM",
                        Description = "Universite du Quebec a Montreal",
                    });

                    session.Store(new SpatialEntity(45.60955, -73.569131) // 11.11 KM 
                    {
                        Id = "se/3",
                        Name = "UQAM",
                        Description = "Universite du Quebec a Montreal",
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.DocumentQuery<SpatialEntity>("SpatialIndex")
                        .Search("Name", "UQAM")
                        .Search("Description", "UQAM")
                        .WithinRadiusOf("Coordinates", 500, 45.50955, -73.569133)
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
                        Name = e.Name.Boost(3),
                        e.Description,
                        Coordinates = CreateSpatialField(e.Latitude, e.Longitude)
                    };

                Index(e => e.Name, FieldIndexing.Search);
                Index(e => e.Description, FieldIndexing.Search);
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
            public string Name { get; set; }
            public string Description { get; set; }
            public double Latitude { get; set; }
            public double Longitude { get; set; }
        }
    }
}
