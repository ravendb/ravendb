using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Spatial
{
    public class SpatialTest2 : RavenTestBase
    {
        public SpatialTest2(ITestOutputHelper output) : base(output)
        {
        }

        public class Entity
        {
            public double Latitude { set; get; }
            public double Longitude { set; get; }
        }

        public class EntitiesByLocation : AbstractIndexCreationTask<Entity>
        {
            public EntitiesByLocation()
            {
                Map = entities => from entity in entities
                                  select new { Coordinates = CreateSpatialField(entity.Latitude, entity.Longitude) };
            }
        }

        [RavenTheory(RavenTestCategory.Spatial)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void WeirdSpatialResults(Options options)
        {
            using (IDocumentStore store = GetDocumentStore(options))
            {
                using (IDocumentSession session = store.OpenSession())
                {
                    Entity entity = new Entity()
                    {
                        Latitude = 45.829507799999988,
                        Longitude = -73.800524699999983
                    };
                    session.Store(entity);
                    session.SaveChanges();
                }

                new EntitiesByLocation().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Query<Entity, EntitiesByLocation>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    // Let's search within a 150km radius
                    var results = session.Advanced.DocumentQuery<Entity, EntitiesByLocation>()
                        .WithinRadiusOf("Coordinates", radius: 150000 * 0.000621, latitude: 45.831909, longitude: -73.810322)
                        // This is less than 1km from the entity
                        .OrderByDistance("Coordinates", latitude: 45.831909, longitude: -73.810322)
                        .ToList();

                    // This works
                    Assert.Equal(results.Count, 1);

                    // Let's search within a 15km radius
                    results = session.Advanced.DocumentQuery<Entity, EntitiesByLocation>()
                        .WithinRadiusOf("Coordinates", radius: 15000 * 0.000621, latitude: 45.831909, longitude: -73.810322)
                        .OrderByDistance("Coordinates", latitude: 45.831909, longitude: -73.810322)
                        .ToList();

                    // This fails
                    Assert.Equal(results.Count, 1);

                    // Let's search within a 1.5km radius
                    results = session.Advanced.DocumentQuery<Entity, EntitiesByLocation>()
                        .WithinRadiusOf("Coordinates", radius: 1500 * 0.000621, latitude: 45.831909, longitude: -73.810322)
                        .OrderByDistance("Coordinates", latitude: 45.831909, longitude: -73.810322)
                        .ToList();

                    // This fails
                    Assert.Equal(results.Count, 1);
                }
            }
        }
    }
}
