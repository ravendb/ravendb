using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_27940 : RavenTestBase
    {
        public RavenDB_27940(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Spatial | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void DistanceTiesAreOrderedByTheNextClause(Options options)
        {
            using var store = GetDocumentStore(options);
            new Places().Execute(store);

            using (var session = store.OpenSession())
            {
                // the same point, so the distances tie; stored out of name order
                foreach (var name in new[] { "d", "b", "e", "a", "c" })
                    session.Store(new Place { Name = name, Lat = 10, Lng = 10 });
                session.Store(new Place { Name = "z", Lat = 20, Lng = 20 });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                const string byDistance = "from index 'Places' order by spatial.distance(Location, spatial.point(10, 10))";

                Assert.Equal(["a", "b", "c", "d", "e", "z"], Names(session.Advanced.RawQuery<Place>(byDistance + ", Name").ToList()));
                Assert.Equal(["e", "d", "c", "b", "a", "z"], Names(session.Advanced.RawQuery<Place>(byDistance + ", Name desc").ToList()));
                Assert.Equal(["a", "b", "c"], Names(session.Advanced.RawQuery<Place>(byDistance + ", Name").Take(3).ToList()));
            }
        }

        private static string[] Names(List<Place> places) => places.Select(x => x.Name).ToArray();

        private class Place
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public double Lat { get; set; }
            public double Lng { get; set; }
        }

        private class Places : AbstractIndexCreationTask<Place>
        {
            public Places()
            {
                Map = places => from p in places
                                select new { p.Name, Location = CreateSpatialField(p.Lat, p.Lng) };
            }
        }
    }
}
