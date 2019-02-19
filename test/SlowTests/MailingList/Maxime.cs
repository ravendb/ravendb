using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class Maxime : RavenTestBase
    {
        [Fact]
        public void WithingRadiusOf_Should_Not_Break_Relevance()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {
                new PlacesByTermsAndLocation().Execute(store);

                var place1 = new Place("Université du Québec à Montréal")
                {
                    Id = "places/1",
                    Description = "L'Université du Québec à Montréal (UQAM) est une université francophone, publique et urbaine de Montréal, dans la province du Québec au Canada.",
                    Latitude = 45.50955,
                    Longitude = -73.569131
                };

                var place2 = new Place("UQAM")
                {
                    Id = "places/2",
                    Description = "L'Université du Québec à Montréal (UQAM) est une université francophone, publique et urbaine de Montréal, dans la province du Québec au Canada.",
                    Latitude = 45.50955,
                    Longitude = -73.569131
                };

                session.Store(place1);
                session.Store(place2);

                session.SaveChanges();

                // places/2: perfect match + boost
                var terms = "UQAM";
                QueryStatistics stats;
                var places = session.Advanced.DocumentQuery<Place, PlacesByTermsAndLocation>()
                    .WaitForNonStaleResults()
                    .Statistics(out stats)
                    .WithinRadiusOf("Coordinates", 500, 45.54545, -73.63908)
                    .OpenSubclause()
                    .WhereLucene("Name", $"{terms}")
                    .WhereLucene("Terms", $"{terms}")
                    .CloseSubclause()
                    .Take(10)
                    .ToList();

                Assert.Equal("places/2", places[0].Id);
                // places/1: perfect match + boost
                terms = "Université Québec Montréal";
                places = session.Advanced.DocumentQuery<Place, PlacesByTermsAndLocation>()
                    .WaitForNonStaleResults()
                    .Statistics(out stats)
                    .WithinRadiusOf("Coordinates", 500, 45.54545, -73.63908)
                    .OpenSubclause()
                    .WhereLucene("Name", $"{terms}")
                    .WhereLucene("Terms", $"{terms}")
                    .CloseSubclause()
                    .Take(10)
                    .ToList();

                Assert.Equal("places/1", places[0].Id);
            }
        }


        [Fact]
        public void Can_just_set_to_sort_by_relevance_without_filtering()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {
                new PlacesByTermsAndLocation().Execute(store);

                var place1 = new Place("Université du Québec à Montréal")
                {
                    Id = "places/1",
                    Description = "L'Université du Québec à Montréal (UQAM) est une université francophone, publique et urbaine de Montréal, dans la province du Québec au Canada.",
                    Latitude = 45.50955,
                    Longitude = -73.569131
                };

                var place2 = new Place("UQAM")
                {
                    Id = "places/2",
                    Description = "L'Université du Québec à Montréal (UQAM) est une université francophone, publique et urbaine de Montréal, dans la province du Québec au Canada.",
                    Latitude = 45.50955,
                    Longitude = -73.569131
                };

                session.Store(place1);
                session.Store(place2);

                session.SaveChanges();

                // places/1: perfect match + boost
                const string terms = "Université Québec Montréal";
                QueryStatistics stats;
                var places = session.Advanced.DocumentQuery<Place, PlacesByTermsAndLocation>()
                    .WaitForNonStaleResults()
                    .Statistics(out stats)
                    .OrderByDistance("Coordinates", 45.54545, -73.63908)
                    .OpenSubclause()
                    .Search("Name", terms)
                    .Search("Terms", terms)
                    .CloseSubclause()
                    .Take(10)
                    .ToList();

                Assert.Equal("places/1", places[0].Id);
                Assert.Equal("places/2", places[1].Id);
            }
        }

        private class Place
        {
            public Place(string name)
            {
                Name = name;
            }

            public string Id { get; set; }
            public string Name { get; set; }
            public string Description { get; set; }
            public string Address { get; set; }
            public double Latitude { get; set; }
            public double Longitude { get; set; }
        }

        private class PlacesByTermsAndLocation : AbstractIndexCreationTask<Place, PlacesByTermsAndLocation.PlaceQuery>
        {
            public class PlaceQuery
            {
                public string Name { get; set; }
                public string Terms { get; set; }
            }

            public PlacesByTermsAndLocation()
            {
                Map = boards =>
                      from b in boards
                      select new
                      {
                          Name = b.Name.Boost(3),
                          Terms = new
                          {
                              b.Description,
                              b.Address
                          },
                          Coordinates = CreateSpatialField(b.Latitude, b.Longitude)
                      };

                Index(p => p.Name, FieldIndexing.Search);
                Index(p => p.Terms, FieldIndexing.Search);
            }
        }
    }
}
