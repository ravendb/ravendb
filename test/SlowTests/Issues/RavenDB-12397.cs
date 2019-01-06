using System.Linq;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12397 : RavenTestBase
    {
        public class Beer
        {
            public string Id { get; set; }

            public string Name { get; set; }
            public string Style { get; set; }            
            public string Brewery { get; set; }            
        }

        public class BeerStyle
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        public class Brewery
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public void Intersection_graph_queries_that_have_two_outgoing_edges_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);
                using (var session = store.OpenSession())
                {
                    var intersectionResults = session.Advanced.RawQuery<JObject>(
                        @"
                            match (Beers as beer)-[Style]->(BeerStyles as beerStyle) and
                                  (Beers as beer)-[Brewery]->(Breweries as brewery)
                        ").ToArray();

                    var unifiedIntersectionResults = session.Advanced.RawQuery<JObject>(
                        @"
                            match (Breweries as brewery)<-[Brewery]-(Beers as beer)-[Style]->(BeerStyles as beerStyle)
                        ").ToArray();

                    Assert.Equal(intersectionResults,unifiedIntersectionResults);
               }
            }
        }
        
        [Fact]
        public void Intersection_graph_queries_that_have_two_incoming_edges_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);
                using (var session = store.OpenSession())
                {
                    var intersectionResults = session.Advanced.RawQuery<JObject>(
                        @"
                            match 
                                  (Beers as beer)-[Style]->(BeerStyles as beerStyle) and
                                  (Beers as anotherBeer)-[Style]->(BeerStyles as beerStyle)
                            where beer != anotherBeer
                        ").ToArray();

                    var unifiedIntersectionResults = session.Advanced.RawQuery<JObject>(
                        @"
                            match (Beers as beer)-[Style]->(BeerStyles as beerStyle)<-[Style]-(Beers as anotherBeer)
                            where beer != anotherBeer
                        ").ToArray();

                    Assert.Equal(intersectionResults,unifiedIntersectionResults);
                }
            }
        }

        [Fact]
        public void Intersection_graph_queries_that_have_both_incoming_and_outgoing_edges_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);
                using (var session = store.OpenSession())
                {
                    var intersectionResults = session.Advanced.RawQuery<JObject>(
                        @"
                            match 
                                (Beers as anotherBeer)-[Brewery]->(Breweries as otherBrewery) and 
                                (Beers as anotherBeer)-[Style]->(BeerStyles as beerStyle) and 
                                (Beers as beer)-[Style]->(BeerStyles as beerStyle) and 
                                (Beers as beer)-[Brewery]->(Breweries as brewery)
                            where beer != anotherBeer
                        ").ToArray();

                    var unifiedIntersectionResults = session.Advanced.RawQuery<JObject>(
                        @"
                            match (Breweries as brewery)<-[Brewery]-(Beers as beer)-[Style]->(BeerStyles as beerStyle)<-[Style]-(Beers as anotherBeer)-[Brewery]->(Breweries as otherBrewery)
                            where beer != anotherBeer
                        ").ToArray();

                    Assert.Equal(intersectionResults,unifiedIntersectionResults);
                }
            }
        }

        private static void CreateData(DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Brewery
                    {Name = "FooBar Brewery"}, "breweries/1");
                session.Store(new Brewery
                    {Name = "BarFoo Brewery"}, "breweries/2");

                session.Store(new BeerStyle
                    {Name = "German-Style Marzen"}, "beerstyles/1");
                session.Store(new BeerStyle
                    {Name = "German-Style Pilsener"}, "beerstyles/2");
                session.Store(new BeerStyle
                    {Name = "Blonde Ale"}, "beerstyles/3");

                session.Store(new Beer
                {
                    Name = "Mega Beer",
                    Brewery = "breweries/1",
                    Style = "beerstyles/1"
                }, "beers/1");

                session.Store(new Beer
                {
                    Name = "Ultra Beer 2000",
                    Brewery = "breweries/2",
                    Style = "beerstyles/2"
                }, "beers/2");
                
                session.Store(new Beer
                {
                    Name = "Giga Beer 3000",
                    Brewery = "breweries/2",
                    Style = "beerstyles/2"
                }, "beers/3");

                session.SaveChanges();
            }
        }
    }
}
