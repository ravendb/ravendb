using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Tests.Infrastructure;
using Xunit;
using Order = FastTests.Server.Basic.Entities.Order;
using Product = FastTests.Server.Basic.Entities.Product;
using OrderLine = FastTests.Server.Basic.Entities.OrderLine;

namespace FastTests.Graph
{
    public class SimpleGraphQueries : RavenTestBase
    {
        private void CreateSimpleData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var entityA = new Entity{ Id = "entity/1", Name = "A" };
                var entityB = new Entity{ Id = "entity/2", Name = "B" };
                var entityC = new Entity{ Id = "entity/3", Name = "C" };

                session.Store(entityA);
                session.Store(entityB);
                session.Store(entityC);

                entityA.References = entityB.Id;
                entityB.References = entityC.Id;
                entityC.References = entityA.Id;

                session.SaveChanges();
            }
        }

        private void CreateDataWithMultipleEdgesOfTheSameType(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var arava = new Dog { Name = "Arava" }; //dogs/1
                var oscar = new Dog { Name = "Oscar" }; //dogs/2
                var pheobe = new Dog { Name = "Pheobe" }; //dogs/3

                session.Store(arava);
                session.Store(oscar);
                session.Store(pheobe);

                //dogs/1 => dogs/2
                arava.Likes = new[] { oscar.Id };
                arava.Dislikes = new[] { pheobe.Id };

                //dogs/2 => dogs/1,dogs/3 (cycle!)
                oscar.Likes = new[] { oscar.Id, pheobe.Id };
                oscar.Dislikes = new string[0];

                //dogs/3 => dogs/2
                pheobe.Likes = new[] { oscar.Id };
                pheobe.Dislikes = new[] { arava.Id };

                session.SaveChanges();
            }
        }

        private void CreateMoviesData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var scifi = new Genre
                {
                    Id = "genres/1",
                    Name = "Sci-Fi"
                };

                var fantasy = new Genre
                {
                    Id = "genres/2",
                    Name = "Fantasy"
                };

                var adventure = new Genre
                {
                    Id = "genres/3",
                    Name = "Adventure"
                };

                session.Store(scifi);
                session.Store(fantasy);
                session.Store(adventure);

                var starwars = new Movie
                {
                    Id = "movies/1",
                    Name = "Star Wars Ep.1",
                    Genres = new List<string>
                    {
                        "genres/1",
                        "genres/2"
                    }
                };

                var firefly = new Movie
                {
                    Id = "movies/2",
                    Name = "Firefly Serenity",
                    Genres = new List<string>
                    {
                        "genres/2",
                        "genres/3"
                    }
                };

                var indianaJones = new Movie
                {
                    Id = "movies/3",
                    Name = "Indiana Jones and the Temple Of Doom",
                    Genres = new List<string>
                    {
                        "genres/3"
                    }
                };

                session.Store(starwars);
                session.Store(firefly);
                session.Store(indianaJones);

                session.Store(new User
                {
                    Id = "users/1",
                    Name = "John Dow",
                    HasRated = new List<User.Rating>
                    {
                        new User.Rating
                        {
                            Movie = "movies/1",
                            Score = 5
                        },
                        new User.Rating
                        {
                            Movie = "movies/2",
                            Score = 7
                        }

                    }
                });

                session.Store(new User
                {
                    Id = "users/2",
                    Name = "Jane Dow",
                    HasRated = new List<User.Rating>
                    {
                        new User.Rating
                        {
                            Movie = "movies/2",
                            Score = 7
                        },
                        new User.Rating
                        {
                            Movie = "movies/3",
                            Score = 9
                        }

                    }
                });

                session.Store(new User
                {
                    Id = "users/3",
                    Name = "Jack Dow",
                    HasRated = new List<User.Rating>
                    {
                        new User.Rating
                        {
                            Movie = "movies/3",
                            Score = 5
                        }
                    }
                });


                session.SaveChanges();
            }
        }

        [Fact]
        public void FindReferences()
        {
            using (var store = GetDocumentStore())
            {
                CreateSimpleData(store);
                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<JObject>(@"match (e:Entities)-[:References]->(e2:Entities)").ToList();

                    Assert.Equal(3, result.Count);
                    Assert.Contains(result, item => item["e"].Value<JObject>("@metadata").Value<string>("@id") == "entity/1" && item["e2"].Value<JObject>("@metadata").Value<string>("@id") == "entity/2");
                    Assert.Contains(result, item => item["e"].Value<JObject>("@metadata").Value<string>("@id") == "entity/2" && item["e2"].Value<JObject>("@metadata").Value<string>("@id") == "entity/3");
                    Assert.Contains(result, item => item["e"].Value<JObject>("@metadata").Value<string>("@id") == "entity/3" && item["e2"].Value<JObject>("@metadata").Value<string>("@id") == "entity/1");
                }
            }
        }
        
        [Fact]
        public void Can_query_with_edge_defined_in_embedded_object()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    var moviesQueryResult = session.Advanced.RawQuery<JObject>(@"
                        match (u:Users(id() = 'users/2'))-[:HasRated.Movie]->(m:Movies) select m
                    ").ToList().Select(x => x["m"].ToObject<Movie>()).ToList();
                    
                    Assert.Equal(2,moviesQueryResult.Count);
                    Assert.Contains(moviesQueryResult.Select(x => x.Name), name => name == "Firefly Serenity" || name == "Indiana Jones and the Temple Of Doom");
                }
            }
        }

        [Fact]
        public void Graph_query_can_handle_edges_defined_in_property_with_whitespaces()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);

                //create properties with whitespace in their name
                var operation = store
                    .Operations
                    .Send(new PatchByQueryOperation(@"
                                      from Orders as o
                                      update
                                      {
                                          o['Order Lines'] = o.Lines
                                      }"));
                    
                operation.WaitForCompletion();

                using (var session = store.OpenSession())
                {
                    //note the whitespace in edge property name in the graph query
                    var resultsAsJson = session.Advanced
                        .RawQuery<JObject>(@"match (o:Orders (id() = 'orders/825-A'))-[:'Order Lines'.Product]->(p:Products) select p.Name as Name").ToList();
                    var productNamesFromMatch = resultsAsJson.Select(r => r["Name"].Value<string>()).ToArray();
                    Assert.Equal(4,productNamesFromMatch.Length); //sanity check

                    var query = session.Advanced.RawQuery<JObject>(@"from Orders where id() = 'orders/825-A' select Lines").ToArray();
                    var productsIdsFromDocumentQuery = query.Select(r => r["Lines"])
                        .SelectMany(x => x)
                        .Select(x => x.ToObject<OrderLine>().Product).ToArray();

                    var productNamesFromDocumentQuery = session.Load<Product>(productsIdsFromDocumentQuery).Select(x => x.Value.Name);
                    
                    //note : OrderByDescending is required because graph and document queries may give results in different order
                    Assert.Equal(productNamesFromDocumentQuery.OrderByDescending(x => x),productNamesFromMatch.OrderByDescending(x => x));
                }
                //
            }
        }

        [Fact(Skip = "Should not work until RavenDB-12073 is implemented")]
        public void Graph_query_should_return_proper_metadata()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    var moviesQueryResult = session.Advanced.RawQuery<JObject>(@"
                        match (u:Users(id() = 'users/2'))-[:HasRated.Movie]->(m:Movies) select m
                    ").ToList().Select(x => x["m"].ToObject<Movie>()).ToList();
                    
                    Assert.Equal(2,moviesQueryResult.Count);
                    //If proper metadata is retrieved, Ids here won't be null
                    Assert.False(moviesQueryResult.Any(x => x.Id == null));
                }
            }
        }

        [Fact(Skip = "Currently is not supposed to work until relevant issue are implemented. " +
                     "See RavenDB-12072 and RavenDB-12074")]
        public void Can_query_intersection_of_multiple_patterns()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<JObject>(@"
                        match (u1:Users)-[:HasRated(Score > 1).Movie]->(m:Movies) AND
                              (u2:Users)-[:HasRated.Movie]->(m:Movies)
                        select u1,u2
                    ").ToList();

                    Assert.NotEmpty(result);
                    Assert.Contains(result, item => item["u1"].Value<string>("Id")== "users/1" && item["u2"].Value<string>("Id") == "users/2");
                    Assert.Contains(result, item => item["u1"].Value<string>("Id") == "users/2" && item["u2"].Value<string>("Id") == "users/3");
                }
            }
        }

        [Fact]
        public void FindTwoFriendliesWhoPointToTheSameVertex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var arava = new Dog { Name = "Arava" }; //dogs/1
                    var oscar = new Dog { Name = "Oscar" }; //dogs/2
                    var pheobe = new Dog { Name = "Pheobe" }; //dogs/3

                    session.Store(arava);
                    session.Store(oscar);
                    session.Store(pheobe);

                    //dogs/1 => dogs/3
                    arava.Likes = new[] { pheobe.Id };

                    //dogs/2 => dogs/3
                    oscar.Likes = new[] { pheobe.Id };

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {                   
                    var friends = session.Advanced.RawQuery<JObject>(@"match (fst:Dogs)-[:Likes]->(snd:Dogs)")
                        .ToList();

                    var resultPairs = friends.Select(x => new
                    {
                        From = x["fst"]["Name"].Value<string>(),
                        To = x["snd"]["Name"].Value<string>()
                    }).ToArray();

                    Assert.Equal(2,resultPairs.Length);
                    Assert.Contains(resultPairs, item => item.From == "Arava" && item.To == "Pheobe");
                    Assert.Contains(resultPairs, item => item.From == "Oscar" && item.To == "Pheobe");
                }
            }
        }

        [Fact]
        public void FindFriendlies()
        {
            using (var store = GetDocumentStore())
            {
                CreateDataWithMultipleEdgesOfTheSameType(store);
                using (var session = store.OpenSession())
                {                   
                    var friends = session.Advanced.RawQuery<JObject>(@"match (fst:Dogs)-[:Likes]->(snd:Dogs)")
                                                  .ToList();

                    var resultPairs = friends.Select(x => new
                    {
                        From = x["fst"]["Name"].Value<string>(),
                        To = x["snd"]["Name"].Value<string>()
                    }).ToArray();
                    
                    //arava -> oscar
                    //oscar -> oscar, phoebe
                    //phoebe -> oscar
                    Assert.Equal(4,resultPairs.Length);
                    Assert.Contains(resultPairs, item => item.From == "Arava" && item.To == "Oscar");
                    Assert.Contains(resultPairs, item => item.From == "Oscar" && item.To == "Oscar");
                    Assert.Contains(resultPairs, item => item.From == "Oscar" && item.To == "Pheobe");
                    Assert.Contains(resultPairs, item => item.From == "Pheobe" && item.To == "Oscar");
                }
            }
        }

        [Fact]
        public void Match_without_any_parameters_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var one= store.OpenSession())
                using (var two = store.OpenSession())
                {
                    var orderFromMatch = one.Advanced.RawQuery<Order>(@"match (o:Orders (id() = 'orders/825-A'))").First();

                    var orderFromLoad = two.Load<Order>("orders/825-A");

                    //compare some meaningful properties, just to be sure
                    Assert.Equal(orderFromLoad.Id, orderFromMatch.Id);
                    Assert.Equal(orderFromLoad.Company,orderFromMatch.Company);
                    Assert.Equal(orderFromLoad.Employee,orderFromMatch.Employee);
                }
            }
        }

        [Fact]
        public void Matching_with_edge_defined_in_embedded_collection_with_array_brackets_syntax_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var resultsAsJson = session.Advanced
                        .RawQuery<JObject>(@"match (o:Orders (id() = 'orders/825-A'))-[:Lines[].Product]->(p:Products) select p.Name as Name").ToList();
                    var productNamesFromMatch = resultsAsJson.Select(r => r["Name"].Value<string>()).ToArray();
                    Assert.Equal(4,productNamesFromMatch.Length); //sanity check

                    var query = session.Advanced.RawQuery<JObject>(@"from Orders where id() = 'orders/825-A' select Lines").ToArray();
                    var productsIdsFromDocumentQuery = query.Select(r => r["Lines"])
                        .SelectMany(x => x)
                        .Select(x => x.ToObject<OrderLine>().Product).ToArray();

                    var productNamesFromDocumentQuery = session.Load<Product>(productsIdsFromDocumentQuery).Select(x => x.Value.Name);
                    
                    //note : OrderByDescending is required because graph and document queries may give results in different order
                    Assert.Equal(productNamesFromDocumentQuery.OrderByDescending(x => x),productNamesFromMatch.OrderByDescending(x => x));
                }
            }
        }

        [Fact(Skip = "Should not work until RavenDB-12072 is implemented")]
        public void Matching_with_edge_defined_in_embedded_collection_with_array_brackets_syntax_and_edge_filter_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var resultsAsJson = session.Advanced
                        .RawQuery<JObject>(@"match (o:Orders (id() = 'orders/825-A'))-[:Lines(ProductName = 'Chang')[].Product]->(p:Products) select p.Name as Name").ToList();
                    var productNameFromMatch = resultsAsJson.Select(r => r["Name"].Value<string>()).First();

                    var query = session.Advanced.RawQuery<OrderLine>(@"
                        declare function FilterOnProductName(lines,productName) {
                            for(var i = 0; i < lines.length; i++){
                                if(lines[i].ProductName == productName){
                                    return lines[i]
                                }
                            }
                            return null;
                        }

                        from Orders as o 
                        where id() = 'orders/825-A'
                        select FilterOnProductName(Lines,'Chang')
                    ").ToArray();
                    var productsIdFromDocumentQuery = query[0].Product;
                    var productNameFromDocumentQuery = session.Load<Product>(productsIdFromDocumentQuery).Name;

                    Assert.Equal(productNameFromDocumentQuery,productNameFromMatch);
                }
            }
        }

        [Fact(Skip = "Should not work until RavenDB-12072 is implemented")]
        public void Matching_with_edge_defined_in_embedded_collection_without_array_brackets_syntax_and_edge_filter_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var resultsAsJson = session.Advanced
                        .RawQuery<JObject>(@"match (o:Orders (id() = 'orders/825-A'))-[:Lines(ProductName = 'Chang').Product]->(p:Products) select p.Name as Name").ToList();
                    var productNameFromMatch = resultsAsJson.Select(r => r["Name"].Value<string>()).First();

                    var query = session.Advanced.RawQuery<OrderLine>(@"
                        declare function FilterOnProductName(lines,productName) {
                            for(var i = 0; i < lines.length; i++){
                                if(lines[i].ProductName == productName){
                                    return lines[i]
                                }
                            }
                            return null;
                        }

                        from Orders as o 
                        where id() = 'orders/825-A'
                        select FilterOnProductName(Lines,'Chang')
                    ").ToArray();
                    var productsIdFromDocumentQuery = query[0].Product;
                    var productNameFromDocumentQuery = session.Load<Product>(productsIdFromDocumentQuery).Name;

                    Assert.Equal(productNameFromDocumentQuery,productNameFromMatch);
                }
            }
        }

        [Fact]
        public void Matching_with_edge_defined_in_embedded_collection_without_array_brackets_syntax_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var resultsAsJson = session.Advanced
                        .RawQuery<JObject>(@"match (o:Orders (id() = 'orders/825-A'))-[:Lines.Product]->(p:Products) select p.Name as Name").ToList();
                    var productNamesFromMatch = resultsAsJson.Select(r => r["Name"].Value<string>()).ToArray();
                    Assert.Equal(4,productNamesFromMatch.Length); //sanity check

                    var query = session.Advanced.RawQuery<JObject>(@"from Orders where id() = 'orders/825-A' select Lines").ToArray();
                    var productsIdsFromDocumentQuery = query.Select(r => r["Lines"])
                        .SelectMany(x => x)
                        .Select(x => x.ToObject<OrderLine>().Product).ToArray();

                    var productNamesFromDocumentQuery = session.Load<Product>(productsIdsFromDocumentQuery).Select(x => x.Value.Name);
                    
                    //note : OrderByDescending is required because graph and document queries may give results in different order
                    Assert.Equal(productNamesFromDocumentQuery.OrderByDescending(x => x),productNamesFromMatch.OrderByDescending(x => x));
                }
            }
        }       

        [Fact]
        public void Matching_with_edge_defined_in_embedded_collection_and_select_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                using (var session = store.OpenSession())
                {
                    var matchQueryResultsAsJson = session.Advanced
                        .RawQuery<JObject>(@"match (o:Orders (id() = 'orders/825-A'))-[l:Lines.Product]->(p:Products) 
                                                    select o,l,p").ToList();
                    Assert.Equal(4, matchQueryResultsAsJson.Count); //sanity check                    

                    var orderFromMatchQuery = matchQueryResultsAsJson.First()["o"].ToObject<Order>();
                    var productNamesFromMatchQuery = matchQueryResultsAsJson.Select(x => x["p"].ToObject<Product>().Name).ToArray();
                    var orderLinesFromMatchQuery = matchQueryResultsAsJson.Select(x => x["l"].Select(ix => ix.ToObject<OrderLine>())).ToArray();

                    var orderFromDocumentQuery = session.Load<Order>("orders/825-A");
                    var linesQuery = session.Advanced.RawQuery<JObject>(@"from Orders where id() = 'orders/825-A' select Lines").ToArray();
                    var productsIdsFromDocumentQuery = linesQuery.Select(r => r["Lines"])
                                                                 .SelectMany(x => x)
                                                                 .Select(x => x.ToObject<OrderLine>().Product)
                                                                 .ToArray();

                    var productNamesFromDocumentQuery = session.Load<Product>(productsIdsFromDocumentQuery).Select(x => x.Value.Name);
                    var orderLinesFromDocumentQuery = linesQuery.Select(r => r["Lines"])
                        .Select(x => x.ToObject<OrderLine[]>()).ToArray();


                    //compare orders
                    Assert.Equal(orderFromDocumentQuery.Lines.Count, orderFromMatchQuery.Lines.Count);
                    Assert.Equal(orderFromDocumentQuery.Employee, orderFromMatchQuery.Employee);
                    Assert.Equal(orderFromDocumentQuery.Company, orderFromMatchQuery.Company);

                    //compare product names
                    Assert.Equal(productNamesFromDocumentQuery.OrderBy(x => x), productNamesFromMatchQuery.OrderBy(x => x));

                    //compare order lines
                    Assert.True(orderLinesFromMatchQuery.All(item => item.Count() == orderLinesFromDocumentQuery.First().Length));
                }
            }
        }
    }
}
