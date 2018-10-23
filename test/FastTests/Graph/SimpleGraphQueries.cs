using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries.Parser;
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
                    Name = "Jack",
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
                    Name = "Jill",
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
                    Name = "Bob",
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
                    var moviesQueryResult = session.Advanced.RawQuery<Movie>(@"
                        match (u:Users(id() = 'users/2'))-[:HasRated.Movie]->(m:Movies) select m
                    ").ToList();
                    
                    Assert.Equal(2,moviesQueryResult.Count);
                    Assert.Contains(moviesQueryResult.Select(x => x.Name), name => name == "Firefly Serenity" || name == "Indiana Jones and the Temple Of Doom");
                }
            }
        }

        [Fact]
        public void Can_flatten_result_for_single_vertex_in_row()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    var allVerticesQuery = session.Advanced.RawQuery<JObject>(@"match (v)").ToList();
                    Assert.False(allVerticesQuery.Any(row => row.ContainsKey("v"))); //we have "flat" results
                }
            }
        }

        [Fact]
        public void Mutliple_results_in_row_wont_flatten_results()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    var allVerticesQuery = session.Advanced.RawQuery<JObject>(@"match (u)-[:HasRated.Movie]->(m)").ToList();
                    Assert.True(allVerticesQuery.All(row => row.ContainsKey("m")));
                    Assert.True(allVerticesQuery.All(row => row.ContainsKey("u")));
                }
            }
        }


        [Fact]
        public void Can_query_without_collection_identifier()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    var allVerticesQuery = session.Advanced.RawQuery<JObject>(@"match (v)").ToList();
                    
                    Assert.Equal(9,allVerticesQuery.Count);
                    var docTypes = allVerticesQuery.Select(x => x["@metadata"]["@collection"].Value<string>()).ToArray();

                    Assert.Equal(3,docTypes.Count(t => t == "Genres"));
                    Assert.Equal(3,docTypes.Count(t => t == "Movies"));
                    Assert.Equal(3,docTypes.Count(t => t == "Users"));
                }
            }
        }

        [Fact]
        public void Can_use_explicit_with_clause()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                        with {from Users} as u
                        match (u)").ToList();
                    
                    Assert.Equal(3,results.Count);
                    var docTypes = results.Select(x => x["@metadata"]["@collection"].Value<string>()).ToArray();
                    Assert.Equal(3,docTypes.Count(t => t == "Users"));
                }
            }
        }

        [Fact]
        public void Can_filter_vertices_with_explicit_with_clause()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                        with {from Users where id() = 'users/2'} as u
                        match (u) select u.Name").ToList().Select(x => x["Name"].Value<string>()).ToArray();
                    
                    Assert.Equal(1,results.Length);
                    results[0] = "Jill";
                }
            }
        }


        [Fact]
        public void Empty_vertex_node_should_fail_the_query()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<Movie>(@"
                        match ()-[:HasRated.Movie]->(m:Movies) select m
                    ").ToList());
                }
            }
        }

        [Fact]
        public void Can_query_with_vertices_from_map_reduce_index()
        {
            using (var store = GetDocumentStore())
            {
                CreateNorthwindDatabase(store);
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                        with { from index 'Orders/ByCompany' order by Count as long desc } as o
                        match (o)-[:Company]->(c:Companies)
                    ").ToList();

                    Assert.NotEmpty(results); //sanity check

                    var companiesInIndex = session.Advanced.RawQuery<JObject>(@"from index 'Orders/ByCompany' select Company").ToList();
                    var companyNames = session.Load<dynamic>(companiesInIndex.Select(x => x["Company"].Value<string>()))
                                              .Select(x => (string)x.Value.Name).ToArray();
                    Assert.Equal(companyNames.Length,results.Count);

                    var companiesFetchedFromGraphQuery = results.Select(x => x["c"]["Name"].Value<string>()).ToArray();
                    foreach (var c in companyNames)
                    {
                        Assert.Contains(c, companiesFetchedFromGraphQuery);
                    }
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
                    WaitForUserToContinueTheTest(store);
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

        [Fact]
        public void Graph_query_should_return_data_in_proper_form()
        //note: for more information see http://issues.hibernatingrhinos.com/issue/RavenDB-12088
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    var moviesQueryResult = session.Advanced.RawQuery<Movie>(@"
                        match (u:Users(id() = 'users/2'))-[:HasRated.Movie]->(m:Movies) select m
                    ").ToList();
                        
                    Assert.Equal(2,moviesQueryResult.Count); //sanity check
                    
                    //If the data retrieved has proper json format, Ids here won't be null as they will be populated
                    //by the same client-side code that handles document query results
                    Assert.False(moviesQueryResult.Any(x => x.Id == null));
                    Assert.Contains("movies/2", moviesQueryResult.Select(x => x.Id));
                    Assert.Contains("movies/3", moviesQueryResult.Select(x => x.Id));
                }
            }
        }

        [Fact]
        public void Can_query_multiple_match_clauses_with_union_no_intersecting_results()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Movie
                    {
                        Name = "M1"
                    },"movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    },"movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    },"movies/3");

                    session.Store(new User
                    {
                        Name = "A",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/1" }
                        }
                    });
                    session.Store(new User
                    {
                        Name = "B",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/2" }
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                       match (u1:Users(Name = 'A'))-[:HasRated.Movie]->(m:Movies)
                             OR
                             (u2:Users(Name = 'B'))-[:HasRated.Movie]->(m:Movies)
                       select u1.Name as u1, m.Name as movie, u2.Name as u2
                    ").ToList().Select(x => new
                    {
                        u1 = x["u1"]?.Value<string>(),
                        u2 = x["u2"]?.Value<string>(),
                        m = x["movie"].Value<string>()
                    }).ToList();

                    Assert.NotEmpty(results);
                    Assert.Equal(2,results.Count);
                    Assert.True(results.Any(x => x.u1 == "A" && x.m == "M1" && x.u2 == null));
                    Assert.True(results.Any(x => x.u1 == null && x.m == "M2" && x.u2 == "B"));
                }
            }
        }

        [Fact]
        public void Can_query_multiple_match_clauses_with_intersection_no_intersecting_results()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Movie
                    {
                        Name = "M1"
                    },"movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    },"movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    },"movies/3");

                    session.Store(new User
                    {
                        Name = "A",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/1" }
                        }
                    });
                    session.Store(new User
                    {
                        Name = "B",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/2" }
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                       match (u1:Users(Name = 'A'))-[:HasRated.Movie]->(m:Movies)
                             AND
                             (u2:Users(Name = 'B'))-[:HasRated.Movie]->(m:Movies)
                       select u1.Name as u1, m.Name as movie, u2.Name as u2
                    ").ToList();

                    //when doing intersection (AND) and there is no intersection in match clause results
                    Assert.Empty(results);
                }
            }
        }

        [Fact]
        public void Can_query_multiple_match_clauses_with_intersection_no_intersecting_results_and_right_clause_has_no_results()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Movie
                    {
                        Name = "M1"
                    },"movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    },"movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    },"movies/3");

                    session.Store(new User
                    {
                        Name = "A",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/1" }
                        }
                    });
                    session.Store(new User
                    {
                        Name = "B",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/2" }
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                       match (u1:Users(Name = 'A'))-[:HasRated.Movie]->(m:Movies)
                             AND
                             (u2:Users(Name = 'NON-EXISTENT'))-[:HasRated.Movie]->(m:Movies)
                       select u1.Name as u1, m.Name as movie, u2.Name as u2
                    ").ToList();

                    Assert.Empty(results);
                }
            }
        }

        [Fact]
        public void Can_query_multiple_match_clauses_with_intersection_no_intersecting_results_and_left_clause_has_no_results()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Movie
                    {
                        Name = "M1"
                    },"movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    },"movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    },"movies/3");

                    session.Store(new User
                    {
                        Name = "A",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/1" }
                        }
                    });
                    session.Store(new User
                    {
                        Name = "B",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/2" }
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                       match (u1:Users(Name = 'NON-EXISTENT'))-[:HasRated.Movie]->(m:Movies)
                             AND
                             (u2:Users(Name = 'B'))-[:HasRated.Movie]->(m:Movies)
                       select u1.Name as u1, m.Name as movie, u2.Name as u2
                    ").ToList();

                    Assert.Empty(results);
                }
            }
        }

        [Fact]
        public void And_Not_should_return_empty_results_where_ALL_results_intersect()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Movie
                    {
                        Name = "M1"
                    },"movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    },"movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    },"movies/3");

                    session.Store(new User
                    {
                        Name = "A",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/1" },
                            new User.Rating{ Movie = "movies/2" }
                        }
                    });
                    session.Store(new User
                    {
                        Name = "B",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/1" },
                            new User.Rating{ Movie = "movies/2" }
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                       match (u1:Users(Name = 'A'))-[:HasRated.Movie]->(m:Movies)
                             AND 
NOT
                             (u2:Users(Name = 'B'))-[:HasRated.Movie]->(m:Movies)
                       select u1.Name as u1, m.Name as movie, u2.Name as u2
                    ").ToList();

                    //when doing intersection (AND) and there is no intersection in match clause results
                    Assert.Empty(results);
                }
            }
        }

        [Fact]
        public void And_Not_should_return_only_results_that_dont_intersect()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Movie
                    {
                        Name = "M1"
                    },"movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    },"movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    },"movies/3");

                    session.Store(new User
                    {
                        Name = "A",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/1" },
                            new User.Rating{ Movie = "movies/2" }
                        }
                    });
                    session.Store(new User
                    {
                        Name = "B",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/1" },
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                       match (u1:Users(Name = 'A'))-[:HasRated.Movie]->(m:Movies)
                             AND NOT
                             (u2:Users(Name = 'B'))-[:HasRated.Movie]->(m:Movies)
                       select u1.Name as u1, m.Name as movie, u2.Name as u2
                    ").ToList().Select(x => new
                    {
                        u1 = x["u1"]?.Value<string>(),
                        u2 = x["u2"]?.Value<string>(),
                        m = x["movie"].Value<string>()
                    }).ToList();
                    
                    Assert.NotEmpty(results);
                    Assert.Equal(1,results.Count);
                    Assert.True(results.Any(x => x.u1 == "A" && x.m == "M2" && x.u2 == null));
                }
            }
        }

         [Fact]
        public void And_Not_should_return_only_results_that_dont_intersect_even_if_right_clause_has_empty_results()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Movie
                    {
                        Name = "M1"
                    },"movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    },"movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    },"movies/3");

                    session.Store(new User
                    {
                        Name = "A",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/1" },
                            new User.Rating{ Movie = "movies/2" }
                        }
                    });
                    session.Store(new User
                    {
                        Name = "B",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/1" },
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                       match (u1:Users(Name = 'A'))-[:HasRated.Movie]->(m:Movies)
                             AND NOT
                             (u2:Users(Name = 'NON-EXISTENT'))-[:HasRated.Movie]->(m:Movies)
                       select u1.Name as u1, m.Name as movie, u2.Name as u2
                    ").ToList().Select(x => new
                    {
                        u1 = x["u1"]?.Value<string>(),
                        u2 = x["u2"]?.Value<string>(),
                        m = x["movie"].Value<string>()
                    }).ToList();
                    
                    Assert.NotEmpty(results);
                    Assert.Equal(2,results.Count);
                    Assert.True(results.Any(x => x.u1 == "A" && x.m == "M1" && x.u2 == null));
                    Assert.True(results.Any(x => x.u1 == "A" && x.m == "M2" && x.u2 == null));
                }
            }
        }

        
        [Fact]
        public void Can_query_multiple_match_clauses_with_union_partial()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Movie
                    {
                        Name = "M1"
                    },"movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    },"movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    },"movies/3");

                    session.Store(new User
                    {
                        Name = "A",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/1" },
                            new User.Rating{ Movie = "movies/3" }
                        }
                    });
                    session.Store(new User
                    {
                        Name = "B",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/2" },
                            new User.Rating{ Movie = "movies/3" }
                        }
                    });

                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                       match (u1:Users(Name = 'A'))-[:HasRated.Movie]->(m:Movies)
                             OR
                             (u2:Users(Name = 'B'))-[:HasRated.Movie]->(m:Movies)
                       select u1.Name as u1, m.Name as movie, u2.Name as u2
                    ").ToList().Select(x => new
                    {
                        u1 = x["u1"]?.Value<string>(),
                        u2 = x["u2"]?.Value<string>(),
                        m = x["movie"].Value<string>()
                    }).ToList();

                    Assert.NotEmpty(results);
                    Assert.Equal(4,results.Count);
                    Assert.True(results.Any(x => x.u1 == "A" && x.m == "M3" && x.u2 == "B"));
                    Assert.True(results.Any(x => x.u1 == "A" && x.m == "M1" && x.u2 == null));
                    Assert.True(results.Any(x => x.u1 == "A" && x.m == "M3" && x.u2 == null));
                    Assert.True(results.Any(x => x.u1 == null && x.m == "M2" && x.u2 == "B"));
                }
            }
        }

        [Fact]
        public void Can_query_multiple_match_clauses_with_union_and_left_clause_results_are_empty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Movie
                    {
                        Name = "M1"
                    },"movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    },"movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    },"movies/3");

                    session.Store(new User
                    {
                        Name = "A",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/1" },
                            new User.Rating{ Movie = "movies/3" }
                        }
                    });
                    session.Store(new User
                    {
                        Name = "B",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/2" },
                            new User.Rating{ Movie = "movies/3" }
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                       match (u1:Users(Name = 'NON-EXISTENT'))-[:HasRated.Movie]->(m:Movies)
                             OR
                             (u2:Users(Name = 'B'))-[:HasRated.Movie]->(m:Movies)
                       select u1.Name as u1, m.Name as movie, u2.Name as u2
                    ").ToList().Select(x => new
                    {
                        u1 = x["u1"]?.Value<string>(),
                        u2 = x["u2"]?.Value<string>(),
                        m = x["movie"].Value<string>()
                    }).ToList();

                    Assert.NotEmpty(results);
                    Assert.Equal(2,results.Count);
                    Assert.True(results.Any(x => x.u1 == null && x.m == "M3" && x.u2 == "B"));
                    Assert.True(results.Any(x => x.u1 == null && x.m == "M2" && x.u2 == "B"));
                }
            }
        }

        [Fact]
        public void Can_query_multiple_match_clauses_with_union_and_both_clause_results_are_empty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Movie
                    {
                        Name = "M1"
                    },"movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    },"movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    },"movies/3");

                    session.Store(new User
                    {
                        Name = "A",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/1" },
                            new User.Rating{ Movie = "movies/3" }
                        }
                    });
                    session.Store(new User
                    {
                        Name = "B",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/2" },
                            new User.Rating{ Movie = "movies/3" }
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                       match (u1:Users(Name = 'NON-EXISTENT'))-[:HasRated.Movie]->(m:Movies)
                             OR
                             (u2:Users(Name = 'NON-EXISTENT2'))-[:HasRated.Movie]->(m:Movies)
                       select u1.Name as u1, m.Name as movie, u2.Name as u2
                    ").ToList();

                    Assert.Empty(results);
                }
            }
        }

        [Fact]
        public void Can_query_multiple_match_clauses_with_intersect_and_both_clause_results_are_empty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Movie
                    {
                        Name = "M1"
                    },"movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    },"movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    },"movies/3");

                    session.Store(new User
                    {
                        Name = "A",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/1" },
                            new User.Rating{ Movie = "movies/3" }
                        }
                    });
                    session.Store(new User
                    {
                        Name = "B",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/2" },
                            new User.Rating{ Movie = "movies/3" }
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                       match (u1:Users(Name = 'NON-EXISTENT'))-[:HasRated.Movie]->(m:Movies)
                             AND
                             (u2:Users(Name = 'NON-EXISTENT2'))-[:HasRated.Movie]->(m:Movies)
                       select u1.Name as u1, m.Name as movie, u2.Name as u2
                    ").ToList();

                    Assert.Empty(results);
                }
            }
        }

        [Fact]
        public void Can_query_multiple_match_clauses_with_except_and_both_clause_results_are_empty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Movie
                    {
                        Name = "M1"
                    },"movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    },"movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    },"movies/3");

                    session.Store(new User
                    {
                        Name = "A",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/1" },
                            new User.Rating{ Movie = "movies/3" }
                        }
                    });
                    session.Store(new User
                    {
                        Name = "B",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/2" },
                            new User.Rating{ Movie = "movies/3" }
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                       match (u1:Users(Name = 'NON-EXISTENT'))-[:HasRated.Movie]->(m:Movies)
                             AND NOT
                             (u2:Users(Name = 'NON-EXISTENT2'))-[:HasRated.Movie]->(m:Movies)
                       select u1.Name as u1, m.Name as movie, u2.Name as u2
                    ").ToList();

                    Assert.Empty(results);
                }
            }
        }

        [Fact]
        public void Invalid_intersection_operator_in_match_clauyse_should_fail_properly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Movie
                    {
                        Name = "M1"
                    },"movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    },"movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    },"movies/3");

                    session.Store(new User
                    {
                        Name = "A",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/1" },
                            new User.Rating{ Movie = "movies/3" }
                        }
                    });
                    session.Store(new User
                    {
                        Name = "B",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/2" },
                            new User.Rating{ Movie = "movies/3" }
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Throws<RavenException>(() => 
                        session.Advanced.RawQuery<JObject>(@"
                           match (u1:Users(Name = 'NON-EXISTENT'))-[:HasRated.Movie]->(m:Movies)
                                  FOOBAR 
                                 (u2:Users(Name = 'NON-EXISTENT2'))-[:HasRated.Movie]->(m:Movies)
                           select u1.Name as u1, m.Name as movie, u2.Name as u2
                        ").ToList());
                }
            }
        }

        [Fact]
        public void Can_query_multiple_match_clauses_with_union_and_right_clause_results_are_empty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Movie
                    {
                        Name = "M1"
                    },"movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    },"movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    },"movies/3");

                    session.Store(new User
                    {
                        Name = "A",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/1" },
                            new User.Rating{ Movie = "movies/3" }
                        }
                    });
                    session.Store(new User
                    {
                        Name = "B",
                        HasRated = new List<User.Rating>
                        {
                            new User.Rating{ Movie = "movies/2" },
                            new User.Rating{ Movie = "movies/3" }
                        }
                    });

                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                       match (u1:Users(Name = 'A'))-[:HasRated.Movie]->(m:Movies)
                             OR
                             (u2:Users(Name = 'NON-EXISTENT'))-[:HasRated.Movie]->(m:Movies)
                       select u1.Name as u1, m.Name as movie, u2.Name as u2
                    ").ToList().Select(x => new
                    {
                        u1 = x["u1"]?.Value<string>(),
                        u2 = x["u2"]?.Value<string>(),
                        m = x["movie"].Value<string>()
                    }).ToList();

                    Assert.NotEmpty(results);
                    Assert.Equal(2,results.Count);
                    Assert.True(results.Any(x => x.u1 == "A" && x.m == "M3" && x.u2 == null));
                    Assert.True(results.Any(x => x.u1 == "A" && x.m == "M1" && x.u2 == null));
                }
            }
        }

        [Fact]
        public void Can_query_multiple_match_clauses_with_explicit_intersection()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                WaitForUserToContinueTheTest(store);
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                       match (u1:Users)-[:HasRated(Score > 1).Movie]->(m:Movies(id() = 'movies/2'))
                         and (u2:Users)-[:HasRated.Movie]->(m:Movies(id() = 'movies/2'))
                       select u1.Name as U1,u2.Name as U2
                    ").ToList().Select(x => new
                    {
                        u1 = x["U1"].Value<string>(),
                        u2 = x["U2"].Value<string>(),
                    }).ToList();

                    //since we didn't use "where" clause to make sure (u1 != u2), we would have all permutations
                    Assert.NotEmpty(results);
                    Assert.Equal(4, results.Count);
                    Assert.Contains(results, item => item.u1 == "Jack" && item.u2 == "Jill");
                    Assert.Contains(results, item => item.u1 == "Jack" && item.u2 == "Jack");
                    Assert.Contains(results, item => item.u1 == "Jill" && item.u2 == "Jill");
                    Assert.Contains(results, item => item.u1 == "Jill" && item.u2 == "Jack");
                }
            }
        }

        [Fact]
        public void Can_query_multiple_match_clauses_with_implicit_intersection()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                       match (u1:Users)-[:HasRated(Score > 1).Movie]->(m:Movies(id() = 'movies/2'))<-[:HasRated.Movie]-(u2:Users)                              
                       select u1.Name as U1,u2.Name as U2
                    ").ToList().Select(x => new
                    {
                        u1 = x["U1"].Value<string>(),
                        u2 = x["U2"].Value<string>(),
                    }).ToList();

                    //since we didn't use "where" clause to make sure (u1 != u2), we would have all permutations
                    Assert.NotEmpty(results);
                    Assert.Equal(4, results.Count);
                    Assert.Contains(results, item => item.u1 == "Jack" && item.u2 == "Jill");
                    Assert.Contains(results, item => item.u1 == "Jack" && item.u2 == "Jack");
                    Assert.Contains(results, item => item.u1 == "Jill" && item.u2 == "Jill");
                    Assert.Contains(results, item => item.u1 == "Jack" && item.u2 == "Jack");
                }
            }
        }

        [Fact]
        public void Incomplete_intersection_query_should_properly_fail()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    Assert.Throws<InvalidQueryException>(() =>
                        session.Advanced.RawQuery<JObject>(@"
                            match (u1:Users)-[:HasRated(Score > 1).Movie]->(m:Movies) AND
                            select u1,u2
                        ").ToList());
                }
            }
        }

        [Fact]
        public void Incomplete_union_query_should_properly_fail()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    Assert.Throws<InvalidQueryException>(() =>
                        session.Advanced.RawQuery<JObject>(@"
                            match (u1:Users)-[:HasRated(Score > 1).Movie]->(m:Movies) OR
                            select u1,u2
                        ").ToList());
                }
            }
        }

        [Fact(Skip = "Should not work until RavenDB-12075 is implemented")]
        public void Graph_query_missing_FROM_vertex_should_fail_properly()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    Assert.Throws<InvalidQueryException>(() =>
                        session.Advanced.RawQuery<JObject>(@"
                            match [:HasRated(Score > 1).Movie]->(m:Movies) 
                        ").ToList());
                }
            }
        }

        [Fact(Skip = "Should not work until RavenDB-12075 is implemented")]
        public void Graph_query_missing_TO_vertex_should_fail_properly()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    Assert.Throws<InvalidQueryException>(() =>
                        session.Advanced.RawQuery<JObject>(@"
                            match (u1:Users)-[:HasRated(Score > 1).Movie]
                        ").ToList());
                }
            }
        }

        [Fact]
        public void Query_with_duplicate_implicit_aliases_in_select_should_fail_properly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var arava = new Dog
                    {
                        Name = "Arava",
                        Likes = new[] {"dogs/1", "dogs/2"}
                    }; //dogs/1
                    var oscar = new Dog
                    {
                        Name = "Oscar"
                    }; //dogs/2
                    var pheobe = new Dog
                    {
                        Name = "Pheobe",
                        Likes = new[] {"dogs/2"}
                    }; //dogs/3

                    session.Store(arava, "dogs/1");
                    session.Store(oscar, "dogs/2");
                    session.Store(pheobe, "dogs/3");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Throws<InvalidQueryException>(() =>
                        session.Advanced.RawQuery<JObject>(@"
                        match (a:Dogs)-[:Likes]->(b:Dogs)-[:Likes]->(c:dogs)
                        select a.Name,b.Name") // <- this is wrong because we have two implicit "Name" aliases in select clause
                            .ToList());
                }
            }
        }

        [Fact]
        public void Query_with_duplicate_explicit_aliases_in_select_should_fail_properly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var arava = new Dog
                    {
                        Name = "Arava",
                        Likes = new[] {"dogs/1", "dogs/2"}
                    }; //dogs/1
                    var oscar = new Dog
                    {
                        Name = "Oscar"
                    }; //dogs/2
                    var pheobe = new Dog
                    {
                        Name = "Pheobe",
                        Likes = new[] {"dogs/2"}
                    }; //dogs/3

                    session.Store(arava, "dogs/1");
                    session.Store(oscar, "dogs/2");
                    session.Store(pheobe, "dogs/3");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Throws<InvalidQueryException>(() =>
                        session.Advanced.RawQuery<JObject>(@"
                        match (a:Dogs)-[:Likes]->(b:Dogs)-[:Likes]->(c:dogs)
                        select a.Name AS Foo,b.Name AS Foo") // <- this is wrong because we have two explicit "Foo" aliases in select clause
                            .ToList());
                }
            }
        }

        [Fact]
        public void Query_with_multiple_hops_in_the_same_direction_should_work()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var arava = new Dog
                    {
                        Name = "Arava",
                        Likes = new []{ "dogs/1","dogs/2" }
                    }; //dogs/1
                    var oscar = new Dog
                    {
                        Name = "Oscar"
                    }; //dogs/2
                    var pheobe = new Dog
                    {
                        Name = "Pheobe",
                        Likes = new []{ "dogs/2" }
                    }; //dogs/3

                    session.Store(arava,"dogs/1");
                    session.Store(oscar,"dogs/2");
                    session.Store(pheobe, "dogs/3");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {                   
                    //note : such query implies implicit intersection between
                    // a -[likes]-> b and b -[likes]-> c, but it doesn't execute interesection-related code
                    var friends = session.Advanced.RawQuery<JObject>(@"
                        match (a:Dogs)-[:Likes]->(b:Dogs)-[:Likes]->(c:dogs)
                        select a.Name as A,b.Name as B,c.Name as C
                        ")
                        .ToList();

                    var resultPairs = friends.Select(x => new
                    {
                        A = x["A"]?.Value<string>(),
                        B = x["B"]?.Value<string>(),
                        C = x["C"]?.Value<string>()
                    }).ToArray();

                    Assert.Equal(2,resultPairs.Length);
                    Assert.Contains(resultPairs, item => item.A == "Arava" && item.B == "Arava" && item.C == "Arava");
                    Assert.Contains(resultPairs, item => item.A == "Arava" && item.B == "Arava" && item.C == "Oscar");
                }
            }
        }

        [Fact]
        public void Query_with_multiple_hops_that_are_cycle_should_work()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var arava = new Dog
                    {
                        Name = "Arava",
                        Likes = new []{ "dogs/2" }
                    }; //dogs/1
                    var oscar = new Dog
                    {
                        Name = "Oscar",
                        Likes = new []{ "dogs/3" }

                    }; //dogs/2
                    var pheobe = new Dog
                    {
                        Name = "Pheobe",
                        Likes = new []{ "dogs/1" }
                    }; //dogs/3

                    session.Store(arava,"dogs/1");
                    session.Store(oscar,"dogs/2");
                    session.Store(pheobe,"dogs/3");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {                   
                    //note : such query implies implicit intersection between
                    // a -[likes]-> b and b -[likes]-> c, but it doesn't execute interesection-related code
                    var friends = session.Advanced.RawQuery<JObject>(@"
                        match (a:Dogs)-[:Likes]->(b:Dogs)-[:Likes]->(c:dogs)
                        select a.Name as A,b.Name as B,c.Name as C
                        ")
                        .ToList();

                    var resultPairs = friends.Select(x => new
                    {
                        A = x["A"]?.Value<string>(),
                        B = x["B"]?.Value<string>(),
                        C = x["C"]?.Value<string>()
                    }).ToArray();

                    Assert.Equal(3,resultPairs.Length);
                    Assert.Contains(resultPairs, item => item.A == "Arava" && item.B == "Oscar" && item.C == "Pheobe");
                    Assert.Contains(resultPairs, item => item.A == "Oscar" && item.B == "Pheobe" && item.C == "Arava");
                    Assert.Contains(resultPairs, item => item.A == "Pheobe" && item.B == "Arava" && item.C == "Oscar");
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
        public void Only_undefined_alias_in_SELECT_should_properly_fail()
        {
            using (var store = GetDocumentStore())
            {
                CreateDataWithMultipleEdgesOfTheSameType(store);

                using (var session = store.OpenSession())
                {
                    //should throw because "foobar" is not defined in the query
                    Assert.Throws<InvalidQueryException>(() => 
                        session.Advanced.RawQuery<JObject>(@"match (fst:Dogs)-[:Likes]->(snd:Dogs) select foobar").ToArray());
                }
            }
        }

        [Fact]
        public void Proper_and_undefined_alias_in_SELECT_should_properly_fail()
        {
            using (var store = GetDocumentStore())
            {
                CreateDataWithMultipleEdgesOfTheSameType(store);

                using (var session = store.OpenSession())
                {
                    //should throw because "foobar" is not defined in the query
                    Assert.Throws<InvalidQueryException>(() => 
                        session.Advanced.RawQuery<JObject>(@"match (fst:Dogs)-[:Likes]->(snd:Dogs) select fst,foobar,snd").ToArray());
                }
            }
        }

        [Fact]
        public void FindFriendlies_with_javascript_select_should_work()
        {
            using (var store = GetDocumentStore())
            {
                CreateDataWithMultipleEdgesOfTheSameType(store);
                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {                   
                    var friends = session.Advanced.RawQuery<JObject>(@"match (fst:Dogs)-[:Likes]->(snd:Dogs) select { a : fst, b: snd }")
                        .ToList();

                    var resultPairs = friends.Select(x => new
                    {
                        From = x["a"]["Name"].Value<string>(),
                        To = x["b"]["Name"].Value<string>()
                    }).ToArray();

                    //arava -> oscar
                    //oscar -> oscar, phoebe
                    //phoebe -> oscar
                    Assert.Equal(4, resultPairs.Length);
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

        [Fact]
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

        [Fact]
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

        [Fact(Skip="Should work after RavenDB-12089 is resolved")]
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
                    var test = matchQueryResultsAsJson.Select(x => x["l"]).ToArray();
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
