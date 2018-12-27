using System.Collections.Generic;
using System.Linq;
using FastTests;
using FastTests.Graph;
using Newtonsoft.Json.Linq;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Graph
{
    public class IntersectionTests : RavenTestBase
    {
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
                    }, "movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    }, "movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    }, "movies/3");

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
                       match (Users as u1 where Name = 'A')-[HasRated select Movie]->(Movies as m)
                             OR
                             (Users as u2 where Name = 'B')-[HasRated select Movie]->(Movies as m)
                       select u1.Name as u1, m.Name as movie, u2.Name as u2
                    ").ToList().Select(x => new
                    {
                        u1 = x["u1"]?.Value<string>(),
                        u2 = x["u2"]?.Value<string>(),
                        m = x["movie"].Value<string>()
                    }).ToList();

                    Assert.NotEmpty(results);
                    Assert.Equal(2, results.Count);
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
                    }, "movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    }, "movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    }, "movies/3");

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
                       match (Users as u1 where Name = 'A')-[HasRated select Movie]->(Movies as m)
                             AND
                             (Users as u2 where Name = 'B')-[HasRated select Movie]->(Movies as m)
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
                    }, "movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    }, "movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    }, "movies/3");

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
                       match (Users as u1 where Name = 'A')-[HasRated select Movie]->(Movies as m)
                             AND
                             (Users as u2 where Name = 'NON-EXISTENT')-[HasRated select Movie]->(Movies as m)
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
                    }, "movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    }, "movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    }, "movies/3");

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
                       match (Users as u1 where Name = 'NON-EXISTENT')-[HasRated select Movie]->(Movies as m)
                             AND
                             (Users as u2 where Name = 'B')-[HasRated select Movie]->(Movies as m)
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
                    }, "movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    }, "movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    }, "movies/3");

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
                       match (Users as u1 where Name = 'A')-[HasRated select Movie]->(Movies as m)
                             AND NOT
                             (Users as u2 where Name = 'B')-[HasRated select Movie]->(Movies as m)
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
                    }, "movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    }, "movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    }, "movies/3");

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
                       match (Users as u1 where Name = 'A')-[HasRated select Movie]->(Movies as m)
                             AND NOT
                             (Users as u2 where Name = 'B')-[HasRated select Movie]->(Movies as m)
                       select u1.Name as u1, m.Name as movie, u2.Name as u2
                    ").ToList().Select(x => new
                    {
                        u1 = x["u1"]?.Value<string>(),
                        u2 = x["u2"]?.Value<string>(),
                        m = x["movie"].Value<string>()
                    }).ToList();

                    Assert.NotEmpty(results);
                    Assert.Equal(1, results.Count);
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
                    }, "movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    }, "movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    }, "movies/3");

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
                       match (Users as u1 where Name = 'A')-[HasRated select Movie]->(Movies as m)
                             AND NOT
                             (Users as u2 where Name = 'NON-EXISTENT')-[HasRated select Movie]->(Movies as m)
                       select u1.Name as u1, m.Name as movie, u2.Name as u2
                    ").ToList().Select(x => new
                    {
                        u1 = x["u1"]?.Value<string>(),
                        u2 = x["u2"]?.Value<string>(),
                        m = x["movie"].Value<string>()
                    }).ToList();

                    Assert.NotEmpty(results);
                    Assert.Equal(2, results.Count);
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
                    }, "movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    }, "movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    }, "movies/3");

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
                       match (Users as u1 where Name = 'A')-[HasRated select Movie]->(Movies as m)
                             OR
                             (Users as u2 where Name = 'B')-[HasRated select Movie]->(Movies as m)
                       select u1.Name as u1, m.Name as movie, u2.Name as u2
                    ").ToList().Select(x => new
                    {
                        u1 = x["u1"]?.Value<string>(),
                        u2 = x["u2"]?.Value<string>(),
                        m = x["movie"].Value<string>()
                    }).ToList();

                    Assert.NotEmpty(results);
                    Assert.Equal(4, results.Count);
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
                    }, "movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    }, "movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    }, "movies/3");

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
                       match (Users as u1 where Name = 'NON-EXISTENT')-[HasRated select Movie]->(Movies as m)
                             OR
                             (Users as u2 where Name = 'B')-[HasRated select Movie]->(Movies as m)
                       select u1.Name as u1, m.Name as movie, u2.Name as u2
                    ").ToList().Select(x => new
                    {
                        u1 = x["u1"]?.Value<string>(),
                        u2 = x["u2"]?.Value<string>(),
                        m = x["movie"].Value<string>()
                    }).ToList();

                    Assert.NotEmpty(results);
                    Assert.Equal(2, results.Count);
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
                    }, "movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    }, "movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    }, "movies/3");

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
                       match (Users as u1 where Name = 'NON-EXISTENT')-[HasRated select Movie]->(Movies as m)
                             OR
                             (Users as u2 where Name = 'NON-EXISTENT2')-[HasRated select Movie]->(Movies as m)
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
                    }, "movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    }, "movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    }, "movies/3");

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
                       match (Users as u1 where Name = 'NON-EXISTENT')-[HasRated select Movie]->(Movies as m)
                             AND
                             (Users as u2 where Name = 'NON-EXISTENT2')-[HasRated select Movie]->(Movies as m)
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
                    }, "movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    }, "movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    }, "movies/3");

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
                       match (Users as u1 where Name = 'NON-EXISTENT')-[HasRated select Movie]->(Movies as m)
                             AND NOT
                             (Users as u2 where Name = 'NON-EXISTENT2')-[HasRated select Movie]->(Movies as m)
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
                    }, "movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    }, "movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    }, "movies/3");

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
                           match (Users as u1 where Name = 'NON-EXISTENT')-[HasRated select Movie]->(Movies as m)
                                  FOOBAR 
                                 (Users as u2 where Name = 'NON-EXISTENT2')-[HasRated select Movie]->(Movies as m)
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
                    }, "movies/1");

                    session.Store(new Movie
                    {
                        Name = "M2"
                    }, "movies/2");

                    session.Store(new Movie
                    {
                        Name = "M3"
                    }, "movies/3");

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
                       match (Users as u1 where Name = 'A')-[HasRated select Movie]->(Movies as m)
                             OR
                             (Users as u2 where Name = 'NON-EXISTENT')-[HasRated select Movie]->(Movies as m)
                       select u1.Name as u1, m.Name as movie, u2.Name as u2
                    ").ToList().Select(x => new
                    {
                        u1 = x["u1"]?.Value<string>(),
                        u2 = x["u2"]?.Value<string>(),
                        m = x["movie"].Value<string>()
                    }).ToList();

                    Assert.NotEmpty(results);
                    Assert.Equal(2, results.Count);
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
                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<JObject>(@"
                       match(Users as u1)-[HasRated where Score > 1 select Movie]->(Movies as m where id() = 'movies/2')
                         and (Users as u2)-[HasRated select Movie]->(Movies as m where id() = 'movies/2')
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
                       match(Users as u1)-[HasRated where Score > 1 select Movie]->(Movies as m where id() = 'movies/2')<-[HasRated select Movie]-(Users as u2)                              
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
                            match(Users as u1)-[HasRated where Score > 1 select Movie]->(Movies as m) AND
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
                            match(Users as u1)-[HasRated where Score > 1 select Movie]->(Movies as m) OR
                            select u1,u2
                        ").ToList());
                }
            }
        }
    }
}
