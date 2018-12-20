using System;
using System.Linq;
using FastTests;
using FastTests.Graph;
using Newtonsoft.Json.Linq;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12206 : RavenTestBase
    {
        [Fact]
        public void Edge_array_with_filter_should_work()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Dog
                    {
                        Name = "Arava",
                        Likes = new[] {"dogs/2-A"}
                    });
                    session.Store(new Dog
                    {
                        Name = "Oscar",
                        Likes = new[] {"dogs/3-A"}
                    });
                    session.Store(new Dog
                    {
                        Name = "Pheobe",
                        Likes = new[] {"dogs/1-A"}
                    });
                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                {               
                    //simple projection
                    var results = session.Advanced.RawQuery<JObject>(@"match (Dogs as d1)-[Likes as l where Likes = 'dogs/2-A']->(Dogs) select l")
                                                    .ToList().Select(x => x["l"].Value<string>()).ToArray();

                    Assert.Equal(1, results.Length);
                    Assert.Contains(results, x => x == "dogs/2-A");
                }
            }
        }

        public class User
        {
            public string Name { get; set; } 
            public int[] CoworkerIds { get; set; }
        }

        [Fact]
        public void Edges_with_non_string_or_object_projections_should_fail()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "John Dow",
                        CoworkerIds = new[] { 2 }
                    },"users/1");
                    session.Store(new User
                    {
                        Name = "Jane Dow",
                        CoworkerIds = new[] { 3, 4 }
                    }, "users/2");

                    session.Store(new User
                    {
                        Name = "Jack Dow",
                        CoworkerIds = Array.Empty<int>()
                    });

                    session.Store(new User
                    {
                        Name = "Jennifer Dow",
                        CoworkerIds = Array.Empty<int>()
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Throws<InvalidQueryException>(() =>
                        session.Advanced.RawQuery<JObject>(@"
                            match (Users as u1 where id() = 'users/2')-[CoworkerIds as ids where ids > 10]->(Users as u2)"
                        ).ToList());

                    Assert.Throws<InvalidQueryException>(() =>
                        session.Advanced.RawQuery<JObject>(@"
                            match (Users as u1 where id() = 'users/2')-[CoworkerIds as ids where ids > 10 select ids]->(Users as u2)"
                        ).ToList());
                }
            }
        }

        [Fact]
        public void Projection_with_edge_array_with_filter_should_work()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Dog
                    {
                        Name = "Arava",
                        Likes = new[] {"dogs/2-A"}
                    });
                    session.Store(new Dog
                    {
                        Name = "Oscar",
                        Likes = new[] {"dogs/3-A"}
                    });
                    session.Store(new Dog
                    {
                        Name = "Pheobe",
                        Likes = new[] {"dogs/1-A"}
                    });
                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                {
                    //simple projection
                    var results = session.Advanced.RawQuery<JObject>(@"match (Dogs as d1)-[Likes as l where Likes = 'dogs/2-A' select Likes]->(Dogs) select l")
                        .ToList().Select(x => x["l"].Value<string>()).ToArray();

                    Assert.Equal(1, results.Length);
                    Assert.Contains(results, x => x == "dogs/2-A");

                    //simple projection with alias in select
                    results = session.Advanced.RawQuery<JObject>(@"match (Dogs as d1)-[Likes as l where Likes = 'dogs/2-A' select l]->(Dogs) select l")
                        .ToList().Select(x => x["l"].Value<string>()).ToArray();

                    Assert.Equal(1, results.Length);
                    Assert.Contains(results, x => x == "dogs/2-A");

                    //simple projection with alias in where
                    results = session.Advanced.RawQuery<JObject>(@"match (Dogs as d1)-[Likes as l where l = 'dogs/2-A']->(Dogs) select l")
                        .ToList().Select(x => x["l"].Value<string>()).ToArray();

                    Assert.Equal(1, results.Length);
                    Assert.Contains(results, x => x == "dogs/2-A");

                }
            }
        }        
    }
}
