using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Xunit;

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
                var arava = new Dog { Name = "Arava" };
                var oscar = new Dog { Name = "Oscar" };
                var pheobe = new Dog { Name = "Pheobe" };

                session.Store(arava);
                session.Store(oscar);
                session.Store(pheobe);

                arava.Likes = new[] { oscar.Id };
                arava.Dislikes = new[] { pheobe.Id };

                oscar.Likes = new[] { oscar.Id, pheobe.Id };

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
                    Assert.Contains(result, item => item["e"].Value<string>("Id") == "entity/1" && item["e2"].Value<string>("Id") == "entity/2");
                    Assert.Contains(result, item => item["e"].Value<string>("Id") == "entity/2" && item["e2"].Value<string>("Id") == "entity/3");
                    Assert.Contains(result, item => item["e"].Value<string>("Id") == "entity/3" && item["e2"].Value<string>("Id") == "entity/1");
                }
            }
        }

        [Fact(Skip = "Currently is not supposed to work. See RavenDB-11707")]
        public void FindReferencesWithTypo()
        {
            using (var store = GetDocumentStore())
            {
                CreateSimpleData(store);
                using (var session = store.OpenSession())
                {
                    Assert.Throws<InvalidOperationException>(() => 
                        session.Advanced.RawQuery<dynamic>(@"match (e:Entity)-[:References]->(e2:Entity)")
                                        .ToList());
                }
            }
        }

        [Fact(Skip = "Currently is not supposed to work. See RavenDB-11706")]
        public void FindUsersWhoRatedTheSameMovie()
        {
            using (var store = GetDocumentStore())
            {
                CreateMoviesData(store);
                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<JObject>(@"
                        match (u1:Users)-[:HasRated(Score > 1).Movie]->(Movies)<-[:HasRated.Movie]-(u2:Users)
                        select u1,u2
                    ").ToList();

                    Assert.NotEmpty(result);
                    Assert.Contains(result, item => item["u1"].Value<string>("Id")== "users/1" && item["u2"].Value<string>("Id") == "users/2");
                    Assert.Contains(result, item => item["u1"].Value<string>("Id") == "users/2" && item["u2"].Value<string>("Id") == "users/3");
                }
            }
        }

        [Fact(Skip = "Currently is not supposed to work. See RavenDB-11704")]
        public void FindFriendlies()
        {
            using (var store = GetDocumentStore())
            {
                CreateDataWithMultipleEdgesOfTheSameType(store);
                using (var session = store.OpenSession())
                {                   
                    var friends = session.Advanced.RawQuery<JObject>(@"match (fst:Dogs)-[:Likes]->(snd:Dogs)")
                                                  .ToList();

                    Assert.Contains(friends, result => result["fst"].Value<string>("Id") == "dogs/1-A" && result["snd"].Value<string>("Id") == "dogs/2-A");
                    Assert.Contains(friends, result => result["fst"].Value<string>("Id") == "dogs/2-A" && result["snd"].Value<string>("Id") == "dogs/2-A");
                    Assert.Contains(friends, result => result["fst"].Value<string>("Id") == "dogs/2-A" && result["snd"].Value<string>("Id") == "dogs/3-A");
                    Assert.Contains(friends, result => result["fst"].Value<string>("Id") == "dogs/3-A" && result["snd"].Value<string>("Id") == "dogs/2-A");
                }
            }
        }
    }
}
