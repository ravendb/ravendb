using System.Collections.Generic;
using Raven.Client.Documents.Commands;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Threading;
using Xunit;

namespace FastTests.Client
{
    public class Load :  RavenTestBase
    {
        [Fact]
        public void Load_Document_By_id()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var user = newSession.Load<User>("users/1");
                    Assert.NotNull(user);
                    Assert.Equal(user.Name, "RavenDB");
                }
            }
        }

        [Fact]
        public void Load_Documents_By_ids()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "RavenDB"}, "users/1");
                    session.Store(new User {Name = "Hibernating Rhinos"}, "users/2");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var user = newSession.Load<User>(new[] {"users/1", "users/2"});
                    Assert.Equal(user.Count, 2);
                }
            }
        }

        [Fact]
        public void Load_Null_Should_Return_Null()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Tony Montana" }, "users/1");
                    session.Store(new User { Name = "Tony Soprano" }, "users/2");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var user1 = newSession.Load<User>((string) null);
                    Assert.Null(user1);
                }
            }
        }

        [Fact]
        public void Load_Multi_Ids_With_Null_Should_Return_Dictionary_Without_nulls()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Tony Montana" }, "users/1");
                    session.Store(new User { Name = "Tony Soprano" }, "users/2");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var orderedArrayOfIdsWithNull = new[] { "users/1", null, "users/2", null };
                    var users1 = newSession.Load<User>(orderedArrayOfIdsWithNull);
                    User user1;
                    User user2;
                    users1.TryGetValue("users/1", out user1);
                    users1.TryGetValue("users/2", out user2);

                    Assert.NotNull(user1);
                    Assert.NotNull(user2);

                    var unorderedSetOfIdsWithNull = new HashSet<string>() { "users/1", null, "users/2", null };
                    var users2 = newSession.Load<User>(unorderedSetOfIdsWithNull);
                    Assert.Equal(users2.Count, 2);
                }
            }
        }

        [Fact]
        public void Load_Document_With_Int_Array_And_Long_Array()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new GeekPerson { Name = "Bebop", FavoritePrimes = new[] { 13, 43, 443, 997 }, FavoriteVeryLargePrimes = new[] { 5000000029, 5000000039 } }, "geeks/1");
                    session.Store(new GeekPerson { Name = "Rocksteady", FavoritePrimes = new[] { 2, 3, 5, 7 }, FavoriteVeryLargePrimes = new[] { 999999999989 } }, "geeks/2");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var geek1 = newSession.Load<GeekPerson>("geeks/1");
                    var geek2 = newSession.Load<GeekPerson>("geeks/2");

                    Assert.Equal(geek1.FavoritePrimes[1], 43);
                    Assert.Equal(geek1.FavoriteVeryLargePrimes[1], 5000000039);

                    Assert.Equal(geek2.FavoritePrimes[3], 7);
                    Assert.Equal(geek2.FavoriteVeryLargePrimes[0], 999999999989);
                }
            }
        }

        [Fact]
        public void Should_Load_Many_Ids_As_Post_Request()
        {
            using (var store = GetDocumentStore())
            {
                var ids = new List<string>();
                using (var session = store.OpenSession())
                {
                    // Length of all the ids together should be larger than 1024 for POST request
                    for (int i = 0; i < 200; i++)
                    {
                        var id = "users/" + i;
                        ids.Add(id);

                        session.Store(new User()
                        {
                            Name = "Person " + i
                        }, id);
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Load<User>(ids);
                    User user77;
                    users.TryGetValue("users/77", out user77);
                    Assert.NotNull(user77);
                    Assert.Equal(user77.Id, "users/77");
                }
            }
        }

        [Fact]
        public void Should_Load_Many_Ids_As_Post_Request_Directly()
        {
            using (var store = GetDocumentStore())
            {
                var ids = new List<string>();
                using (var session = store.OpenSession())
                {
                    // Length of all the ids together should be larger than 1024 for POST request
                    for (int i = 0; i < 200; i++)
                    {
                        var id = "users/" + i;
                        ids.Add(id);
                        session.Store(new User()
                        {
                            Name = "Person " + i
                        }, id);
                    }
                    session.SaveChanges();
                }
                var rq1 = store.GetRequestExecutor();
                var cmd = new GetDocumentsCommand(ids.ToArray(), null, true);
                using (var ctx = new JsonOperationContext(1024, 1024, SharedMultipleUseFlag.None))
                {
                    rq1.Execute(cmd, ctx);
                }
            }
        }
    }
}
