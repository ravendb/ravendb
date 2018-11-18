using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client.Counters
{
    public class QueryOnCounters : RavenTestBase
    {
        [Fact]
        public void RawQuerySelectSingleCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Downloads", 200);
                    session.CountersFor("users/3-A").Increment("Likes", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<CounterResult>("from users select counter(\"Downloads\")")
                        .ToList();

                    Assert.Equal(3, query.Count);
                    Assert.Equal(100, query[0].Downloads);
                    Assert.Equal(200, query[1].Downloads);
                    Assert.Null(query[2].Downloads);
                }
            }
        }

        [Fact]
        public void RawQuerySelectSingleCounterWithDocAlias()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Downloads", 200);
                    session.CountersFor("users/3-A").Increment("Likes", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<CounterResult>("from users as u select counter(u, \"Downloads\")")
                        .ToList();

                    Assert.Equal(3, query.Count);
                    Assert.Equal(100, query[0].Downloads);
                    Assert.Equal(200, query[1].Downloads);
                    Assert.Null(query[2].Downloads);
                }
            }
        }

        [Fact]
        public void RawQuerySelectSingleCounterWithCounterAlias()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("downloads", 100);
                    session.CountersFor("users/2-A").Increment("downloads", 200);
                    session.CountersFor("users/3-A").Increment("likes", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<CounterResult2>("from users select counter(\"downloads\") as DownloadsCount")
                        .ToList();

                    Assert.Equal(3, query.Count);
                    Assert.Equal(100, query[0].DownloadsCount);
                    Assert.Equal(200, query[1].DownloadsCount);
                    Assert.Null(query[2].DownloadsCount);
                }
            }
        }

        [Fact]
        public void RawQuerySelectSingleCounterRawValues()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("downloads", 100);
                    session.CountersFor("users/2-A").Increment("downloads", 200);
                    session.CountersFor("users/3-A").Increment("likes", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<CounterResult3>("from users as u select counterRaw(u, \"downloads\")")
                        .ToList();

                    Assert.Equal(3, query.Count);

                    Assert.Equal(100, query[0].Downloads.Values.Single());
                    Assert.Equal(200, query[1].Downloads.Values.Single());
                    Assert.Equal(0, query[2].Downloads.Count);
                }
            }
        }

        [Fact]
        public void RawQuerySelectMultipuleCounters()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/1-A").Increment("Likes", 200);

                    session.CountersFor("users/2-A").Increment("Downloads", 400);
                    session.CountersFor("users/2-A").Increment("Likes", 800);

                    session.CountersFor("users/3-A").Increment("Likes", 1600);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<CounterResult>("from users select counter(\"Downloads\"), counter(\"Likes\")")
                        .ToList();

                    Assert.Equal(3, query.Count);
                    Assert.Equal(100, query[0].Downloads);
                    Assert.Equal(200, query[0].Likes);

                    Assert.Equal(400, query[1].Downloads);
                    Assert.Equal(800, query[1].Likes);

                    Assert.Null(query[2].Downloads);
                    Assert.Equal(1600, query[2].Likes);
                }
            }
        }

        [Fact]
        public void RawQuerySimpleProjectionWithCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen" }, "users/3-A");


                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Downloads", 200);
                    session.CountersFor("users/3-A").Increment("Likes", 400);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<CounterResult>("from users select Name, counter(\"Downloads\")")
                        .ToList();

                    Assert.Equal(3, query.Count);

                    Assert.Equal("Jerry", query[0].Name);
                    Assert.Equal(100, query[0].Downloads);

                    Assert.Equal("Bob", query[1].Name);
                    Assert.Equal(200, query[1].Downloads);

                    Assert.Equal("Pigpen", query[2].Name);
                    Assert.Null(query[2].Downloads);
                }
            }
        }

        [Fact]
        public void RawQueryJsProjectionWithCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("downloads", 100);
                    session.CountersFor("users/2-A").Increment("downloads", 200);
                    session.CountersFor("users/3-A").Increment("likes", 400);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<CounterResult>("from users as u select { Name : u.Name, Downloads : counter(u, \"downloads\") }")
                        .ToList();

                    Assert.Equal(3, query.Count);

                    Assert.Equal("Jerry", query[0].Name);
                    Assert.Equal(100, query[0].Downloads);

                    Assert.Equal("Bob", query[1].Name);
                    Assert.Equal(200, query[1].Downloads);

                    Assert.Equal("Pigpen", query[2].Name);
                    Assert.Null(query[2].Downloads);
                }
            }
        }

        [Fact]
        public void RawQueryJsProjectionWithCounterRawValues()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("downloads", 100);
                    session.CountersFor("users/1-A").Increment("likes", 200);

                    session.CountersFor("users/2-A").Increment("downloads", 300);
                    session.CountersFor("users/2-A").Increment("likes", 400);


                    session.CountersFor("users/3-A").Increment("likes", 500);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<CounterResult4>("from users as u select { Name : u.Name, Downloads : counter(u, \"downloads\"), Likes : counterRaw(u, \"likes\") }")
                        .ToList();

                    Assert.Equal(3, query.Count);

                    Assert.Equal("Jerry", query[0].Name);
                    Assert.Equal(100, query[0].Downloads);
                    Assert.Equal(200, query[0].Likes.Values.Single());

                    Assert.Equal("Bob", query[1].Name);
                    Assert.Equal(300, query[1].Downloads);
                    Assert.Equal(400, query[1].Likes.Values.Single());

                    Assert.Equal("Pigpen", query[2].Name);
                    Assert.Equal(500, query[2].Likes.Values.Single());
                    Assert.Null(query[2].Downloads);
                }
            }
        }

        [Fact]
        public void RawQuerySimpleProjectionWithParameters()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/1-A").Increment("Likes", 200);

                    session.CountersFor("users/2-A").Increment("Downloads", 300);
                    session.CountersFor("users/2-A").Increment("Likes", 400);

                    session.CountersFor("users/3-A").Increment("Downloads", 500);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<CounterResult>("from users select counter($p0), counter($p1)")
                        .AddParameter("p0", "Downloads")
                        .AddParameter("p1", "Likes")
                        .ToList();

                    Assert.Equal(3, query.Count);
                    Assert.Equal(100, query[0].Downloads);
                    Assert.Equal(200, query[0].Likes);

                    Assert.Equal(300, query[1].Downloads);
                    Assert.Equal(400, query[1].Likes);

                    Assert.Equal(500, query[2].Downloads);
                    Assert.Null(query[2].Likes);
                }
            }
        }

        [Fact]
        public void RawQueryJsProjectionWithParameters()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/1-A").Increment("Likes", 200);

                    session.CountersFor("users/2-A").Increment("Downloads", 300);
                    session.CountersFor("users/2-A").Increment("Likes", 400);

                    session.CountersFor("users/3-A").Increment("Downloads", 500);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<CounterResult2>("from users as u select { DownloadsCount: counter(u, $p0), LikesCount: counter(u, $p1) }")
                        .AddParameter("p0", "Downloads")
                        .AddParameter("p1", "Likes")
                        .ToList();

                    Assert.Equal(3, query.Count);
                    Assert.Equal(100, query[0].DownloadsCount);
                    Assert.Equal(200, query[0].LikesCount);

                    Assert.Equal(300, query[1].DownloadsCount);
                    Assert.Equal(400, query[1].LikesCount);

                    Assert.Equal(500, query[2].DownloadsCount);
                    Assert.Null(query[2].LikesCount);
                }
            }
        }

        [Fact]
        public void RawQuerySelectCounterWithParameterAndAlias()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("downloads", 100);
                    session.CountersFor("users/2-A").Increment("downloads", 300);
                    session.CountersFor("users/3-A").Increment("downloads", 500);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<CounterResult2>("from users select counter($p0) as DownloadsCount")
                        .AddParameter("p0", "downloads")
                        .ToList();

                    Assert.Equal(3, query.Count);
                    Assert.Equal(100, query[0].DownloadsCount);
                    Assert.Equal(300, query[1].DownloadsCount);
                    Assert.Equal(500, query[2].DownloadsCount);
                }
            }
        }

        [Fact]
        public void RawQuerySelectCounterWithWhere()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", Age = 75 }, "users/1-A");
                    session.Store(new User { Name = "Bob", Age = 68 }, "users/2-A");
                    session.Store(new User { Name = "Pigpen", Age = 71 }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Downloads", 200);
                    session.CountersFor("users/3-A").Increment("Likes", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<CounterResult>("from users where Age > 70 select Name, counter(\"Downloads\")")
                        .ToList();

                    Assert.Equal(2, query.Count);

                    Assert.Equal("Jerry", query[0].Name);
                    Assert.Equal(100, query[0].Downloads);

                    Assert.Equal("Pigpen", query[1].Name);
                    Assert.Null(query[1].Downloads);

                }
            }
        }

        [Fact]
        public void RawQuerySelectCounterFromLoadedDoc()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", FriendId = "users/2-A" }, "users/1-A");
                    session.Store(new User { Name = "Bob", FriendId = "users/3-A" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen", FriendId = "users/1-A" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Downloads", 200);
                    session.CountersFor("users/3-A").Increment("Likes", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<CounterResult7>(@"from users as u 
                                                    load u.FriendId as f 
                                                    select Name,
                                                           counter(u, $p0),
                                                           counter(f, $p0) as FriendsDownloads")
                        .AddParameter("p0", "Downloads")
                        .ToList();

                    Assert.Equal(3, query.Count);

                    Assert.Equal("Jerry", query[0].Name);
                    Assert.Equal(100, query[0].Downloads);
                    Assert.Equal(200, query[0].FriendsDownloads);

                    Assert.Equal("Bob", query[1].Name);
                    Assert.Equal(200, query[1].Downloads);
                    Assert.Null(query[1].FriendsDownloads);

                    Assert.Equal("Pigpen", query[2].Name);
                    Assert.Null(query[2].Downloads);
                    Assert.Equal(100, query[2].FriendsDownloads);

                }
            }
        }

        [Fact]
        public void RawQuerySelectCounterFromLoadedDocJsProjection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", FriendId = "users/2-A" }, "users/1-A");
                    session.Store(new User { Name = "Bob", FriendId = "users/3-A" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen", FriendId = "users/1-A" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Downloads", 200);
                    session.CountersFor("users/3-A").Increment("Likes", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<CounterResult7>(@"from users as u 
                                                 load u.FriendId as f 
                                                 select  
                                                 { 
                                                    Name: u.Name, 
                                                    Downloads : counter(u, $p0), 
                                                    FriendsDownloads : counter(f, $p0)  
                                                 }")
                        .AddParameter("p0", "Downloads")
                        .ToList();

                    Assert.Equal(3, query.Count);

                    Assert.Equal("Jerry", query[0].Name);
                    Assert.Equal(100, query[0].Downloads);
                    Assert.Equal(200, query[0].FriendsDownloads);

                    Assert.Equal("Bob", query[1].Name);
                    Assert.Equal(200, query[1].Downloads);
                    Assert.Null(query[1].FriendsDownloads);

                    Assert.Equal("Pigpen", query[2].Name);
                    Assert.Null(query[2].Downloads);
                    Assert.Equal(100, query[2].FriendsDownloads);

                }
            }
        }

        [Fact]
        public void RawQueryGetCounterValueFromMetadata()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Likes", 500);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<CounterResult6>(@"
                            from Users as u
                            select
                            {
                                Counter: counter(u, u[""@metadata""][""@counters""][0])
                            }")
                        .ToList();

                    Assert.Equal(2, query.Count);
                    Assert.Equal(100, query[0].Counter);
                    Assert.Equal(500, query[1].Counter);

                }
            }
        }

        [Fact]
        public void RawQueryGetAllCountersValuesFromMetadata()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/1-A").Increment("Likes", 200);

                    session.CountersFor("users/2-A").Increment("Downloads", 300);
                    session.CountersFor("users/2-A").Increment("Likes", 400);

                    session.CountersFor("users/3-A").Increment("Downloads", 500);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<CounterResult5>(@"
                            declare function getCounters(doc)
                            {
                                var names = doc[""@metadata""][""@counters""];
                                if (names == null)
                                    return names;
                                var res = { };
                                var length = names.length;
                                for (var i = 0; i < length; i++)
                                {
                                    var curName = names[i];
                                    res[curName] = counter(doc, curName);
                                }
                                return res;
                            }
                            from Users as u
                            select
                            {
                                Counters: getCounters(u)
                            }")
                        .ToList();

                    Assert.Equal(3, query.Count);

                    Assert.Equal(100, query[0].Counters["Downloads"]);
                    Assert.Equal(200, query[0].Counters["Likes"]);

                    Assert.Equal(300, query[1].Counters["Downloads"]);
                    Assert.Equal(400, query[1].Counters["Likes"]);

                    Assert.Equal(500, query[2].Counters["Downloads"]);
                    Assert.False(query[2].Counters.ContainsKey("Likes"));
                }
            }
        }

        [Fact]
        public void SessionQuerySelectSingleCounter_UsingRavenQueryCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Downloads", 200);
                    session.CountersFor("users/3-A").Increment("Likes", 300);


                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                                select RavenQuery.Counter("Downloads");

                    Assert.Equal("from Users select counter(Downloads) as Downloads", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(3, queryResult.Count);
                    Assert.Equal(100, queryResult[0]);
                    Assert.Equal(200, queryResult[1]);
                    Assert.Null(queryResult[2]);
                }
            }
        }

        [Fact]
        public void SessionQuerySelectSingleCounterWithDocAlias_UsingRavenQueryCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Downloads", 200);
                    session.CountersFor("users/3-A").Increment("Likes", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                                select RavenQuery.Counter(user, "Downloads");

                    Assert.Equal("from Users as user select counter(user, Downloads) as Downloads", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(3, queryResult.Count);
                    Assert.Equal(100, queryResult[0]);
                    Assert.Equal(200, queryResult[1]);
                    Assert.Null(queryResult[2]);
                }
            }
        }

        [Fact]
        public void SessionQuerySelectCounterAndProjectToNewAnonymousType_UsingRavenQueryCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Downloads", 200);
                    session.CountersFor("users/3-A").Increment("Likes", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                                select new
                                {
                                    DownloadsCount = RavenQuery.Counter(user, "Downloads")
                                };

                    Assert.Equal("from Users as user select counter(user, Downloads) as DownloadsCount", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(3, queryResult.Count);
                    Assert.Equal(100, queryResult[0].DownloadsCount);
                    Assert.Equal(200, queryResult[1].DownloadsCount);
                    Assert.Null(queryResult[2].DownloadsCount);
                }
            }
        }

        [Fact]
        public void SessionQuerySelectCounterAndProjectToMemberInit_UsingRavenQueryCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Downloads", 200);
                    session.CountersFor("users/3-A").Increment("Likes", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                                select new CounterResult2
                                {
                                    DownloadsCount = RavenQuery.Counter(user, "Downloads")
                                };

                    Assert.Equal("from Users as user select counter(user, Downloads) as DownloadsCount", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(3, queryResult.Count);
                    Assert.Equal(100, queryResult[0].DownloadsCount);
                    Assert.Equal(200, queryResult[1].DownloadsCount);
                    Assert.Null(queryResult[2].DownloadsCount);
                }
            }
        }

        [Fact]
        public void SessionQuerySelectCounterFromLoadedDoc_UsingRavenQueryCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", FriendId = "users/2-A" }, "users/1-A");
                    session.Store(new User { Name = "Bob", FriendId = "users/3-A" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen", FriendId = "users/1-A" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Downloads", 200);
                    session.CountersFor("users/3-A").Increment("Likes", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                let f = RavenQuery.Load<User>(u.FriendId)
                                select RavenQuery.Counter(f, "Downloads");

                    Assert.Equal("from Users as u load u.FriendId as f " +
                                 "select counter(f, Downloads) as Downloads", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(3, queryResult.Count);

                    Assert.Equal(200, queryResult[0]);
                    Assert.Null(queryResult[1]);
                    Assert.Equal(100, queryResult[2]);

                }
            }
        }

        [Fact]
        public void SessionQuerySelectMultipuleCounters_UsingRavenQueryCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/1-A").Increment("Likes", 200);

                    session.CountersFor("users/2-A").Increment("Downloads", 400);
                    session.CountersFor("users/2-A").Increment("Likes", 800);

                    session.CountersFor("users/3-A").Increment("Likes", 1600);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                                select new
                                {
                                    Downloads = RavenQuery.Counter(user, "Downloads"),
                                    Likes = RavenQuery.Counter(user, "Likes")
                                };

                    Assert.Equal("from Users as user " +
                                 "select counter(user, Downloads) as Downloads, " +
                                        "counter(user, Likes) as Likes"
                                , query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(3, queryResult.Count);
                    Assert.Equal(100, queryResult[0].Downloads);
                    Assert.Equal(200, queryResult[0].Likes);

                    Assert.Equal(400, queryResult[1].Downloads);
                    Assert.Equal(800, queryResult[1].Likes);

                    Assert.Null(queryResult[2].Downloads);
                    Assert.Equal(1600, queryResult[2].Likes);
                }
            }
        }

        [Fact]
        public void SessionQuerySelectSingleCounterWithDocAlias_UsingSessionCountersFor()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Downloads", 200);
                    session.CountersFor("users/3-A").Increment("Likes", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                                select session.CountersFor(user).Get("Downloads");

                    Assert.Equal("from Users as user select counter(user, Downloads) as Downloads", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(3, queryResult.Count);
                    Assert.Equal(100, queryResult[0]);
                    Assert.Equal(200, queryResult[1]);
                    Assert.Null(queryResult[2]);
                }
            }
        }

        [Fact]
        public void SessionQuerySelectCounterAndProjectToNewAnonymousType_UsingSessionGetCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Downloads", 200);
                    session.CountersFor("users/3-A").Increment("Likes", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                                select new
                                {
                                    DownloadsCount = session.CountersFor(user).Get("Downloads")
                                };

                    Assert.Equal("from Users as user select counter(user, Downloads) as DownloadsCount", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(3, queryResult.Count);
                    Assert.Equal(100, queryResult[0].DownloadsCount);
                    Assert.Equal(200, queryResult[1].DownloadsCount);
                    Assert.Null(queryResult[2].DownloadsCount);
                }
            }
        }

        [Fact]
        public void SessionQuerySelectCounterAndProjectToMemberInit_UsingSessionCountersFor()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Downloads", 200);
                    session.CountersFor("users/3-A").Increment("Likes", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                                select new CounterResult2
                                {
                                    DownloadsCount = session.CountersFor(user).Get("Downloads")
                                };

                    Assert.Equal("from Users as user select counter(user, Downloads) as DownloadsCount", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(3, queryResult.Count);
                    Assert.Equal(100, queryResult[0].DownloadsCount);
                    Assert.Equal(200, queryResult[1].DownloadsCount);
                    Assert.Null(queryResult[2].DownloadsCount);
                }
            }
        }

        [Fact]
        public void SessionQuerySelectCounterFromLoadedDoc_UsingSessionCountersFor()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", FriendId = "users/2-A" }, "users/1-A");
                    session.Store(new User { Name = "Bob", FriendId = "users/3-A" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen", FriendId = "users/1-A" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Downloads", 200);
                    session.CountersFor("users/3-A").Increment("Likes", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                let f = RavenQuery.Load<User>(u.FriendId)
                                select session.CountersFor(f).Get("Downloads");

                    Assert.Equal("from Users as u load u.FriendId as f " +
                                 "select counter(f, Downloads) as Downloads", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(3, queryResult.Count);

                    Assert.Equal(200, queryResult[0]);
                    Assert.Null(queryResult[1]);
                    Assert.Equal(100, queryResult[2]);

                }
            }
        }

        [Fact]
        public void SessionQuerySelectMultipuleCounters_UsingSessionCountersFor()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/1-A").Increment("Likes", 200);

                    session.CountersFor("users/2-A").Increment("Downloads", 400);
                    session.CountersFor("users/2-A").Increment("Likes", 800);

                    session.CountersFor("users/3-A").Increment("Likes", 1600);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                                select new
                                {
                                    Downloads = session.CountersFor(user).Get("Downloads"),
                                    Likes = session.CountersFor(user).Get("Likes")
                                };

                    Assert.Equal("from Users as user " +
                                 "select counter(user, Downloads) as Downloads, " +
                                        "counter(user, Likes) as Likes"
                                , query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(3, queryResult.Count);
                    Assert.Equal(100, queryResult[0].Downloads);
                    Assert.Equal(200, queryResult[0].Likes);

                    Assert.Equal(400, queryResult[1].Downloads);
                    Assert.Equal(800, queryResult[1].Likes);

                    Assert.Null(queryResult[2].Downloads);
                    Assert.Equal(1600, queryResult[2].Likes);
                }
            }
        }


        [Fact]
        public void SessionQuerySelectSingleCounterJsProjection_UsingRavenQueryCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", Age = 55 }, "users/1-A");
                    session.Store(new User { Name = "Bob", Age = 68 }, "users/2-A");
                    session.Store(new User { Name = "Pigpen", Age = 27 }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Downloads", 200);
                    session.CountersFor("users/3-A").Increment("Likes", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                                select new
                                {
                                    Name = user.Name + user.Age, //creates js projection
                                    Downloads = RavenQuery.Counter(user, "Downloads")
                                };

                    Assert.Equal("from Users as user select { Name : user.Name+user.Age, Downloads : counter(user, \"Downloads\") }"
                                , query.ToString());

                    var queryResult = query.ToList();
                    Assert.Equal(3, queryResult.Count);

                    Assert.Equal("Jerry55", queryResult[0].Name);
                    Assert.Equal(100, queryResult[0].Downloads);

                    Assert.Equal("Bob68", queryResult[1].Name);
                    Assert.Equal(200, queryResult[1].Downloads);

                    Assert.Equal("Pigpen27", queryResult[2].Name);
                    Assert.Null(queryResult[2].Downloads);

                }
            }
        }

        [Fact]
        public void SessionQuerySelectSingleCounterJsProjection_UsingSessionCountersFor()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", Age = 55 }, "users/1-A");
                    session.Store(new User { Name = "Bob", Age = 68 }, "users/2-A");
                    session.Store(new User { Name = "Pigpen", Age = 27 }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Downloads", 200);
                    session.CountersFor("users/3-A").Increment("Likes", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                                select new
                                {
                                    Name = user.Name + user.Age, //creates js projection
                                    Downloads = session.CountersFor(user).Get("Downloads")
                                };

                    Assert.Equal("from Users as user select { Name : user.Name+user.Age, Downloads : counter(user, \"Downloads\") }"
                                , query.ToString());

                    var queryResult = query.ToList();
                    Assert.Equal(3, queryResult.Count);

                    Assert.Equal("Jerry55", queryResult[0].Name);
                    Assert.Equal(100, queryResult[0].Downloads);

                    Assert.Equal("Bob68", queryResult[1].Name);
                    Assert.Equal(200, queryResult[1].Downloads);

                    Assert.Equal("Pigpen27", queryResult[2].Name);
                    Assert.Null(queryResult[2].Downloads);

                }
            }
        }

        [Fact]
        public void SessionQuerySelectMultipleCounters_UsingBothSessionCountersForAndRavenQueryCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", Age = 55 }, "users/1-A");
                    session.Store(new User { Name = "Bob", Age = 68 }, "users/2-A");
                    session.Store(new User { Name = "Pigpen", Age = 27 }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Downloads", 200);
                    session.CountersFor("users/3-A").Increment("Likes", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                                select new
                                {
                                    Likes = session.CountersFor(user).Get("Likes"),
                                    Downloads = RavenQuery.Counter(user, "Downloads")
                                };

                    Assert.Equal("from Users as user select " +
                                 "counter(user, Likes) as Likes, counter(user, Downloads) as Downloads"
                                , query.ToString());

                    var queryResult = query.ToList();
                    Assert.Equal(3, queryResult.Count);

                    Assert.Null(queryResult[0].Likes);
                    Assert.Equal(100, queryResult[0].Downloads);

                    Assert.Null(queryResult[1].Likes);
                    Assert.Equal(200, queryResult[1].Downloads);

                    Assert.Equal(300, queryResult[2].Likes);
                    Assert.Null(queryResult[2].Downloads);

                }
            }
        }

        [Fact]
        public void SessionQuerySelectCounterViaLet()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Downloads", 200);
                    session.CountersFor("users/3-A").Increment("Likes", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                                let c = RavenQuery.Counter(user, "Downloads")
                                select new
                                {
                                    Name = user.Name,
                                    Downloads = c
                                };

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(user) {
	var c = counter(user, ""Downloads"");
	return { Name : user.Name, Downloads : c };
}
from Users as user select output(user)", query.ToString());

                    var queryResult = query.ToList();
                    Assert.Equal(3, queryResult.Count);

                    Assert.Equal("Jerry", queryResult[0].Name);
                    Assert.Equal(100, queryResult[0].Downloads);

                    Assert.Equal("Bob", queryResult[1].Name);
                    Assert.Equal(200, queryResult[1].Downloads);

                    Assert.Equal("Pigpen", queryResult[2].Name);
                    Assert.Null(queryResult[2].Downloads);

                }
            }
        }

        [Fact]
        public void SessionQuerySelectCounterViaLet_UsingSessionCountersFor()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry" }, "users/1-A");
                    session.Store(new User { Name = "Bob" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Downloads", 200);
                    session.CountersFor("users/3-A").Increment("Likes", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                                let c = session.CountersFor(user).Get("Downloads")
                                select new
                                {
                                    Name = user.Name,
                                    Downloads = c
                                };

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(user) {
	var c = counter(user, ""Downloads"");
	return { Name : user.Name, Downloads : c };
}
from Users as user select output(user)", query.ToString());

                    var queryResult = query.ToList();
                    Assert.Equal(3, queryResult.Count);

                    Assert.Equal("Jerry", queryResult[0].Name);
                    Assert.Equal(100, queryResult[0].Downloads);

                    Assert.Equal("Bob", queryResult[1].Name);
                    Assert.Equal(200, queryResult[1].Downloads);

                    Assert.Equal("Pigpen", queryResult[2].Name);
                    Assert.Null(queryResult[2].Downloads);

                }
            }
        }

        [Fact]
        public void SessionQuerySelectCounterFromLoadedDocJsProjection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jerry", FriendId = "users/2-A" }, "users/1-A");
                    session.Store(new User { Name = "Bob", FriendId = "users/3-A" }, "users/2-A");
                    session.Store(new User { Name = "Pigpen", FriendId = "users/1-A" }, "users/3-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Downloads", 200);
                    session.CountersFor("users/3-A").Increment("Likes", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from user in session.Query<User>()
                                let f = RavenQuery.Load<User>(user.FriendId)
                                select new
                                {
                                    Name = user.Name,
                                    Downloads = RavenQuery.Counter(user, "Downloads"),
                                    FriendsDownloads = RavenQuery.Counter(f, "Downloads")
                                };

                    Assert.Equal("from Users as user " +
                                 "load user.FriendId as f " +
                                 "select { Name : user.Name, " +
                                          "Downloads : counter(user, \"Downloads\"), " +
                                          "FriendsDownloads : counter(f, \"Downloads\") }"
                                , query.ToString());

                    var queryResult = query.ToList();
                    Assert.Equal(3, queryResult.Count);

                    Assert.Equal("Jerry", queryResult[0].Name);
                    Assert.Equal(100, queryResult[0].Downloads);
                    Assert.Equal(200, queryResult[0].FriendsDownloads);

                    Assert.Equal("Bob", queryResult[1].Name);
                    Assert.Equal(200, queryResult[1].Downloads);
                    Assert.Null(queryResult[1].FriendsDownloads);

                    Assert.Equal("Pigpen", queryResult[2].Name);
                    Assert.Null(queryResult[2].Downloads);
                    Assert.Equal(100, queryResult[2].FriendsDownloads);

                }
            }
        }

        [Fact]
        public void SessionQueryIncludeSingleCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "companies/1-A"
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Company = "companies/2-A"
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Company = "companies/3-A"
                    }, "orders/3-A");

                    session.CountersFor("orders/1-A").Increment("Downloads", 100);
                    session.CountersFor("orders/2-A").Increment("Downloads", 200);
                    session.CountersFor("orders/3-A").Increment("Downloads", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Order>()
                        .Include(i => i.IncludeCounter("Downloads"));

                    Assert.Equal("from Orders include counters($p0)"
                                , query.ToString());

                    var queryResult = query.ToList();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    var order = queryResult[0];
                    Assert.Equal("orders/1-A", order.Id);
                    var val = session.CountersFor(order).Get("Downloads");
                    Assert.Equal(100, val);

                    order = queryResult[1];
                    Assert.Equal("orders/2-A", order.Id);
                    val = session.CountersFor(order).Get("Downloads");
                    Assert.Equal(200, val);

                    order = queryResult[2];
                    Assert.Equal("orders/3-A", order.Id);
                    val = session.CountersFor(order).Get("Downloads");
                    Assert.Equal(300, val);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);


                }
            }
        }

        [Fact]
        public void SessionQueryIncludeMultipleCounters()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "companies/1-A"
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Company = "companies/2-A"
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Company = "companies/3-A"
                    }, "orders/3-A");

                    session.CountersFor("orders/1-A").Increment("Downloads", 100);
                    session.CountersFor("orders/2-A").Increment("Downloads", 200);
                    session.CountersFor("orders/3-A").Increment("Downloads", 300);

                    session.CountersFor("orders/1-A").Increment("Likes", 1000);
                    session.CountersFor("orders/2-A").Increment("Likes", 2000);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Order>()
                        .Include(i => i.IncludeCounters(new[] { "Downloads", "Likes" }));

                    Assert.Equal("from Orders include counters($p0)"
                                , query.ToString());

                    var queryResult = query.ToList();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    var order = queryResult[0];
                    Assert.Equal("orders/1-A", order.Id);
                    var val = session.CountersFor(order).Get("Downloads");
                    Assert.Equal(100, val);
                    val = session.CountersFor(order).Get("Likes");
                    Assert.Equal(1000, val);

                    order = queryResult[1];
                    Assert.Equal("orders/2-A", order.Id);
                    val = session.CountersFor(order).Get("Downloads");
                    Assert.Equal(200, val);
                    val = session.CountersFor(order).Get("Likes");
                    Assert.Equal(2000, val);

                    order = queryResult[2];
                    Assert.Equal("orders/3-A", order.Id);
                    val = session.CountersFor(order).Get("Downloads");
                    Assert.Equal(300, val);

                    // should register missing counters
                    val = session.CountersFor(order).Get("Likes");
                    Assert.Null(val);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);


                }
            }
        }

        [Fact]
        public void SessionQueryIncludeAllCounters()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "companies/1-A"
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Company = "companies/2-A"
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Company = "companies/3-A"
                    }, "orders/3-A");

                    session.CountersFor("orders/1-A").Increment("Downloads", 100);
                    session.CountersFor("orders/2-A").Increment("Downloads", 200);
                    session.CountersFor("orders/3-A").Increment("Downloads", 300);

                    session.CountersFor("orders/1-A").Increment("Likes", 1000);
                    session.CountersFor("orders/2-A").Increment("Likes", 2000);

                    session.CountersFor("orders/1-A").Increment("Votes", 10000);
                    session.CountersFor("orders/3-A").Increment("Cats", 5);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Order>()
                        .Include(i => i.IncludeAllCounters());

                    Assert.Equal("from Orders include counters()"
                                , query.ToString());

                    var queryResult = query.ToList();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    var order = queryResult[0];
                    Assert.Equal("orders/1-A", order.Id);
                    var dic = session.CountersFor(order).GetAll();
                    Assert.Equal(3, dic.Count);
                    Assert.Equal(100, dic["Downloads"]);
                    Assert.Equal(1000, dic["Likes"]);
                    Assert.Equal(10000, dic["Votes"]);

                    order = queryResult[1];
                    Assert.Equal("orders/2-A", order.Id);
                    dic = session.CountersFor(order).GetAll();
                    Assert.Equal(2, dic.Count);
                    Assert.Equal(200, dic["Downloads"]);
                    Assert.Equal(2000, dic["Likes"]);

                    order = queryResult[2];
                    Assert.Equal("orders/3-A", order.Id);
                    dic = session.CountersFor(order).GetAll();
                    Assert.Equal(2, dic.Count);
                    Assert.Equal(300, dic["Downloads"]);
                    Assert.Equal(5, dic["Cats"]);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public void SessionQueryIncludeCounterAndDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "companies/1-A"
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Company = "companies/2-A"
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Company = "companies/3-A"
                    }, "orders/3-A");
                    session.Store(new Company
                    {
                        Name = "HR"
                    }, "companies/1-A");
                    session.Store(new Company
                    {
                        Name = "HP"
                    }, "companies/2-A");
                    session.Store(new Company
                    {
                        Name = "Google"
                    }, "companies/3-A");


                    session.CountersFor("orders/1-A").Increment("Downloads", 100);
                    session.CountersFor("orders/2-A").Increment("Downloads", 200);
                    session.CountersFor("orders/3-A").Increment("Downloads", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Order>()
                        .Include(i => i.IncludeCounter("Downloads")
                            .IncludeDocuments(x => x.Company));

                    Assert.Equal("from Orders include " +
                                 "Company,counters($p0)"
                                , query.ToString());

                    var queryResult = query.ToList();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included documents should be in cache
                    session.Load<User>(new[] { "companies/1-A", "companies/2-A", "companies/3-A" });
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    var order = queryResult[0];
                    Assert.Equal("orders/1-A", order.Id);
                    var val = session.CountersFor(order).Get("Downloads");
                    Assert.Equal(100, val);

                    order = queryResult[1];
                    Assert.Equal("orders/2-A", order.Id);
                    val = session.CountersFor(order).Get("Downloads");
                    Assert.Equal(200, val);

                    order = queryResult[2];
                    Assert.Equal("orders/3-A", order.Id);
                    val = session.CountersFor(order).Get("Downloads");
                    Assert.Equal(300, val);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task AsyncSessionQueryIncludeCounterAndDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "companies/1-A"
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Company = "companies/2-A"
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Company = "companies/3-A"
                    }, "orders/3-A");
                    session.Store(new Company
                    {
                        Name = "HR"
                    }, "companies/1-A");
                    session.Store(new Company
                    {
                        Name = "HP"
                    }, "companies/2-A");
                    session.Store(new Company
                    {
                        Name = "Google"
                    }, "companies/3-A");


                    session.CountersFor("orders/1-A").Increment("Downloads", 100);
                    session.CountersFor("orders/2-A").Increment("Downloads", 200);
                    session.CountersFor("orders/3-A").Increment("Downloads", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<Order>()
                        .Include(i => i.IncludeCounter("Downloads")
                            .IncludeDocuments(x => x.Company));

                    Assert.Equal("from Orders include " +
                                 "Company,counters($p0)"
                                , query.ToString());

                    var queryResult = await query.ToListAsync();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included documents should be in cache
                    await session.LoadAsync<User>(new[] { "companies/1-A", "companies/2-A", "companies/3-A" });
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    var order = queryResult[0];
                    Assert.Equal("orders/1-A", order.Id);
                    var val = await session.CountersFor(order).GetAsync("Downloads");
                    Assert.Equal(100, val);

                    order = queryResult[1];
                    Assert.Equal("orders/2-A", order.Id);
                    val = await session.CountersFor(order).GetAsync("Downloads");
                    Assert.Equal(200, val);

                    order = queryResult[2];
                    Assert.Equal("orders/3-A", order.Id);
                    val = await session.CountersFor(order).GetAsync("Downloads");
                    Assert.Equal(300, val);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void SessionQueryIncludeCounterAndDocumentWithWhere()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "companies/1-A",
                        ShipTo = new Address
                        {
                            Country = "Brazil"
                        }
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Company = "companies/2-A",
                        ShipTo = new Address
                        {
                            Country = "Russia"
                        }
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Company = "companies/3-A",
                        ShipTo = new Address
                        {
                            Country = "Brazil"
                        }
                    }, "orders/3-A");
                    session.Store(new Company
                    {
                        Name = "HR"
                    }, "companies/1-A");
                    session.Store(new Company
                    {
                        Name = "HP"
                    }, "companies/2-A");
                    session.Store(new Company
                    {
                        Name = "Google"
                    }, "companies/3-A");


                    session.CountersFor("orders/1-A").Increment("Downloads", 100);
                    session.CountersFor("orders/2-A").Increment("Downloads", 200);
                    session.CountersFor("orders/3-A").Increment("Downloads", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Order>()
                            .Include(i => i.IncludeCounter("Downloads")
                                           .IncludeDocuments(x => x.Company))

                            .Where(x => x.ShipTo.Country == "Brazil");

                    Assert.Equal("from Orders where ShipTo.Country = $p1 " +
                                 "include Company,counters($p0)"
                                , query.ToString());

                    var queryResult = query.ToList();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included documents should be in cache
                    session.Load<User>(new[] { "companies/1-A", "companies/3-A" });
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    var order = queryResult[0];
                    Assert.Equal("orders/1-A", order.Id);
                    var val = session.CountersFor(order).Get("Downloads");
                    Assert.Equal(100, val);

                    order = queryResult[1];
                    Assert.Equal("orders/3-A", order.Id);
                    val = session.CountersFor(order).Get("Downloads");
                    Assert.Equal(300, val);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // this should not be in cache
                    session.Load<User>("companies/2-A");
                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    // this should not be in cache
                    val = session.CountersFor("orders/2-A").Get("Downloads");
                    Assert.Equal(200, val);
                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public async Task AsyncSessionQueryIncludeCounterAndDocumentWithWhere()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "companies/1-A",
                        ShipTo = new Address
                        {
                            Country = "Brazil"
                        }
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Company = "companies/2-A",
                        ShipTo = new Address
                        {
                            Country = "Russia"
                        }
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Company = "companies/3-A",
                        ShipTo = new Address
                        {
                            Country = "Brazil"
                        }
                    }, "orders/3-A");
                    session.Store(new Company
                    {
                        Name = "HR"
                    }, "companies/1-A");
                    session.Store(new Company
                    {
                        Name = "HP"
                    }, "companies/2-A");
                    session.Store(new Company
                    {
                        Name = "Google"
                    }, "companies/3-A");


                    session.CountersFor("orders/1-A").Increment("Downloads", 100);
                    session.CountersFor("orders/2-A").Increment("Downloads", 200);
                    session.CountersFor("orders/3-A").Increment("Downloads", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<Order>()
                            .Include(i => i.IncludeCounter("Downloads")
                                           .IncludeDocuments(x => x.Company))

                            .Where(x => x.ShipTo.Country == "Brazil");

                    Assert.Equal("from Orders where ShipTo.Country = $p1 " +
                                 "include Company,counters($p0)"
                                , query.ToString());

                    var queryResult = await query.ToListAsync();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included documents should be in cache
                    await session.LoadAsync<User>(new[] { "companies/1-A", "companies/3-A" });
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    var order = queryResult[0];
                    Assert.Equal("orders/1-A", order.Id);
                    var val = await session.CountersFor(order).GetAsync("Downloads");
                    Assert.Equal(100, val);

                    order = queryResult[1];
                    Assert.Equal("orders/3-A", order.Id);
                    val = await session.CountersFor(order).GetAsync("Downloads");
                    Assert.Equal(300, val);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // this should not be in cache
                    await session.LoadAsync<User>("companies/2-A");
                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    // this should not be in cache
                    val = await session.CountersFor("orders/2-A").GetAsync("Downloads");
                    Assert.Equal(200, val);
                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public void RawQueryIncludeCounterWithParameter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "companies/1-A"
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Company = "companies/2-A"
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Company = "companies/3-A"
                    }, "orders/3-A");

                    session.CountersFor("orders/1-A").Increment("Downloads", 100);
                    session.CountersFor("orders/2-A").Increment("Downloads", 200);
                    session.CountersFor("orders/3-A").Increment("Downloads", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<Order>("from orders include counters($p0)")
                        .AddParameter("p0", "Downloads")
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    var order = query[0];
                    Assert.Equal("orders/1-A", order.Id);
                    var val = session.CountersFor(order).Get("Downloads");
                    Assert.Equal(100, val);

                    order = query[1];
                    Assert.Equal("orders/2-A", order.Id);
                    val = session.CountersFor(order).Get("Downloads");
                    Assert.Equal(200, val);

                    order = query[2];
                    Assert.Equal("orders/3-A", order.Id);
                    val = session.CountersFor(order).Get("Downloads");
                    Assert.Equal(300, val);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);


                }
            }
        }

        [Fact]
        public void RawQueryIncludeCounterWithArrayParameter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "companies/1-A"
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Company = "companies/2-A"
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Company = "companies/3-A"
                    }, "orders/3-A");

                    session.CountersFor("orders/1-A").Increment("Downloads", 100);
                    session.CountersFor("orders/2-A").Increment("Downloads", 200);
                    session.CountersFor("orders/3-A").Increment("Downloads", 300);
                    session.CountersFor("orders/1-A").Increment("Likes", 1000);
                    session.CountersFor("orders/2-A").Increment("Likes", 2000);
                    session.CountersFor("orders/3-A").Increment("Likes", 3000);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<Order>("from orders include counters($p0)")
                        .AddParameter("p0", new[] { "Downloads", "Likes" })
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    var order = query[0];
                    Assert.Equal("orders/1-A", order.Id);
                    var dic = session.CountersFor(order).Get(new[] { "Downloads", "Likes" });
                    Assert.Equal(100, dic["Downloads"]);
                    Assert.Equal(1000, dic["Likes"]);

                    order = query[1];
                    Assert.Equal("orders/2-A", order.Id);
                    dic = session.CountersFor(order).Get(new[] { "Downloads", "Likes" });
                    Assert.Equal(200, dic["Downloads"]);
                    Assert.Equal(2000, dic["Likes"]);

                    order = query[2];
                    Assert.Equal("orders/3-A", order.Id);
                    dic = session.CountersFor(order).Get(new[] { "Downloads", "Likes" });
                    Assert.Equal(300, dic["Downloads"]);
                    Assert.Equal(3000, dic["Likes"]);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public void RawQueryIncludeCounterOfLoadedDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Employee = "employees/1-A"
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Employee = "employees/2-A"
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Employee = "employees/3-A"
                    }, "orders/3-A");
                    session.Store(new Employee
                    {
                        FirstName = "Aviv"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/2-A");
                    session.Store(new Employee
                    {
                        FirstName = "Bob"
                    }, "employees/3-A");

                    session.CountersFor("employees/1-A").Increment("Downloads", 100);
                    session.CountersFor("employees/2-A").Increment("Downloads", 200);
                    session.CountersFor("employees/3-A").Increment("Downloads", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<Order>("from orders as o load o.Employee as e " +
                                         "include counters(e, 'Downloads')")
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    var val = session.CountersFor("employees/1-A").Get("Downloads");
                    Assert.Equal(100, val);

                    val = session.CountersFor("employees/2-A").Get("Downloads");
                    Assert.Equal(200, val);

                    val = session.CountersFor("employees/3-A").Get("Downloads");
                    Assert.Equal(300, val);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public void RawQueryIncludeAllCountersOfLoadedDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Employee = "employees/1-A"
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Employee = "employees/2-A"
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Employee = "employees/3-A"
                    }, "orders/3-A");
                    session.Store(new Employee
                    {
                        FirstName = "Aviv"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/2-A");
                    session.Store(new Employee
                    {
                        FirstName = "Bob"
                    }, "employees/3-A");

                    session.CountersFor("employees/1-A").Increment("Downloads", 100);
                    session.CountersFor("employees/2-A").Increment("Downloads", 200);
                    session.CountersFor("employees/3-A").Increment("Downloads", 300);
                    session.CountersFor("employees/1-A").Increment("Likes", 1000);
                    session.CountersFor("employees/2-A").Increment("Likes", 2000);
                    session.CountersFor("employees/3-A").Increment("Cats", 5);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<Order>("from orders as o load o.Employee as e " +
                                         "include counters(e)")
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    var dic = session.CountersFor("employees/1-A").GetAll();
                    Assert.Equal(2, dic.Count);
                    Assert.Equal(100, dic["Downloads"]);
                    Assert.Equal(1000, dic["Likes"]);

                    dic = session.CountersFor("employees/2-A").GetAll();
                    Assert.Equal(2, dic.Count);
                    Assert.Equal(200, dic["Downloads"]);
                    Assert.Equal(2000, dic["Likes"]);

                    dic = session.CountersFor("employees/3-A").GetAll();
                    Assert.Equal(2, dic.Count);
                    Assert.Equal(300, dic["Downloads"]);
                    Assert.Equal(5, dic["Cats"]);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public void RawQueryIncludeCountersOfDocumentAndOfLoadedDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Employee = "employees/1-A"
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Employee = "employees/2-A"
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Employee = "employees/3-A"
                    }, "orders/3-A");
                    session.Store(new Employee
                    {
                        FirstName = "Aviv"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/2-A");
                    session.Store(new Employee
                    {
                        FirstName = "Bob"
                    }, "employees/3-A");

                    session.CountersFor("orders/1-A").Increment("Likes", 100);
                    session.CountersFor("orders/2-A").Increment("Likes", 200);
                    session.CountersFor("orders/3-A").Increment("Likes", 300);
                    session.CountersFor("employees/1-A").Increment("Downloads", 1000);
                    session.CountersFor("employees/2-A").Increment("Downloads", 2000);
                    session.CountersFor("employees/3-A").Increment("Downloads", 3000);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<Order>("from orders as o load o.Employee as e " +
                                         "include counters(o, 'Likes'), counters(e, 'Downloads')")
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache

                    var order = query[0];
                    Assert.Equal("orders/1-A", order.Id);
                    var val = session.CountersFor(order).Get("Likes");
                    Assert.Equal(100, val);

                    order = query[1];
                    Assert.Equal("orders/2-A", order.Id);
                    val = session.CountersFor(order).Get("Likes");
                    Assert.Equal(200, val);

                    order = query[2];
                    Assert.Equal("orders/3-A", order.Id);
                    val = session.CountersFor(order).Get("Likes");
                    Assert.Equal(300, val);

                    val = session.CountersFor("employees/1-A").Get("Downloads");
                    Assert.Equal(1000, val);

                    val = session.CountersFor("employees/2-A").Get("Downloads");
                    Assert.Equal(2000, val);

                    val = session.CountersFor("employees/3-A").Get("Downloads");
                    Assert.Equal(3000, val);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public void SessionQueryIncludeCounterOfRelatedDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Employee = "employees/1-A"
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Employee = "employees/2-A"
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Employee = "employees/3-A"
                    }, "orders/3-A");
                    session.Store(new Employee
                    {
                        FirstName = "Aviv"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/2-A");
                    session.Store(new Employee
                    {
                        FirstName = "Bob"
                    }, "employees/3-A");

                    session.CountersFor("employees/1-A").Increment("Downloads", 100);
                    session.CountersFor("employees/2-A").Increment("Downloads", 200);
                    session.CountersFor("employees/3-A").Increment("Downloads", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Order>()
                        .Include(i => i.IncludeCounter(o => o.Employee, "Downloads"));

                    Assert.Equal("from Orders as o " +
                                 "include counters(o.Employee, $p0)"
                        , query.ToString());

                    var results = query.ToList();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    var val = session.CountersFor("employees/1-A").Get("Downloads");
                    Assert.Equal(100, val);

                    val = session.CountersFor("employees/2-A").Get("Downloads");
                    Assert.Equal(200, val);

                    val = session.CountersFor("employees/3-A").Get("Downloads");
                    Assert.Equal(300, val);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public async Task AsyncSessionQueryIncludeCounterOfRelatedDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Employee = "employees/1-A"
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Employee = "employees/2-A"
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Employee = "employees/3-A"
                    }, "orders/3-A");
                    session.Store(new Employee
                    {
                        FirstName = "Aviv"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/2-A");
                    session.Store(new Employee
                    {
                        FirstName = "Bob"
                    }, "employees/3-A");

                    session.CountersFor("employees/1-A").Increment("Downloads", 100);
                    session.CountersFor("employees/2-A").Increment("Downloads", 200);
                    session.CountersFor("employees/3-A").Increment("Downloads", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<Order>()
                        .Include(i => i.IncludeCounter(o => o.Employee, "Downloads"));

                    Assert.Equal("from Orders as o " +
                                 "include counters(o.Employee, $p0)"
                        , query.ToString());

                    var results = await query.ToListAsync();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    var val = await session.CountersFor("employees/1-A").GetAsync("Downloads");
                    Assert.Equal(100, val);

                    val = await session.CountersFor("employees/2-A").GetAsync("Downloads");
                    Assert.Equal(200, val);

                    val = await session.CountersFor("employees/3-A").GetAsync("Downloads");
                    Assert.Equal(300, val);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public void SessionQueryIncludeCountersOfRelatedDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Employee = "employees/1-A"
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Employee = "employees/2-A"
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Employee = "employees/3-A"
                    }, "orders/3-A");
                    session.Store(new Employee
                    {
                        FirstName = "Aviv"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/2-A");
                    session.Store(new Employee
                    {
                        FirstName = "Bob"
                    }, "employees/3-A");

                    session.CountersFor("employees/1-A").Increment("Downloads", 100);
                    session.CountersFor("employees/2-A").Increment("Downloads", 200);
                    session.CountersFor("employees/3-A").Increment("Downloads", 300);

                    session.CountersFor("employees/1-A").Increment("Likes", 1000);
                    session.CountersFor("employees/2-A").Increment("Likes", 2000);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Order>()
                        .Include(i => i.IncludeCounters(o => o.Employee, new[] { "Downloads", "Likes" }));

                    Assert.Equal("from Orders as o " +
                                 "include counters(o.Employee, $p0)"
                        , query.ToString());

                    var results = query.ToList();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    var dic = session.CountersFor("employees/1-A").Get(new[] { "Downloads", "Likes" });
                    Assert.Equal(100, dic["Downloads"]);
                    Assert.Equal(1000, dic["Likes"]);

                    dic = session.CountersFor("employees/2-A").Get(new[] { "Downloads", "Likes" });
                    Assert.Equal(200, dic["Downloads"]);
                    Assert.Equal(2000, dic["Likes"]);

                    dic = session.CountersFor("employees/3-A").Get(new[] { "Downloads", "Likes" });
                    Assert.Equal(300, dic["Downloads"]);
                    Assert.Null(dic["Likes"]);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public void SessionQueryIncludeCountersOfDocumentAndOfRelatedDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Employee = "employees/1-A"
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Employee = "employees/2-A"
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Employee = "employees/3-A"
                    }, "orders/3-A");
                    session.Store(new Employee
                    {
                        FirstName = "Aviv"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/2-A");
                    session.Store(new Employee
                    {
                        FirstName = "Bob"
                    }, "employees/3-A");

                    session.CountersFor("orders/1-A").Increment("Likes", 100);
                    session.CountersFor("orders/2-A").Increment("Likes", 200);
                    session.CountersFor("orders/3-A").Increment("Likes", 300);

                    session.CountersFor("employees/1-A").Increment("Downloads", 1000);
                    session.CountersFor("employees/2-A").Increment("Downloads", 2000);
                    session.CountersFor("employees/3-A").Increment("Downloads", 3000);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Order>()
                        .Include(i => i
                            .IncludeCounter("Likes")
                            .IncludeCounter(x => x.Employee, "Downloads"));

                    Assert.Equal("from Orders as x " +
                                 "include counters(x, $p0),counters(x.Employee, $p1)"
                        , query.ToString());

                    var orders = query.ToList();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    var order = orders[0];
                    Assert.Equal("orders/1-A", order.Id);
                    var val = session.CountersFor(order).Get("Likes");
                    Assert.Equal(100, val);

                    order = orders[1];
                    Assert.Equal("orders/2-A", order.Id);
                    val = session.CountersFor(order).Get("Likes");
                    Assert.Equal(200, val);

                    order = orders[2];
                    Assert.Equal("orders/3-A", order.Id);
                    val = session.CountersFor(order).Get("Likes");
                    Assert.Equal(300, val);

                    val = session.CountersFor("employees/1-A").Get("Downloads");
                    Assert.Equal(1000, val);

                    val = session.CountersFor("employees/2-A").Get("Downloads");
                    Assert.Equal(2000, val);

                    val = session.CountersFor("employees/3-A").Get("Downloads");
                    Assert.Equal(3000, val);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public async Task AsyncSessionQueryIncludeCountersOfDocumentAndOfRelatedDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Employee = "employees/1-A"
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Employee = "employees/2-A"
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Employee = "employees/3-A"
                    }, "orders/3-A");
                    session.Store(new Employee
                    {
                        FirstName = "Aviv"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/2-A");
                    session.Store(new Employee
                    {
                        FirstName = "Bob"
                    }, "employees/3-A");

                    session.CountersFor("orders/1-A").Increment("Likes", 100);
                    session.CountersFor("orders/2-A").Increment("Likes", 200);
                    session.CountersFor("orders/3-A").Increment("Likes", 300);

                    session.CountersFor("employees/1-A").Increment("Downloads", 1000);
                    session.CountersFor("employees/2-A").Increment("Downloads", 2000);
                    session.CountersFor("employees/3-A").Increment("Downloads", 3000);

                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<Order>()
                        .Include(i => i
                            .IncludeCounter("Likes")
                            .IncludeCounter(x => x.Employee, "Downloads"));

                    Assert.Equal("from Orders as x " +
                                 "include counters(x, $p0),counters(x.Employee, $p1)"
                        , query.ToString());

                    var orders = await query.ToListAsync();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    var order = orders[0];
                    Assert.Equal("orders/1-A", order.Id);
                    var val = await session.CountersFor(order).GetAsync("Likes");
                    Assert.Equal(100, val);

                    order = orders[1];
                    Assert.Equal("orders/2-A", order.Id);
                    val = await session.CountersFor(order).GetAsync("Likes");
                    Assert.Equal(200, val);

                    order = orders[2];
                    Assert.Equal("orders/3-A", order.Id);
                    val = await session.CountersFor(order).GetAsync("Likes");
                    Assert.Equal(300, val);

                    val = await session.CountersFor("employees/1-A").GetAsync("Downloads");
                    Assert.Equal(1000, val);

                    val = await session.CountersFor("employees/2-A").GetAsync("Downloads");
                    Assert.Equal(2000, val);

                    val = await session.CountersFor("employees/3-A").GetAsync("Downloads");
                    Assert.Equal(3000, val);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public void SessionQueryIncludeCountersOfDocumentAndOfRelatedDocumentWhere()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Employee = "employees/1-A",
                        OrderedAt = new DateTime(1999, 1, 21)
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Employee = "employees/2-A",
                        OrderedAt = new DateTime(2016, 6, 6)
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Employee = "employees/3-A",
                        OrderedAt = new DateTime(1942, 8, 1)
                    }, "orders/3-A");
                    session.Store(new Employee
                    {
                        FirstName = "Aviv"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/2-A");
                    session.Store(new Employee
                    {
                        FirstName = "Bob"
                    }, "employees/3-A");

                    session.CountersFor("orders/1-A").Increment("Likes", 100);
                    session.CountersFor("orders/2-A").Increment("Likes", 200);
                    session.CountersFor("orders/3-A").Increment("Likes", 300);

                    session.CountersFor("employees/1-A").Increment("Downloads", 1000);
                    session.CountersFor("employees/2-A").Increment("Downloads", 2000);
                    session.CountersFor("employees/3-A").Increment("Downloads", 3000);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Order>()
                        .Include(i => i
                            .IncludeCounter("Likes")
                            .IncludeCounter(x => x.Employee, "Downloads"))
                        .Where(o => o.OrderedAt.Year < 2000);

                    Assert.Equal("from Orders as x where x.OrderedAt.Year < $p2 " +
                                 "include counters(x, $p0),counters(x.Employee, $p1)"
                        , query.ToString());

                    var orders = query.ToList();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    Assert.Equal(2, orders.Count);

                    var order = orders[0];
                    Assert.Equal("orders/1-A", order.Id);

                    var val = session.CountersFor(order).Get("Likes");
                    Assert.Equal(100, val);

                    val = session.CountersFor(order.Employee).Get("Downloads");
                    Assert.Equal(1000, val);

                    order = orders[1];
                    Assert.Equal("orders/3-A", order.Id);

                    val = session.CountersFor(order).Get("Likes");
                    Assert.Equal(300, val);

                    val = session.CountersFor(order.Employee).Get("Downloads");
                    Assert.Equal(3000, val);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public void SessionQueryIncludeCountersWithSelect()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Employee = "employees/1-A"
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Employee = "employees/2-A"
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Employee = "employees/3-A"
                    }, "orders/3-A");
                    session.Store(new Employee
                    {
                        FirstName = "Aviv"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/2-A");
                    session.Store(new Employee
                    {
                        FirstName = "Bob"
                    }, "employees/3-A");

                    session.CountersFor("orders/1-A").Increment("Likes", 100);
                    session.CountersFor("orders/2-A").Increment("Likes", 200);
                    session.CountersFor("orders/3-A").Increment("Likes", 300);

                    session.CountersFor("employees/1-A").Increment("Downloads", 1000);
                    session.CountersFor("employees/2-A").Increment("Downloads", 2000);
                    session.CountersFor("employees/3-A").Increment("Downloads", 3000);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Order>()
                        .Include(i => i
                            .IncludeCounter("Likes")
                            .IncludeCounter(x => x.Employee, "Downloads"))
                        .Select(o => new
                        {
                            o.Id,
                            o.Employee
                        });

                    Assert.Equal("from Orders as x " +
                                 "select id() as Id, Employee " +
                                 "include counters(x, $p0),counters(x.Employee, $p1)"
                        , query.ToString());

                    var results = query.ToList();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    Assert.Equal(3, results.Count);

                    var order = results[0];
                    Assert.Equal("orders/1-A", order.Id);

                    var val = session.CountersFor(order.Id).Get("Likes");
                    Assert.Equal(100, val);
                    val = session.CountersFor(order.Employee).Get("Downloads");
                    Assert.Equal(1000, val);

                    order = results[1];
                    Assert.Equal("orders/2-A", order.Id);

                    val = session.CountersFor(order.Id).Get("Likes");
                    Assert.Equal(200, val);
                    val = session.CountersFor(order.Employee).Get("Downloads");
                    Assert.Equal(2000, val);

                    order = results[2];
                    Assert.Equal("orders/3-A", order.Id);

                    val = session.CountersFor(order.Id).Get("Likes");
                    Assert.Equal(300, val);
                    val = session.CountersFor(order.Employee).Get("Downloads");
                    Assert.Equal(3000, val);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public async Task AsyncSessionQueryIncludeCountersWithSelect()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Employee = "employees/1-A"
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Employee = "employees/2-A"
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Employee = "employees/3-A"
                    }, "orders/3-A");
                    session.Store(new Employee
                    {
                        FirstName = "Aviv"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/2-A");
                    session.Store(new Employee
                    {
                        FirstName = "Bob"
                    }, "employees/3-A");

                    session.CountersFor("orders/1-A").Increment("Likes", 100);
                    session.CountersFor("orders/2-A").Increment("Likes", 200);
                    session.CountersFor("orders/3-A").Increment("Likes", 300);

                    session.CountersFor("employees/1-A").Increment("Downloads", 1000);
                    session.CountersFor("employees/2-A").Increment("Downloads", 2000);
                    session.CountersFor("employees/3-A").Increment("Downloads", 3000);

                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<Order>()
                        .Include(i => i
                            .IncludeCounter("Likes")
                            .IncludeCounter(x => x.Employee, "Downloads"))
                        .Select(o => new
                        {
                            o.Id,
                            o.Employee
                        });

                    Assert.Equal("from Orders as x " +
                                 "select id() as Id, Employee " +
                                 "include counters(x, $p0),counters(x.Employee, $p1)"
                        , query.ToString());

                    var results = await query.ToListAsync();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    Assert.Equal(3, results.Count);

                    var order = results[0];
                    Assert.Equal("orders/1-A", order.Id);

                    var val = await session.CountersFor(order.Id).GetAsync("Likes");
                    Assert.Equal(100, val);
                    val = await session.CountersFor(order.Employee).GetAsync("Downloads");
                    Assert.Equal(1000, val);

                    order = results[1];
                    Assert.Equal("orders/2-A", order.Id);

                    val = await session.CountersFor(order.Id).GetAsync("Likes");
                    Assert.Equal(200, val);
                    val = await session.CountersFor(order.Employee).GetAsync("Downloads");
                    Assert.Equal(2000, val);

                    order = results[2];
                    Assert.Equal("orders/3-A", order.Id);

                    val = await session.CountersFor(order.Id).GetAsync("Likes");
                    Assert.Equal(300, val);
                    val = await session.CountersFor(order.Employee).GetAsync("Downloads");
                    Assert.Equal(3000, val);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public void SessionQueryIncludeCountersUsingFromAliasWithSelectAndWhere()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Employee = "employees/1-A",
                        OrderedAt = new DateTime(1999, 1, 21)
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Employee = "employees/2-A",
                        OrderedAt = new DateTime(2016, 6, 6)
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Employee = "employees/3-A",
                        OrderedAt = new DateTime(1942, 8, 1)
                    }, "orders/3-A");
                    session.Store(new Employee
                    {
                        FirstName = "Aviv"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/2-A");
                    session.Store(new Employee
                    {
                        FirstName = "Bob"
                    }, "employees/3-A");

                    session.CountersFor("orders/1-A").Increment("Likes", 100);
                    session.CountersFor("orders/2-A").Increment("Likes", 200);
                    session.CountersFor("orders/3-A").Increment("Likes", 300);

                    session.CountersFor("employees/1-A").Increment("Downloads", 1000);
                    session.CountersFor("employees/2-A").Increment("Downloads", 2000);
                    session.CountersFor("employees/3-A").Increment("Downloads", 3000);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from o in session.Query<Order>()
                                    .Include(i => i
                                        .IncludeCounter("Likes")
                                        .IncludeCounter(x => x.Employee, "Downloads")
                                        .IncludeDocuments("Employee"))
                                where o.OrderedAt.Year < 2000
                                select new
                                {
                                    o.Id,
                                    o.OrderedAt,
                                    o.Employee,

                                    //this will create js projection with from-alias 'o'
                                    Foo = o.Employee + o.Company
                                };

                    Assert.Equal("from Orders as o " +
                                 "where o.OrderedAt.Year < $p2 " +
                                 "select { Id : id(o), OrderedAt : o.OrderedAt, " +
                                    "Employee : o.Employee, Foo : o.Employee+o.Company } " +
                                 "include Employee,counters(o, $p0),counters(o.Employee, $p1)"
                        , query.ToString());

                    var results = query.ToList();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included documents should be in cache
                    session.Load<Employee>(new[] { "employees/1-A", "employees/3-A" });
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    Assert.Equal(2, results.Count);

                    var order = results[0];
                    Assert.Equal("orders/1-A", order.Id);
                    Assert.Equal(new DateTime(1999, 1, 21), order.OrderedAt);

                    var val = session.CountersFor(order.Id).Get("Likes");
                    Assert.Equal(100, val);

                    val = session.CountersFor(order.Employee).Get("Downloads");
                    Assert.Equal(1000, val);

                    order = results[1];
                    Assert.Equal("orders/3-A", order.Id);
                    Assert.Equal(new DateTime(1942, 8, 1), order.OrderedAt);

                    val = session.CountersFor(order.Id).Get("Likes");
                    Assert.Equal(300, val);

                    val = session.CountersFor(order.Employee).Get("Downloads");
                    Assert.Equal(3000, val);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public async Task AsyncSessionQueryIncludeCountersUsingFromAliasWithSelectAndWhere()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Employee = "employees/1-A",
                        OrderedAt = new DateTime(1999, 1, 21)
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Employee = "employees/2-A",
                        OrderedAt = new DateTime(2016, 6, 6)
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Employee = "employees/3-A",
                        OrderedAt = new DateTime(1942, 8, 1)
                    }, "orders/3-A");
                    session.Store(new Employee
                    {
                        FirstName = "Aviv"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/2-A");
                    session.Store(new Employee
                    {
                        FirstName = "Bob"
                    }, "employees/3-A");

                    session.CountersFor("orders/1-A").Increment("Likes", 100);
                    session.CountersFor("orders/2-A").Increment("Likes", 200);
                    session.CountersFor("orders/3-A").Increment("Likes", 300);

                    session.CountersFor("employees/1-A").Increment("Downloads", 1000);
                    session.CountersFor("employees/2-A").Increment("Downloads", 2000);
                    session.CountersFor("employees/3-A").Increment("Downloads", 3000);

                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = from o in session.Query<Order>()
                                    .Include(i => i
                                        .IncludeCounter("Likes")
                                        .IncludeCounter(x => x.Employee, "Downloads")
                                        .IncludeDocuments("Employee"))
                                where o.OrderedAt.Year < 2000
                                select new
                                {
                                    o.Id,
                                    o.OrderedAt,
                                    o.Employee,

                                    //this will create js projection with from-alias 'o'
                                    Foo = o.Employee + o.Company
                                };

                    Assert.Equal("from Orders as o " +
                                 "where o.OrderedAt.Year < $p2 " +
                                 "select { Id : id(o), OrderedAt : o.OrderedAt, " +
                                    "Employee : o.Employee, Foo : o.Employee+o.Company } " +
                                 "include Employee,counters(o, $p0),counters(o.Employee, $p1)"
                        , query.ToString());

                    var results = await query.ToListAsync();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included documents should be in cache
                    await session.LoadAsync<Employee>(new[] { "employees/1-A", "employees/3-A" });
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    Assert.Equal(2, results.Count);

                    var order = results[0];
                    Assert.Equal("orders/1-A", order.Id);
                    Assert.Equal(new DateTime(1999, 1, 21), order.OrderedAt);

                    var val = await session.CountersFor(order.Id).GetAsync("Likes");
                    Assert.Equal(100, val);

                    val = await session.CountersFor(order.Employee).GetAsync("Downloads");
                    Assert.Equal(1000, val);

                    order = results[1];
                    Assert.Equal("orders/3-A", order.Id);
                    Assert.Equal(new DateTime(1942, 8, 1), order.OrderedAt);

                    val = await session.CountersFor(order.Id).GetAsync("Likes");
                    Assert.Equal(300, val);

                    val = await session.CountersFor(order.Employee).GetAsync("Downloads");
                    Assert.Equal(3000, val);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public void SessionQueryIncludeAllCountersOfDocumentAndOfRelatedDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Employee = "employees/1-A"
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Employee = "employees/2-A"
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Employee = "employees/3-A"
                    }, "orders/3-A");
                    session.Store(new Employee
                    {
                        FirstName = "Aviv"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/2-A");
                    session.Store(new Employee
                    {
                        FirstName = "Bob"
                    }, "employees/3-A");

                    session.CountersFor("orders/1-A").Increment("Likes", 100);
                    session.CountersFor("orders/2-A").Increment("Likes", 200);
                    session.CountersFor("orders/3-A").Increment("Likes", 300);
                    session.CountersFor("orders/1-A").Increment("Downloads", 1000);
                    session.CountersFor("orders/2-A").Increment("Downloads", 2000);

                    session.CountersFor("employees/1-A").Increment("Likes", 100);
                    session.CountersFor("employees/2-A").Increment("Likes", 200);
                    session.CountersFor("employees/3-A").Increment("Likes", 300);
                    session.CountersFor("employees/1-A").Increment("Downloads", 1000);
                    session.CountersFor("employees/2-A").Increment("Downloads", 2000);
                    session.CountersFor("employees/3-A").Increment("Cats", 5);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Order>()
                        .Include(i => i
                            .IncludeAllCounters()
                            .IncludeAllCounters(x => x.Employee));

                    Assert.Equal("from Orders as x " +
                                 "include counters(x),counters(x.Employee)"
                        , query.ToString());

                    var orders = query.ToList();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    var order = orders[0];
                    Assert.Equal("orders/1-A", order.Id);
                    var dic = session.CountersFor(order).GetAll();
                    Assert.Equal(2, dic.Count);
                    Assert.Equal(100, dic["Likes"]);
                    Assert.Equal(1000, dic["Downloads"]);

                    order = orders[1];
                    Assert.Equal("orders/2-A", order.Id);
                    dic = session.CountersFor(order).GetAll();
                    Assert.Equal(2, dic.Count);
                    Assert.Equal(200, dic["Likes"]);
                    Assert.Equal(2000, dic["Downloads"]);

                    order = orders[2];
                    Assert.Equal("orders/3-A", order.Id);
                    dic = session.CountersFor(order).GetAll();
                    Assert.Equal(1, dic.Count);
                    Assert.Equal(300, dic["Likes"]);

                    dic = session.CountersFor("employees/1-A").GetAll();
                    Assert.Equal(2, dic.Count);
                    Assert.Equal(100, dic["Likes"]);
                    Assert.Equal(1000, dic["Downloads"]);

                    dic = session.CountersFor("employees/2-A").GetAll();
                    Assert.Equal(2, dic.Count);
                    Assert.Equal(200, dic["Likes"]);
                    Assert.Equal(2000, dic["Downloads"]);

                    dic = session.CountersFor("employees/3-A").GetAll();
                    Assert.Equal(2, dic.Count);
                    Assert.Equal(300, dic["Likes"]);
                    Assert.Equal(5, dic["Cats"]);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public void SessionQueryIncludeDocumentAndCountersOfDocumentAndOfRelatedDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Employee = "employees/1-A"
                    }, "orders/1-A");
                    session.Store(new Order
                    {
                        Employee = "employees/2-A"
                    }, "orders/2-A");
                    session.Store(new Order
                    {
                        Employee = "employees/3-A"
                    }, "orders/3-A");
                    session.Store(new Employee
                    {
                        FirstName = "Aviv"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/2-A");
                    session.Store(new Employee
                    {
                        FirstName = "Bob"
                    }, "employees/3-A");

                    session.CountersFor("orders/1-A").Increment("Likes", 100);
                    session.CountersFor("orders/2-A").Increment("Likes", 200);
                    session.CountersFor("orders/3-A").Increment("Likes", 300);

                    session.CountersFor("employees/1-A").Increment("Downloads", 1000);
                    session.CountersFor("employees/2-A").Increment("Downloads", 2000);
                    session.CountersFor("employees/3-A").Increment("Downloads", 3000);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Order>()
                        .Include(i => i
                            .IncludeDocuments(o => o.Employee)
                            .IncludeCounter("Likes")
                            .IncludeCounter(x => x.Employee, "Downloads"));

                    Assert.Equal("from Orders as x " +
                                 "include Employee,counters(x, $p0),counters(x.Employee, $p1)"
                        , query.ToString());

                    var orders = query.ToList();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included documents should be in cache
                    var employees = session.Load<Employee>(
                        new[] { "employees/1-A", "employees/2-A", "employees/3-A" });

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    // included counters should be in cache
                    var order = orders[0];
                    Assert.Equal("orders/1-A", order.Id);
                    var val = session.CountersFor(order).Get("Likes");
                    Assert.Equal(100, val);

                    order = orders[1];
                    Assert.Equal("orders/2-A", order.Id);
                    val = session.CountersFor(order).Get("Likes");
                    Assert.Equal(200, val);

                    order = orders[2];
                    Assert.Equal("orders/3-A", order.Id);
                    val = session.CountersFor(order).Get("Likes");
                    Assert.Equal(300, val);

                    var employee = employees["employees/1-A"];
                    val = session.CountersFor(employee).Get("Downloads");
                    Assert.Equal(1000, val);

                    employee = employees["employees/2-A"];
                    val = session.CountersFor(employee).Get("Downloads");
                    Assert.Equal(2000, val);

                    employee = employees["employees/3-A"];
                    val = session.CountersFor(employee).Get("Downloads");
                    Assert.Equal(3000, val);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        private class User
        {
            public string Name { get; set; }
            public int Age { get; set; }
            public string FriendId { get; set; }
        }

        private class CounterResult
        {
            public long? Downloads { get; set; }
            public long? Likes { get; set; }
            public string Name { get; set; }
        }

        private class CounterResult2
        {
            public long? DownloadsCount { get; set; }
            public long? LikesCount { get; set; }
        }

        private class CounterResult3
        {
            public Dictionary<string, long> Downloads { get; set; }
        }

        private class CounterResult4
        {
            public long? Downloads { get; set; }
            public string Name { get; set; }
            public Dictionary<string, long> Likes { get; set; }
        }

        private class CounterResult5
        {
            public Dictionary<string, long> Counters { get; set; }
        }

        private class CounterResult6
        {
            public long? Counter { get; set; }
        }

        private class CounterResult7
        {
            public long? Downloads { get; set; }
            public long? FriendsDownloads { get; set; }
            public string Name { get; set; }
        }
    }
}
