using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13432 : RavenTestBase
    {
        public RavenDB_13432(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CountersInSelectClauseShouldAffectQueryEtag_CollectionQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.CountersFor("users/1").Increment("Downloads", 100);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var counterValue = session.Query<User>().Select(u => RavenQuery.Counter(u, "Downloads")).First();
                    Assert.Equal(100, counterValue);
                }

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1").Increment("Downloads", 50);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // should not be served from cache 
                    var counterValue = session.Query<User>().Select(u => RavenQuery.Counter(u, "Downloads")).First();
                    Assert.Equal(150, counterValue);
                }
            }
        }

        [Fact]
        public void CountersInSelectClauseShouldAffectQueryEtag_JsProjection_CollectionQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.CountersFor("users/1").Increment("Downloads", 100);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>().Select(u => new
                    {
                        Name = u.Name + " "+ u.LastName, // creates JS projection
                        Counter = RavenQuery.Counter(u, "Downloads")
                    }).First();

                    Assert.Equal(100, query.Counter);
                }

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1").Increment("Downloads", 50);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // should not be served from cache 
                    var query = session.Query<User>().Select(u => new
                    {
                        Name = u.Name + " " + u.LastName, // creates JS projection
                        Counter = RavenQuery.Counter(u, "Downloads")
                    }).First();

                    Assert.Equal(150, query.Counter);
                }
            }
        }

        [Fact]
        public void CountersInSelectClauseShouldAffectQueryEtag_DynamicIndexQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "ayende"
                    }, "users/1");
                    session.CountersFor("users/1").Increment("Downloads", 100);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var counterValue = session.Query<User>()
                        .Where(u => u.Name == "ayende")
                        .Select(u => RavenQuery.Counter(u, "Downloads"))
                        .First();

                    Assert.Equal(100, counterValue);
                }

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1").Increment("Downloads", 50);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // should not be served from cache 
                    var counterValue = session.Query<User>()
                        .Where(u => u.Name == "ayende")
                        .Select(u => RavenQuery.Counter(u, "Downloads"))
                        .First();

                    Assert.Equal(150, counterValue);
                }
            }
        }

        [Fact]
        public void CountersInSelectClauseShouldAffectQueryEtag_JsProjection_DynamicIndexQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "ayende"
                    }, "users/1");
                    session.CountersFor("users/1").Increment("Downloads", 100);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Where(u => u.Name == "ayende")
                        .Select(u => new
                        {
                            Name = u.Name + " " + u.LastName, // creates JS projection
                            Counter = RavenQuery.Counter(u, "Downloads")
                        })
                        .First();

                    Assert.Equal(100, query.Counter);
                }

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1").Increment("Downloads", 50);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // should not be served from cache 
                    var query = session.Query<User>()
                        .Where(u => u.Name == "ayende")
                        .Select(u => new
                        {
                            Name = u.Name + " " + u.LastName, // creates JS projection
                            Counter = RavenQuery.Counter(u, "Downloads")
                        })
                        .First();

                    Assert.Equal(150, query.Counter);
                }
            }
        }

        [Fact]
        public void CountersInSelectClauseShouldAffectQueryEtag_StaticIndexQuery()
        {
            using (var store = GetDocumentStore())
            {
                new UsersByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "ayende"
                    }, "users/1");
                    session.CountersFor("users/1").Increment("Downloads", 100);
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var counterValue = session.Query<User, UsersByName>()
                        .Where(u => u.Name == "ayende")
                        .Select(u => RavenQuery.Counter(u, "Downloads"))
                        .First();

                    Assert.Equal(100, counterValue);
                }

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1").Increment("Downloads", 50);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // should not be served from cache 
                    var counterValue = session.Query<User, UsersByName>()
                        .Where(u => u.Name == "ayende")
                        .Select(u => RavenQuery.Counter(u, "Downloads"))
                        .First();

                    Assert.Equal(150, counterValue);
                }
            }
        }

        [Fact]
        public void CountersInSelectClauseShouldAffectQueryEtag_JsProjection_StaticIndexQuery()
        {
            using (var store = GetDocumentStore())
            {
                new UsersByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "ayende"
                    }, "users/1");
                    session.CountersFor("users/1").Increment("Downloads", 100);
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User, UsersByName>()
                        .Where(u => u.Name == "ayende")
                        .Select(u => new
                        {
                            Name = u.Name + " " + u.LastName, // creates JS projection
                            Counter = RavenQuery.Counter(u, "Downloads")
                        })
                        .First();

                    Assert.Equal(100, query.Counter);
                }

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1").Increment("Downloads", 50);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // should not be served from cache 
                    var query = session.Query<User, UsersByName>()
                        .Where(u => u.Name == "ayende")
                        .Select(u => new
                        {
                            Name = u.Name + " " + u.LastName, // creates JS projection
                            Counter = RavenQuery.Counter(u, "Downloads")
                        })
                        .First();

                    Assert.Equal(150, query.Counter);
                }
            }
        }

        private class UsersByName : AbstractIndexCreationTask<User>
        {
            public UsersByName()
            {
                Map = users =>  from user in users
                                select new
                                {
                                    user.Name
                                };
            }
        }
    }
}
