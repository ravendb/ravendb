using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Server.ServerWide.Context;
using Xunit;
using User = Raven.Tests.Core.Utils.Entities.User;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14034 : RavenTestBase
    {
        public RavenDB_14034(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CmpxchgInWhereShouldAffectQueryEtag_DynamicIndexQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Ayende"
                    });

                    session.SaveChanges();
                }

                // put compare exchange value

                var key = "names/oren";

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(key, "Ayende");

                    session.SaveChanges();
                }

                // query based on compare exchange value

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<User>("from Users Where Name == cmpxchg('names/oren')");


                    var result = query.First();
                    Assert.Equal("Ayende", result.Name);

                }

                // update the compare exchange value

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var cmpxchg = session.Advanced.ClusterTransaction.GetCompareExchangeValue<string>(key);
                    cmpxchg.Value = "Rahien";

                    session.SaveChanges();
                }

                // verify that the compare exchange value is updated 

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var cmpxchg = session.Advanced.ClusterTransaction.GetCompareExchangeValue<string>(key);

                    Assert.Equal("Rahien", cmpxchg.Value);

                }

                // re run the query
                // the query result should not be served from cache 

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<User>("from Users Where Name == cmpxchg('names/oren')");

                    var result = query.FirstOrDefault();
                    Assert.Null(result);
                }
            }
        }

        [Fact]
        public void CmpxchgInWhereShouldAffectQueryEtag_StaticIndexQuery()
        {
            using (var store = GetDocumentStore())
            {
                new UsersByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Ayende"
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                // put compare exchange value

                var key = "names/oren";

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(key, "Ayende");

                    session.SaveChanges();
                }

                // query based on compare exchange value

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User, UsersByName>()
                        .Where(u => u.Name == RavenQuery.CmpXchg<string>(key));

                    var result = query.First();
                    Assert.Equal("Ayende", result.Name);

                }

                // update the compare exchange value

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var cmpxchg = session.Advanced.ClusterTransaction.GetCompareExchangeValue<string>(key);
                    cmpxchg.Value = "Rahien";

                    session.SaveChanges();
                }

                // verify that the compare exchange value is updated 

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var cmpxchg = session.Advanced.ClusterTransaction.GetCompareExchangeValue<string>(key);

                    Assert.Equal("Rahien", cmpxchg.Value);

                }

                // re run the query
                // the query result should not be served from cache 

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User, UsersByName>()
                        .Where(u => u.Name == RavenQuery.CmpXchg<string>(key));

                    var result = query.FirstOrDefault();
                    Assert.Null(result);
                }
            }
        }

        [Fact]
        public void CmpxchgInSelectShouldAffectQueryEtag_IndexQuery()
        {
            using (var store = GetDocumentStore())
            {
                var key = "names/ayende";

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = key,
                        Age = 35
                    });

                    session.SaveChanges();
                }

                // put compare exchange value


                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(key, "Oren");

                    session.SaveChanges();
                }

                // query based on compare exchange value

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                                .Where(u => u.Age > 18)
                                .Select(u => new
                                {
                                    CmpXngValue = RavenQuery.CmpXchg<string>(u.Name)
                                });

                    var result = query.First();
                    Assert.Equal("Oren", result.CmpXngValue);

                }

                // update the compare exchange value

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var cmpxchg = session.Advanced.ClusterTransaction.GetCompareExchangeValue<string>(key);
                    cmpxchg.Value = "Rahien";

                    session.SaveChanges();
                }

                // verify that the compare exchange value is updated 

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var cmpxchg = session.Advanced.ClusterTransaction.GetCompareExchangeValue<string>(key);

                    Assert.Equal("Rahien", cmpxchg.Value);

                }

                // re run the query
                // the query result should not be served from cache 

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Where(u => u.Age > 18)
                        .Select(u => new
                        {
                            CmpXngValue = RavenQuery.CmpXchg<string>(u.Name)
                        });

                    var result = query.First();
                    Assert.Equal("Rahien", result.CmpXngValue);
                }
            }
        }

        [Fact]
        public void CmpxchgInSelectShouldAffectQueryEtag_CollectionQuery()
        {
            using (var store = GetDocumentStore())
            {
                var key = "names/ayende";

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = key
                    });

                    session.SaveChanges();
                }

                // put compare exchange value


                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(key, "Oren");

                    session.SaveChanges();
                }

                // query based on compare exchange value

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Select(u => new
                        {
                            CmpXngValue = RavenQuery.CmpXchg<string>(u.Name)
                        });

                    var result = query.First();
                    Assert.Equal("Oren", result.CmpXngValue);

                }

                // update the compare exchange value

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var cmpxchg = session.Advanced.ClusterTransaction.GetCompareExchangeValue<string>(key);
                    cmpxchg.Value = "Rahien";

                    session.SaveChanges();
                }

                // verify that the compare exchange value is updated 

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var cmpxchg = session.Advanced.ClusterTransaction.GetCompareExchangeValue<string>(key);

                    Assert.Equal("Rahien", cmpxchg.Value);

                }

                // re run the query
                // the query result should not be served from cache 

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                                .Select(u => new
                                {
                                    CmpXngValue = RavenQuery.CmpXchg<string>(u.Name)
                                });
                    var result = query.First();
                    Assert.Equal("Rahien", result.CmpXngValue);
                }
            }
        }

        [Fact]
        public void ShouldConsiderBothCountersAndCmpXchgInQueryEtagComputation()
        {
            using (var store = GetDocumentStore())
            {
                var key = "names/ayende";

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = key
                    }, "users/1");

                    session.CountersFor("users/1").Increment("downloads", 100);

                    session.SaveChanges();
                }

                // put compare exchange value


                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(key, "Oren");

                    session.SaveChanges();
                }

                // query based on compare exchange value

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Select(u => new
                        {
                            CmpXngValue = RavenQuery.CmpXchg<string>(u.Name),
                            CounterValue = RavenQuery.Counter(u, "downloads")
                        });

                    var result = query.First();
                    Assert.Equal("Oren", result.CmpXngValue);
                    Assert.Equal(100, result.CounterValue);
                }

                // update the compare exchange value

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var cmpxchg = session.Advanced.ClusterTransaction.GetCompareExchangeValue<string>(key);
                    cmpxchg.Value = "Rahien";

                    session.SaveChanges();
                }

                // verify that the compare exchange value is updated 

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var cmpxchg = session.Advanced.ClusterTransaction.GetCompareExchangeValue<string>(key);

                    Assert.Equal("Rahien", cmpxchg.Value);

                }

                // re run the query
                // the query result should not be served from cache 

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                                .Select(u => new
                                {
                                    CmpXngValue = RavenQuery.CmpXchg<string>(u.Name),
                                    CounterValue = RavenQuery.Counter(u, "downloads")
                                });

                    var result = query.First();
                    Assert.Equal("Rahien", result.CmpXngValue);
                    Assert.Equal(100, result.CounterValue);
                }

                // increment counter value

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1").Increment("downloads", 100);
                    session.SaveChanges();
                }

                // re run the query
                // the query result should not be served from cache 

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                                .Select(u => new
                                {
                                    CmpXngValue = RavenQuery.CmpXchg<string>(u.Name),
                                    CounterValue = RavenQuery.Counter(u, "downloads")
                                });

                    var result = query.First();
                    Assert.Equal("Rahien", result.CmpXngValue);
                    Assert.Equal(200, result.CounterValue);
                }
            }
        }

        [Fact]
        public async Task CanGetLastCmpXchgIndexForDatabase()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var key = "names/ayende/";
                var keys = new string[10];

                // put compare exchange values on both databases

                foreach (var store in new[] { store1, store2 })
                {
                    for (int i = 0; i < 10; i++)
                    {
                        using (var session = store.OpenSession(new SessionOptions
                        {
                            TransactionMode = TransactionMode.ClusterWide
                        }))
                        {
                            var current = key + i;
                            keys[i] = current;

                            session.Advanced.ClusterTransaction.CreateCompareExchangeValue(current, "Oren/" + i);

                            session.SaveChanges();
                        }
                    }
                }

                // update one compare exchange value from each database

                foreach (var store in new[] { store1, store2 })
                {
                    using (var session = store.OpenSession(new SessionOptions
                    {
                        TransactionMode = TransactionMode.ClusterWide
                    }))
                    {
                        var cmpxchg = session.Advanced.ClusterTransaction.GetCompareExchangeValue<string>(keys[5]);
                        cmpxchg.Value = "Rahien";

                        session.SaveChanges();
                    }
                }

                // verify that GetLastCompareExchangeIndexForDatabase() returns the correct result 

                foreach (var store in new[] { store1, store2 })
                {
                    using (var session = store.OpenSession(new SessionOptions
                    {
                        TransactionMode = TransactionMode.ClusterWide
                    }))
                    {
                        var expectedIndex = session.Advanced.ClusterTransaction
                            .GetCompareExchangeValues<string>(keys)
                            .Max(cmpxchg => cmpxchg.Value.Index);

                        var documentDatabase = await Databases.GetDocumentDatabaseInstanceFor(store);
                        using (documentDatabase.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionContext))
                        using (transactionContext.OpenReadTransaction())
                        {
                            var lastCmpXchgIndex = documentDatabase.ServerStore.Cluster
                                .GetLastCompareExchangeIndexForDatabase(transactionContext, store.Database);

                            Assert.Equal(expectedIndex, lastCmpXchgIndex);
                        }
                    }
                }
            }
        }

        private class UsersByName : AbstractIndexCreationTask<Core.Utils.Entities.User>
        {
            public UsersByName()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Name
                               };
            }
        }

    }
}
