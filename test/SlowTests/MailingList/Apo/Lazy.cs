using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList.Apo
{
    public class Lazy : RavenTestBase
    {
        public Lazy(ITestOutputHelper output) : base(output)
        {
        }

        private class TestClass
        {
            public string Id { get; set; }

            public string Value { get; set; }

            public DateTime Date { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void LazyWhereAndOrderBy(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestClass() { Id = "testid", Value = "test1", Date = DateTime.UtcNow });
                    session.Store(new TestClass() { Value = "test2", Date = DateTime.UtcNow });
                    session.Store(new TestClass() { Value = "test3", Date = DateTime.UtcNow.AddMinutes(1) });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var hello = new List<TestClass>();

                    // should not throw
                    session.Query<TestClass>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Where(x => x.Date >= DateTime.UtcNow.AddMinutes(-1))
                            .OrderByDescending(x => x.Date)
                            .Lazily(result =>
                            {
                                hello = result.ToList();
                            });
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task LazyQuery_WhenDefineCallBack_ShouldExecuteAsItIsInRegularQuery(Options options)
        {
            var regularQuery = new List<string>();
            var lazyQuery = new List<string>();
            var asyncLazyQuery = new List<string>();
            List<string> current = null;

            using var store = GetDocumentStore(options);
            store.OnBeforeQuery += (s, e) =>
            {
                current.Add("SessionOnBeforeQuery");
                e.QueryCustomization.AfterQueryExecuted(_ => current.Add("AfterQueryExecuted"));
            };
                
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new TestClass { Id = "testid", Value = "test1", Date = DateTime.UtcNow });
                await session.SaveChangesAsync();
            }

            current = regularQuery;
            using (var session = store.OpenAsyncSession())
            {
                _ = await Customize(session.Query<TestClass>()).ToArrayAsync();
            }
                
            current = lazyQuery;
            using (var session = store.OpenSession())
            {
                _ = Customize(session.Query<TestClass>()).Lazily().Value.ToArray();
            }
            
            current = asyncLazyQuery;
            using (var session = store.OpenAsyncSession())
            {
                _ = await Customize(session.Query<TestClass>()).LazilyAsync().Value;
            }

            Assert.Equal(regularQuery, lazyQuery);
            Assert.Equal(regularQuery, asyncLazyQuery);
                
            IRavenQueryable<TestClass> Customize(IRavenQueryable<TestClass> query)
            {
                return query.Customize(x => x.BeforeQueryExecuted(_ => current.Add("QueryOnBeforeQuery")));
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CountLazily_WhenDefineCallBack_ShouldExecuteAsItIsInRegularQuery(Options options)
        {
            var regularQuery = new List<string>();
            var lazyQuery = new List<string>();
            var asyncLazyQuery = new List<string>();
            List<string> current = null;

            using var store = GetDocumentStore(options);
            store.OnBeforeQuery += (s, e) =>
            {
                current.Add("SessionOnBeforeQuery");
                e.QueryCustomization.AfterQueryExecuted(_ => current.Add("AfterQueryExecuted"));
            };
                
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new TestClass { Id = "testid", Value = "test1", Date = DateTime.UtcNow });
                await session.SaveChangesAsync();
            }

            current = regularQuery;
            using (var session = store.OpenAsyncSession())
            {
                _ = await Customize(session.Query<TestClass>()).CountAsync();
            }
                
            current = lazyQuery;
            using (var session = store.OpenSession())
            {
                _ = Customize(session.Query<TestClass>()).CountLazily().Value;
            }
            
            current = asyncLazyQuery;
            using (var session = store.OpenAsyncSession())
            {
                _ = await Customize(session.Query<TestClass>()).CountLazilyAsync().Value;
            }

            Assert.Equal(regularQuery, lazyQuery);
            Assert.Equal(regularQuery, asyncLazyQuery);
                
            IRavenQueryable<TestClass> Customize(IRavenQueryable<TestClass> query)
            {
                return query.Customize(x => x.BeforeQueryExecuted(_ => current.Add("QueryOnBeforeQuery")));
            }
        }
    }
}
