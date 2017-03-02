using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Bugs
{
    public class AggressiveCaching : RavenTestBase
    {
        public DocumentStore InitAggressiveCaching()
        {
            var store = GetDocumentStore();
            store.DisableAggressiveCaching();
            
            using (var session = store.OpenSession())
            {
                session.Store(new User());
                session.SaveChanges();
            }
            Server.Metrics.Reset();
            return store;
        }

        [Fact(Skip = "Wait for http://issues.hibernatingrhinos.com/issue/RavenDB-6244")]
        public void CanAggressivelyCacheLoads()
        {     
            using (var store = InitAggressiveCaching())
            {
                for (var i = 0; i < 5; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
                        {
                            session.Load<User>("users/1");
                        }
                    }
                }
                Assert.Equal(1, Server.Metrics.RequestsMeter.Count);
            }
        }

        [Fact(Skip = "Wait for http://issues.hibernatingrhinos.com/issue/RavenDB-6244")]
        public async Task CanAggressivelyCacheLoads_Async()
        {
            using (var store = InitAggressiveCaching())
            {
                for (var i = 0; i < 5; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
                        {
                            await session.LoadAsync<User>("users/1");
                        }
                    }
                }
                Assert.Equal(1, Server.Metrics.RequestsMeter.Count);
            }
        }


        [Fact(Skip = "Wait for http://issues.hibernatingrhinos.com/issue/RavenDB-6244")]
        public void CanAggressivelyCacheQueries()
        {
            using (var store = InitAggressiveCaching())
            {
                for (var i = 0; i < 5; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
                        {
                            session.Query<User>().ToList();
                        }
                    }
                }
                Assert.Equal(1, Server.Metrics.RequestsMeter.Count);
            }           
        }

        // TODO: NOTE: I think this test is not complete, since the assertion here is exactly the same as in CanAggressivelyCacheQueries.
        [Fact(Skip = "Wait for http://issues.hibernatingrhinos.com/issue/RavenDB-6244")]
        public void WaitForNonStaleResultsIgnoresAggressiveCaching()
        {
            using (var store = InitAggressiveCaching())
            {
                using (var session = store.OpenSession())
                {
                    using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
                    {
                        session.Query<User>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .ToList();
                    }
                }
                Assert.Equal(1, Server.Metrics.RequestsMeter.Count);
            }
        }
    }
}
