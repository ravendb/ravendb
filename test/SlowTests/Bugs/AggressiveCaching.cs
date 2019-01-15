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

        [Fact]
        public void CanAggressivelyCacheLoads_404()
        {
            using (var store = InitAggressiveCaching())
            {
                var requestExecutor = store.GetRequestExecutor();

                var oldNumOfRequests = requestExecutor.NumberOfServerRequests;
                for (var i = 0; i < 5; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
                        {
                            session.Load<User>("users/not-there");
                        }
                    }
                }
                Assert.InRange(requestExecutor.NumberOfServerRequests, oldNumOfRequests + 1, oldNumOfRequests + 2);
            }
        }

        [Fact]
        public void CanAggressivelyCacheLoads()
        {
            using (var store = InitAggressiveCaching())
            {
                var requestExecutor = store.GetRequestExecutor();
                var oldNumOfRequests = requestExecutor.NumberOfServerRequests;
                for (var i = 0; i < 5; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
                        {
                            session.Load<User>("users/1-A");
                        }
                    }
                }
                Assert.InRange(requestExecutor.NumberOfServerRequests, oldNumOfRequests + 1, oldNumOfRequests + 2);
            }
        }

        [Fact]
        public async Task CanAggressivelyCacheLoads_Async()
        {
            using (var store = InitAggressiveCaching())
            {
                var requestExecutor = store.GetRequestExecutor();
                var oldNumOfRequests = requestExecutor.NumberOfServerRequests;

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
                Assert.InRange(requestExecutor.NumberOfServerRequests, oldNumOfRequests + 1, oldNumOfRequests + 2);
            }
        }


        [Fact]
        public void CanAggressivelyCacheQueries()
        {
            using (var store = InitAggressiveCaching())
            {
                var requestExecutor = store.GetRequestExecutor();
                var oldNumOfRequests = requestExecutor.NumberOfServerRequests;

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
                Assert.InRange(requestExecutor.NumberOfServerRequests, oldNumOfRequests + 1, oldNumOfRequests + 2);
            }
        }

        [Fact]
        public void WaitForNonStaleResultsIgnoresAggressiveCaching()
        {
            using (var store = InitAggressiveCaching())
            {
                var requestExecutor = store.GetRequestExecutor();
                var oldNumOfRequests = requestExecutor.NumberOfServerRequests;
                for (var i = 0; i < 5; i++)
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
                }
                Assert.NotEqual(oldNumOfRequests + 1, requestExecutor.NumberOfServerRequests);
            }
        }
    }
}
