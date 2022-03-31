using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Http;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_7498 : RavenTestBase
    {
        public RavenDB_7498(ITestOutputHelper output) : base(output)
        {
        }

        public DocumentStore InitAggressiveCachingWithNotTrackingMode()
        {
            var store = GetDocumentStore(new Options()
            {
                ModifyDocumentStore = s =>
                    s.Conventions.AggressiveCache.Mode = AggressiveCacheMode.DoNotTrackChanges
            });
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
            using (var store = InitAggressiveCachingWithNotTrackingMode())
            using (var anotherStore = new DocumentStore()
            {
                Urls = store.Urls,
                Database = store.Database
            }.Initialize())
            {
                var requestExecutor = store.GetRequestExecutor();

                var oldNumOfRequests = requestExecutor.NumberOfServerRequests;

                var backgroundUpdatesCts = new CancellationTokenSource();

                var backgroundUpdatesTask = Task.Factory.StartNew(() =>
                {
                    while (backgroundUpdatesCts.IsCancellationRequested == false)
                    {
                        using (var session = anotherStore.OpenSession())
                        {
                            session.Store(new User());
                            session.Store(new User(), "users/1-A");
                            session.SaveChanges();
                        };
                    }
                });

                for (var i = 0; i < 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
                        {
                            session.Load<User>("users/not-there");

                            // we shouldn't be notified by update by changes api due to AggressiveCacheMode.NoChangesTracking usage
                            // just in case let's wait a bit to make sure we aren't getting them and aren't invalidating the cache
                            Thread.Sleep(100);
                        }
                    }
                }

                Assert.Equal(oldNumOfRequests + 1, requestExecutor.NumberOfServerRequests);

                backgroundUpdatesCts.Cancel();

                Assert.True(backgroundUpdatesTask.Wait(TimeSpan.FromMinutes(1)));
            }
        }

        [Fact]
        public void CanAggressivelyCacheLoads()
        {
            using (var store = InitAggressiveCachingWithNotTrackingMode())
            using (var anotherStore = new DocumentStore()
            {
                Urls = store.Urls,
                Database = store.Database
            }.Initialize())
            {
                var requestExecutor = store.GetRequestExecutor();
                var oldNumOfRequests = requestExecutor.NumberOfServerRequests;

                var backgroundUpdatesCts = new CancellationTokenSource();

                var backgroundUpdatesTask = Task.Factory.StartNew(() =>
                {
                    while (backgroundUpdatesCts.IsCancellationRequested == false)
                    {
                        using (var session = anotherStore.OpenSession())
                        {
                            session.Store(new User());
                            session.Store(new User(), "users/1-A");
                            session.SaveChanges();
                        };
                    }
                });

                for (var i = 0; i < 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
                        {
                            session.Load<User>("users/1-A");

                            // we shouldn't be notified by update by changes api due to AggressiveCacheMode.NoChangesTracking usage
                            // just in case let's wait a bit to make sure we aren't getting them and aren't invalidating the cache
                            Thread.Sleep(100); 
                        }
                    }
                }

                Assert.Equal(oldNumOfRequests + 1, requestExecutor.NumberOfServerRequests);

                backgroundUpdatesCts.Cancel();

                Assert.True(backgroundUpdatesTask.Wait(TimeSpan.FromMinutes(1)));
            }
        }

        [Fact]
        public async Task CanAggressivelyCacheLoads_Async()
        {
            using (var store = InitAggressiveCachingWithNotTrackingMode())
            using (var anotherStore = new DocumentStore()
            {
                Urls = store.Urls,
                Database = store.Database
            }.Initialize())
            {
                var requestExecutor = store.GetRequestExecutor();
                var oldNumOfRequests = requestExecutor.NumberOfServerRequests;

                var backgroundUpdatesCts = new CancellationTokenSource();

                var backgroundUpdatesTask = Task.Factory.StartNew(() =>
                {
                    while (backgroundUpdatesCts.IsCancellationRequested == false)
                    {
                        using (var session = anotherStore.OpenSession())
                        {
                            session.Store(new User());
                            session.Store(new User(), "users/1-A");
                            session.SaveChanges();
                        };
                    }
                });

                for (var i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
                        {
                            await session.LoadAsync<User>("users/1");

                            // we shouldn't be notified by update by changes api due to AggressiveCacheMode.NoChangesTracking usage
                            // just in case let's wait a bit to make sure we aren't getting them and aren't invalidating the cache
                            Thread.Sleep(100);
                        }
                    }
                }
                Assert.Equal(oldNumOfRequests + 1, requestExecutor.NumberOfServerRequests);

                backgroundUpdatesCts.Cancel();

                await backgroundUpdatesTask;
            }
        }


        [Fact]
        public void CanAggressivelyCacheQueries()
        {
            using (var store = InitAggressiveCachingWithNotTrackingMode())
            using (var anotherStore = new DocumentStore()
            {
                Urls = store.Urls,
                Database = store.Database
            }.Initialize())
            {
                var requestExecutor = store.GetRequestExecutor();
                var oldNumOfRequests = requestExecutor.NumberOfServerRequests;

                var backgroundUpdatesCts = new CancellationTokenSource();

                var backgroundUpdatesTask = Task.Factory.StartNew(() =>
                {
                    while (backgroundUpdatesCts.IsCancellationRequested == false)
                    {
                        using (var session = anotherStore.OpenSession())
                        {
                            session.Store(new User());
                            session.Store(new User(), "users/1-A");
                            session.SaveChanges();
                        };
                    }
                });

                for (var i = 0; i < 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        using (session.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromMinutes(5)))
                        {
                            session.Query<User>().ToList();
                        }
                    }
                }
                Assert.Equal(oldNumOfRequests + 1, requestExecutor.NumberOfServerRequests);

                backgroundUpdatesCts.Cancel();

                Assert.True(backgroundUpdatesTask.Wait(TimeSpan.FromMinutes(1)));
            }
        }

        [Fact]
        public void WaitForNonStaleResultsIgnoresAggressiveCaching()
        {
            using (var store = InitAggressiveCachingWithNotTrackingMode())
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
