using System;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class TrackingModeTests : RavenTestBase
    {
        public TrackingModeTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void CanConfigureTrackingModeForClusterwideSessions(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var exp = Assert.Throws<InvalidOperationException>(() =>
                {
                    using (IDocumentSession session = store.OpenSession(new SessionOptions()
                    {
                        TransactionMode = TransactionMode.ClusterWide,
                        TrackingMode = TrackingMode.TrackAllEntities,
                    }))
                    {

                    }
                });
                Assert.Equal("TrackingMode cannot be set to TrackAllEntities when TransactionMode is ClusterWide.", exp.Message);

                exp = Assert.Throws<InvalidOperationException>(() =>
                {
                    using (IDocumentSession session = store.OpenSession(new SessionOptions()
                    {
                        TrackingMode = TrackingMode.TrackAllEntities,
                        TransactionMode = TransactionMode.ClusterWide,

                    }))
                    {

                    }
                });
                Assert.Equal("TrackingMode cannot be set to TrackAllEntities when TransactionMode is ClusterWide.", exp.Message);

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    NoTracking = true,
                    TransactionMode = TransactionMode.ClusterWide,

                }))
                {

                }
                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    TransactionMode = TransactionMode.ClusterWide,
                    NoTracking = true,


                }))
                {

                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                       {
                           NoTracking = false,
                           TransactionMode = TransactionMode.ClusterWide,

                       }))
                {

                }
                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                       {
                           TransactionMode = TransactionMode.ClusterWide,
                           NoTracking = false,


                       }))
                {

                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void CanConfigureTrackingModeForSessions(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var exp = Assert.Throws<InvalidOperationException>(() =>
                {
                    using (IDocumentSession session = store.OpenSession(new SessionOptions()
                    {
                        TrackingMode = TrackingMode.TrackAllEntities,
                        NoTracking = true
                    }))
                    {

                    }
                });
                Assert.Equal("NoTracking cannot be set when TrackingMode was set. Please use TrackingMode instead of NoTracking.", exp.Message);
                exp = Assert.Throws<InvalidOperationException>(() =>
                {
                    using (IDocumentSession session = store.OpenSession(new SessionOptions()
                    {
                        TrackingMode = TrackingMode.Default,
                        NoTracking = true
                    }))
                    {

                    }
                });
                Assert.Equal("NoTracking cannot be set when TrackingMode was set. Please use TrackingMode instead of NoTracking.", exp.Message);
                exp = Assert.Throws<InvalidOperationException>(() =>
                {
                    using (IDocumentSession session = store.OpenSession(new SessionOptions()
                    {
                        TrackingMode = TrackingMode.NoTracking,
                        NoTracking = true
                    }))
                    {

                    }
                });
                Assert.Equal("NoTracking cannot be set when TrackingMode was set. Please use TrackingMode instead of NoTracking.", exp.Message);
                exp = Assert.Throws<InvalidOperationException>(() =>
                {
                    using (IDocumentSession session = store.OpenSession(new SessionOptions()
                    {
                        TrackingMode = TrackingMode.TrackAllEntities,
                        NoTracking = false
                    }))
                    {

                    }
                });
                Assert.Equal("NoTracking cannot be set when TrackingMode was set. Please use TrackingMode instead of NoTracking.", exp.Message);
                exp = Assert.Throws<InvalidOperationException>(() =>
                {

                    using (IDocumentSession session = store.OpenSession(new SessionOptions()
                    {
                        TrackingMode = TrackingMode.Default,
                        NoTracking = false
                    }))
                    {

                    }
                });
                Assert.Equal("NoTracking cannot be set when TrackingMode was set. Please use TrackingMode instead of NoTracking.", exp.Message);
                exp = Assert.Throws<InvalidOperationException>(() =>
                {
                    using (IDocumentSession session = store.OpenSession(new SessionOptions()
                    {
                        TrackingMode = TrackingMode.NoTracking,
                        NoTracking = false
                    }))
                    {

                    }
                });
                Assert.Equal("NoTracking cannot be set when TrackingMode was set. Please use TrackingMode instead of NoTracking.", exp.Message);
                exp = Assert.Throws<InvalidOperationException>(() =>
                {
                    using (IDocumentSession session = store.OpenSession(new SessionOptions()
                    {
                        NoTracking = true,
                        TrackingMode = TrackingMode.TrackAllEntities,
                    }))
                    {

                    }
                });
                Assert.Equal("TrackingMode cannot be set when NoTracking was set. Please use TrackingMode instead of NoTracking.", exp.Message);
                exp = Assert.Throws<InvalidOperationException>(() =>
                {
                    using (IDocumentSession session = store.OpenSession(new SessionOptions()
                    {
                        NoTracking = true,
                        TrackingMode = TrackingMode.Default,
                    }))
                    {

                    }
                });
                Assert.Equal("TrackingMode cannot be set when NoTracking was set. Please use TrackingMode instead of NoTracking.", exp.Message);
                exp = Assert.Throws<InvalidOperationException>(() =>
                {
                    using (IDocumentSession session = store.OpenSession(new SessionOptions()
                    {
                        NoTracking = true,
                        TrackingMode = TrackingMode.NoTracking,
                    }))
                    {

                    }
                });
                Assert.Equal("TrackingMode cannot be set when NoTracking was set. Please use TrackingMode instead of NoTracking.", exp.Message);
                exp = Assert.Throws<InvalidOperationException>(() =>
                {
                    using (IDocumentSession session = store.OpenSession(new SessionOptions()
                    {
                        NoTracking = false,
                        TrackingMode = TrackingMode.TrackAllEntities,
                    }))
                    {

                    }
                });
                Assert.Equal("TrackingMode cannot be set when NoTracking was set. Please use TrackingMode instead of NoTracking.", exp.Message);
                exp = Assert.Throws<InvalidOperationException>(() =>
                {

                    using (IDocumentSession session = store.OpenSession(new SessionOptions()
                    {
                        NoTracking = false,
                        TrackingMode = TrackingMode.Default,
                    }))
                    {

                    }
                });
                Assert.Equal("TrackingMode cannot be set when NoTracking was set. Please use TrackingMode instead of NoTracking.", exp.Message);
                exp = Assert.Throws<InvalidOperationException>(() =>
                {
                    using (IDocumentSession session = store.OpenSession(new SessionOptions()
                    {
                        NoTracking = false,
                        TrackingMode = TrackingMode.NoTracking,
                    }))
                    {

                    }
                });


                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    NoTracking = false
                }))
                {
                    var inMemSes = ((InMemoryDocumentSessionOperations)session);
                    Assert.Equal(inMemSes.TrackingMode, TrackingMode.Default);
                }
                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    NoTracking = true
                }))
                {
                    var inMemSes = ((InMemoryDocumentSessionOperations)session);
                    Assert.Equal(inMemSes.TrackingMode, TrackingMode.NoTracking);
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    TrackingMode = TrackingMode.NoTracking,
                }))
                {
                    var inMemSes = ((InMemoryDocumentSessionOperations)session);
                    Assert.Equal(inMemSes.TrackingMode, TrackingMode.NoTracking);
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    TrackingMode = TrackingMode.Default,
                }))
                {
                    var inMemSes = ((InMemoryDocumentSessionOperations)session);
                    Assert.Equal(inMemSes.TrackingMode, TrackingMode.Default);
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    TrackingMode = TrackingMode.TrackAllEntities,
                }))
                {
                    var inMemSes = ((InMemoryDocumentSessionOperations)session);
                    Assert.Equal(inMemSes.TrackingMode, TrackingMode.TrackAllEntities);
                }
            }
        }
    }
}
