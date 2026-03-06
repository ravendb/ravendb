using System;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
#pragma warning disable CS0618 // Type or member is obsolete

namespace FastTests.Client
{
    public class TrackingModeTests : RavenTestBase
    {
        public TrackingModeTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void NoTracking_CanBeSetMultipleTimes_WithoutThrowingMisleadingError()
        {
            var options = new SessionOptions();

            // First assignment - should not throw
            options.NoTracking = true;
            Assert.True(options.NoTracking);
            Assert.Equal(TrackingMode.NoTracking, options.TrackingMode);

            // Second assignment (toggling off) - must not throw with a misleading
            // "TrackingMode was set" message, because TrackingMode was never set explicitly
            var ex = Record.Exception(() => options.NoTracking = false);
            Assert.Null(ex);
            Assert.False(options.NoTracking);
            Assert.Equal(TrackingMode.Default, options.TrackingMode);

            // Third assignment (toggling back on) - must still not throw
            ex = Record.Exception(() => options.NoTracking = true);
            Assert.Null(ex);
            Assert.True(options.NoTracking);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void NoTracking_DoesNotSetTrackingModeWasSet_Flag()
        {
            var options = new SessionOptions();

            options.NoTracking = true;

            // TrackingModeWasSet must remain false so that the cross-flag guard between
            // NoTracking and TrackingMode still works correctly in both directions
            Assert.False(options.TrackingModeWasSet);
            Assert.True(options.NoTrackingWasSet);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void TrackingMode_AfterNoTracking_ShouldThrow()
        {
            var options = new SessionOptions();
            options.NoTracking = true;

            // Setting TrackingMode explicitly after NoTracking was used must still throw
            var ex = Assert.Throws<InvalidOperationException>(() =>
                options.TrackingMode = TrackingMode.TrackAllEntities);

            Assert.Contains(nameof(SessionOptions.TrackingMode), ex.Message);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void NoTracking_AfterTrackingMode_ShouldThrow()
        {
            var options = new SessionOptions();
            options.TrackingMode = TrackingMode.NoTracking;

            // Setting NoTracking explicitly after TrackingMode was used must still throw
            var ex = Assert.Throws<InvalidOperationException>(() => options.NoTracking = false);

            Assert.Contains(nameof(SessionOptions.NoTracking), ex.Message);
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void CanConfigureTrackingModeForClusterwideSessions(Options options)
        {
            var exp = Assert.Throws<InvalidOperationException>(() =>
            {
                var ses = new SessionOptions()
                {
                    TransactionMode = TransactionMode.ClusterWide,
                    TrackingMode = TrackingMode.TrackAllEntities,
                };
            });
            Assert.Equal("TrackingMode cannot be set to TrackAllEntities when TransactionMode is ClusterWide.", exp.Message);

            exp = Assert.Throws<InvalidOperationException>(() =>
            {
                var session = new SessionOptions()
                {
                    TrackingMode = TrackingMode.TrackAllEntities,
                    TransactionMode = TransactionMode.ClusterWide,

                };
            });
            Assert.Equal("TrackingMode cannot be set to TrackAllEntities when TransactionMode is ClusterWide.", exp.Message);

            var session = new SessionOptions()
            {
                NoTracking = true,
                TransactionMode = TransactionMode.ClusterWide,
            };

            session = new SessionOptions()
            {
                TransactionMode = TransactionMode.ClusterWide,
                NoTracking = true,
            };

            session = new SessionOptions()
            {
                NoTracking = false,
                TransactionMode = TransactionMode.ClusterWide,

            };
            session = new SessionOptions()
            {
                TransactionMode = TransactionMode.ClusterWide,
                NoTracking = false,


            };

            session = new SessionOptions()
            {
                TransactionMode = TransactionMode.SingleNode,
                TrackingMode = TrackingMode.TrackAllEntities,
            };
            session = new SessionOptions()
            {
                TrackingMode = TrackingMode.TrackAllEntities,
                TransactionMode = TransactionMode.SingleNode,

            };
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void CanConfigureTrackingModeForSessions(Options options)
        {
            var exp = Assert.Throws<InvalidOperationException>(() =>
            {
                var session = new SessionOptions()
                {
                    TrackingMode = TrackingMode.TrackAllEntities,
                    NoTracking = true
                };
            });
            Assert.Equal("NoTracking cannot be set when TrackingMode was set. Please use TrackingMode instead of NoTracking.", exp.Message);
            exp = Assert.Throws<InvalidOperationException>(() =>
            {
                var session = new SessionOptions()
                {
                    TrackingMode = TrackingMode.Default,
                    NoTracking = true
                };
            });
            Assert.Equal("NoTracking cannot be set when TrackingMode was set. Please use TrackingMode instead of NoTracking.", exp.Message);
            exp = Assert.Throws<InvalidOperationException>(() =>
            {
                var session = new SessionOptions()
                {
                    TrackingMode = TrackingMode.NoTracking,
                    NoTracking = true
                };

            });
            Assert.Equal("NoTracking cannot be set when TrackingMode was set. Please use TrackingMode instead of NoTracking.", exp.Message);
            exp = Assert.Throws<InvalidOperationException>(() =>
            {
                var session = new SessionOptions()
                {
                    TrackingMode = TrackingMode.TrackAllEntities,
                    NoTracking = false
                };
            });
            Assert.Equal("NoTracking cannot be set when TrackingMode was set. Please use TrackingMode instead of NoTracking.", exp.Message);
            exp = Assert.Throws<InvalidOperationException>(() =>
            {

                var session = new SessionOptions()
                {
                    TrackingMode = TrackingMode.Default,
                    NoTracking = false
                };
            });
            Assert.Equal("NoTracking cannot be set when TrackingMode was set. Please use TrackingMode instead of NoTracking.", exp.Message);
            exp = Assert.Throws<InvalidOperationException>(() =>
            {
                var session = new SessionOptions()
                {
                    TrackingMode = TrackingMode.NoTracking,
                    NoTracking = false
                };
            });
            Assert.Equal("NoTracking cannot be set when TrackingMode was set. Please use TrackingMode instead of NoTracking.", exp.Message);
            exp = Assert.Throws<InvalidOperationException>(() =>
            {
                var session = new SessionOptions()
                {
                    NoTracking = true,
                    TrackingMode = TrackingMode.TrackAllEntities,
                };
            });
            Assert.Equal("TrackingMode cannot be set when NoTracking was set. Please use TrackingMode instead of NoTracking.", exp.Message);
            exp = Assert.Throws<InvalidOperationException>(() =>
            {
                var session = new SessionOptions()
                {
                    NoTracking = true,
                    TrackingMode = TrackingMode.Default,
                };
            });
            Assert.Equal("TrackingMode cannot be set when NoTracking was set. Please use TrackingMode instead of NoTracking.", exp.Message);
            exp = Assert.Throws<InvalidOperationException>(() =>
            {
                var session = new SessionOptions()
                {
                    NoTracking = true,
                    TrackingMode = TrackingMode.NoTracking,
                };
            });
            Assert.Equal("TrackingMode cannot be set when NoTracking was set. Please use TrackingMode instead of NoTracking.", exp.Message);
            exp = Assert.Throws<InvalidOperationException>(() =>
            {
                var session = new SessionOptions()
                {
                    NoTracking = false,
                    TrackingMode = TrackingMode.TrackAllEntities,
                };
            });
            Assert.Equal("TrackingMode cannot be set when NoTracking was set. Please use TrackingMode instead of NoTracking.", exp.Message);
            exp = Assert.Throws<InvalidOperationException>(() =>
            {

                var session = new SessionOptions()
                {
                    NoTracking = false,
                    TrackingMode = TrackingMode.Default,
                };
            });
            Assert.Equal("TrackingMode cannot be set when NoTracking was set. Please use TrackingMode instead of NoTracking.", exp.Message);
            exp = Assert.Throws<InvalidOperationException>(() =>
            {
                var session = new SessionOptions()
                {
                    NoTracking = false,
                    TrackingMode = TrackingMode.NoTracking,
                };
            });
            using (var store = GetDocumentStore(options))
            {
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
