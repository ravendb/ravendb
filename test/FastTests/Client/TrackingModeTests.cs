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
        public void NoTracking_CanBeSetMultipleTimes()
        {
            var options = new SessionOptions();

            options.NoTracking = true;
            Assert.True(options.NoTracking);

            options.NoTracking = false;
            Assert.False(options.NoTracking);

            options.NoTracking = true;
            Assert.True(options.NoTracking);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void UseOptimisticConcurrency_AfterOptimisticConcurrencyMode_ShouldThrow()
        {
            var options = new SessionOptions { OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads };

            using (var store = GetDocumentStore())
            {
                using var session = store.OpenSession(options);
                var advanced = session.Advanced;
                advanced.OptimisticConcurrencyMode = OptimisticConcurrencyMode.Writes;

                var ex = Assert.Throws<InvalidOperationException>(() => advanced.UseOptimisticConcurrency = true);
                Assert.Contains(nameof(InMemoryDocumentSessionOperations.UseOptimisticConcurrency), ex.Message);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void OptimisticConcurrencyMode_AfterUseOptimisticConcurrency_ShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                using var session = store.OpenSession();
                var advanced = session.Advanced;
                advanced.UseOptimisticConcurrency = true;

                var ex = Assert.Throws<InvalidOperationException>(() => advanced.OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads);
                Assert.Contains(nameof(InMemoryDocumentSessionOperations.OptimisticConcurrencyMode), ex.Message);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void OptimisticConcurrencyMode_Writes_WithClusterWide_ShouldThrow()
        {
            var exp = Assert.Throws<InvalidOperationException>(() =>
            {
                _ = new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide,
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.Writes,
                };
            });
            Assert.Contains(nameof(OptimisticConcurrencyMode), exp.Message);

            exp = Assert.Throws<InvalidOperationException>(() =>
            {
                _ = new SessionOptions
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.Writes,
                    TransactionMode = TransactionMode.ClusterWide,
                };
            });
            Assert.Contains(nameof(OptimisticConcurrencyMode), exp.Message);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void OptimisticConcurrencyMode_WritesAndReads_WithClusterWide_ShouldThrow()
        {
            var exp = Assert.Throws<InvalidOperationException>(() =>
            {
                _ = new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide,
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                };
            });
            Assert.Contains(nameof(OptimisticConcurrencyMode), exp.Message);

            exp = Assert.Throws<InvalidOperationException>(() =>
            {
                _ = new SessionOptions
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                    TransactionMode = TransactionMode.ClusterWide,
                };
            });
            Assert.Contains(nameof(OptimisticConcurrencyMode), exp.Message);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void OptimisticConcurrencyMode_None_WithClusterWide_ShouldNotThrow()
        {
            _ = new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide,
                OptimisticConcurrencyMode = OptimisticConcurrencyMode.None,
            };

            _ = new SessionOptions
            {
                OptimisticConcurrencyMode = OptimisticConcurrencyMode.None,
                TransactionMode = TransactionMode.ClusterWide,
            };
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void OptimisticConcurrencyMode_WithSingleNode_ShouldNotThrow()
        {
            _ = new SessionOptions
            {
                TransactionMode = TransactionMode.SingleNode,
                OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
            };

            _ = new SessionOptions
            {
                OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                TransactionMode = TransactionMode.SingleNode,
            };
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void UseOptimisticConcurrency_MapsCorrectly()
        {
            using (var store = GetDocumentStore())
            {
                using var session = store.OpenSession();
                var advanced = session.Advanced;

                Assert.Equal(OptimisticConcurrencyMode.None, advanced.OptimisticConcurrencyMode);

                advanced.UseOptimisticConcurrency = true;
                Assert.Equal(OptimisticConcurrencyMode.Writes, advanced.OptimisticConcurrencyMode);

                advanced.UseOptimisticConcurrency = false;
                Assert.Equal(OptimisticConcurrencyMode.None, advanced.OptimisticConcurrencyMode);
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void CanConfigureOptimisticConcurrencyModeForSessions(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession(new SessionOptions { NoTracking = false }))
                {
                    var inMemSes = (InMemoryDocumentSessionOperations)session;
                    Assert.False(inMemSes.NoTracking);
                }

                using (var session = store.OpenSession(new SessionOptions { NoTracking = true }))
                {
                    var inMemSes = (InMemoryDocumentSessionOperations)session;
                    Assert.True(inMemSes.NoTracking);
                }

                using (var session = store.OpenSession(new SessionOptions
                       {
                           OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                       }))
                {
                    var inMemSes = (InMemoryDocumentSessionOperations)session;
                    Assert.Equal(OptimisticConcurrencyMode.WritesAndReads, inMemSes.OptimisticConcurrencyMode);
                }

                using (var session = store.OpenSession(new SessionOptions
                       {
                           OptimisticConcurrencyMode = OptimisticConcurrencyMode.Writes
                       }))
                {
                    var inMemSes = (InMemoryDocumentSessionOperations)session;
                    Assert.Equal(OptimisticConcurrencyMode.Writes, inMemSes.OptimisticConcurrencyMode);
                }

                using (var session = store.OpenSession(new SessionOptions
                       {
                           OptimisticConcurrencyMode = OptimisticConcurrencyMode.None
                       }))
                {
                    var inMemSes = (InMemoryDocumentSessionOperations)session;
                    Assert.Equal(OptimisticConcurrencyMode.None, inMemSes.OptimisticConcurrencyMode);
                }
            }
        }
    }
}
