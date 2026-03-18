using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
#pragma warning disable CS0618 // Type or member is obsolete

namespace FastTests.Client
{
    public class OptimisticConcurrencyModeTests : RavenTestBase
    {
        public OptimisticConcurrencyModeTests(ITestOutputHelper output) : base(output)
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

        [RavenFact(RavenTestCategory.ClientApi)]
        public void OptimisticConcurrencyMode_Writes_WithNoTracking_ShouldThrow()
        {
            var exp = Assert.Throws<InvalidOperationException>(() =>
            {
                _ = new SessionOptions
                {
                    NoTracking = true,
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.Writes,
                };
            });
            Assert.Contains(nameof(OptimisticConcurrencyMode), exp.Message);

            exp = Assert.Throws<InvalidOperationException>(() =>
            {
                _ = new SessionOptions
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.Writes,
                    NoTracking = true,
                };
            });
            Assert.Contains(nameof(OptimisticConcurrencyMode), exp.Message);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void OptimisticConcurrencyMode_WritesAndReads_WithNoTracking_ShouldThrow()
        {
            var exp = Assert.Throws<InvalidOperationException>(() =>
            {
                _ = new SessionOptions
                {
                    NoTracking = true,
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                };
            });
            Assert.Contains(nameof(OptimisticConcurrencyMode), exp.Message);

            exp = Assert.Throws<InvalidOperationException>(() =>
            {
                _ = new SessionOptions
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                    NoTracking = true,
                };
            });
            Assert.Contains(nameof(OptimisticConcurrencyMode), exp.Message);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void OptimisticConcurrencyMode_None_WithNoTracking_ShouldNotThrow()
        {
            _ = new SessionOptions
            {
                NoTracking = true,
                OptimisticConcurrencyMode = OptimisticConcurrencyMode.None,
            };

            _ = new SessionOptions
            {
                OptimisticConcurrencyMode = OptimisticConcurrencyMode.None,
                NoTracking = true,
            };
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void NoTracking_WithOptimisticConcurrencyMode_FromConventions_ShouldThrow()
        {
            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDocumentStore = s => s.Conventions.OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                   }))
            {
                var exp = Assert.Throws<InvalidOperationException>(() =>
                {
                    store.OpenSession(new SessionOptions { NoTracking = true });
                });
                Assert.Contains(nameof(OptimisticConcurrencyMode), exp.Message);
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
        [RavenFact(RavenTestCategory.ClientApi)]
        public void NoTracking_WithOptimisticConcurrencyMode_Writes_FromConventions_ShouldThrow()
        {
            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDocumentStore = s => s.Conventions.OptimisticConcurrencyMode = OptimisticConcurrencyMode.Writes
                   }))
            {
                var exp = Assert.Throws<InvalidOperationException>(() =>
                {
                    store.OpenSession(new SessionOptions { NoTracking = true });
                });
                Assert.Contains(nameof(OptimisticConcurrencyMode), exp.Message);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void UseOptimisticConcurrency_AfterSessionOptionsOptimisticConcurrencyMode_ShouldThrow()
        {
            // Copilot review: constructor should set _optimisticConcurrencyModeWasSet when
            // options.OptimisticConcurrencyMode is provided, so that UseOptimisticConcurrency
            // cannot be set afterwards without going through advanced.OptimisticConcurrencyMode first
            using (var store = GetDocumentStore())
            {
                using var session = store.OpenSession(new SessionOptions
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                });

                // should throw because the session was created with the new API via SessionOptions
                var ex = Assert.Throws<InvalidOperationException>(() => session.Advanced.UseOptimisticConcurrency = true);
                Assert.Contains(nameof(InMemoryDocumentSessionOperations.UseOptimisticConcurrency), ex.Message);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void UseOptimisticConcurrency_WhenInheritedFromConventions_ShouldNotThrow()
        {
            // When conventions set OptimisticConcurrencyMode but SessionOptions doesn't explicitly set it,
            // the session inherits from conventions. In this case, UseOptimisticConcurrency should still
            // be settable (no flag was set on the session itself).
            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDocumentStore = s => s.Conventions.OptimisticConcurrencyMode = OptimisticConcurrencyMode.Writes
                   }))
            {
                using var session = store.OpenSession(); // no explicit SessionOptions.OptimisticConcurrencyMode

                // should NOT throw because the mode was inherited from conventions, not explicitly set on session
                session.Advanced.UseOptimisticConcurrency = false;
                Assert.Equal(OptimisticConcurrencyMode.None, session.Advanced.OptimisticConcurrencyMode);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void Conventions_UseOptimisticConcurrency_LossyRoundTrip()
        {
            var conventions = new DocumentConventions();

            // set WritesAndReads via new API
            conventions.OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads;
            Assert.Equal(OptimisticConcurrencyMode.WritesAndReads, conventions.OptimisticConcurrencyMode);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void Conventions_MutualExclusion_OptimisticConcurrencyMode_Then_UseOptimisticConcurrency_ShouldThrow()
        {
            var conventions = new DocumentConventions();

            conventions.OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads;

            var ex = Assert.Throws<InvalidOperationException>(() => conventions.UseOptimisticConcurrency = true);
            Assert.Contains(nameof(DocumentConventions.UseOptimisticConcurrency), ex.Message);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void Conventions_MutualExclusion_UseOptimisticConcurrency_Then_OptimisticConcurrencyMode_ShouldThrow()
        {
            var conventions = new DocumentConventions();

            conventions.UseOptimisticConcurrency = true;

            var ex = Assert.Throws<InvalidOperationException>(() => conventions.OptimisticConcurrencyMode = OptimisticConcurrencyMode.Writes);
            Assert.Contains(nameof(DocumentConventions.OptimisticConcurrencyMode), ex.Message);
        }
    }
}
