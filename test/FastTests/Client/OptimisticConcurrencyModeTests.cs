using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
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

        private class SimpleDoc
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void RegisterForConcurrencyCheck_WithEmptyId_ShouldThrow()
        {
            using var store = GetDocumentStore();
            using var session = store.OpenSession();

            Assert.Throws<ArgumentNullException>(() => session.Advanced.RegisterForConcurrencyCheck(null, "cv"));
            Assert.Throws<ArgumentNullException>(() => session.Advanced.RegisterForConcurrencyCheck(string.Empty, "cv"));
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void RegisterForConcurrencyCheck_ThrowsWhenRegisteredDocumentWasModified()
        {
            using var store = GetDocumentStore();

            using (var seed = store.OpenSession())
            {
                seed.Store(new SimpleDoc { Name = "Original" }, "docs/watched");
                seed.Store(new SimpleDoc { Name = "Other" }, "docs/other");
                seed.SaveChanges();
            }

            // capture the change vector of the watched document in one session
            string changeVector;
            using (var sessionA = store.OpenSession())
            {
                changeVector = sessionA.Advanced.GetChangeVectorFor(sessionA.Load<SimpleDoc>("docs/watched"));
            }

            // a different session modifies the watched document in the background
            using (var background = store.OpenSession())
            {
                background.Load<SimpleDoc>("docs/watched").Name = "Changed";
                background.SaveChanges();
            }

            // this session uses the default None mode, proving the registered check is honored regardless of mode
            using (var sessionB = store.OpenSession())
            {
                sessionB.Load<SimpleDoc>("docs/other").Name = "Edited";
                sessionB.Advanced.RegisterForConcurrencyCheck("docs/watched", changeVector);

                var e = Assert.Throws<ConcurrencyException>(() => sessionB.SaveChanges());
                Assert.Equal("docs/watched", e.Id);
            }

            // the whole batch must have been rolled back, so docs/other is untouched
            using (var verify = store.OpenSession())
            {
                Assert.Equal("Other", verify.Load<SimpleDoc>("docs/other").Name);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void RegisterForConcurrencyCheck_SucceedsWhenRegisteredDocumentUnchanged()
        {
            using var store = GetDocumentStore();

            using (var seed = store.OpenSession())
            {
                seed.Store(new SimpleDoc { Name = "Original" }, "docs/watched");
                seed.Store(new SimpleDoc { Name = "Other" }, "docs/other");
                seed.SaveChanges();
            }

            string changeVector;
            using (var sessionA = store.OpenSession())
            {
                changeVector = sessionA.Advanced.GetChangeVectorFor(sessionA.Load<SimpleDoc>("docs/watched"));
            }

            using (var sessionB = store.OpenSession())
            {
                sessionB.Load<SimpleDoc>("docs/other").Name = "Edited";
                sessionB.Advanced.RegisterForConcurrencyCheck("docs/watched", changeVector);
                sessionB.SaveChanges();
            }

            using (var verify = store.OpenSession())
            {
                Assert.Equal("Edited", verify.Load<SimpleDoc>("docs/other").Name);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void RegisterForConcurrencyCheck_EmptyChangeVectorAssertsDocumentDoesNotExist()
        {
            using var store = GetDocumentStore();

            using (var seed = store.OpenSession())
            {
                seed.Store(new SimpleDoc { Name = "Other" }, "docs/other");
                seed.SaveChanges();
            }

            // the document we assert absent is created by another session
            using (var background = store.OpenSession())
            {
                background.Store(new SimpleDoc { Name = "Now exists" }, "docs/watched");
                background.SaveChanges();
            }

            using (var sessionB = store.OpenSession())
            {
                sessionB.Load<SimpleDoc>("docs/other").Name = "Edited";
                sessionB.Advanced.RegisterForConcurrencyCheck("docs/watched", string.Empty);

                Assert.Throws<ConcurrencyException>(() => sessionB.SaveChanges());
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void RegisterForConcurrencyCheck_EmptyChangeVectorSucceedsWhenDocumentStillAbsent()
        {
            using var store = GetDocumentStore();

            using (var seed = store.OpenSession())
            {
                seed.Store(new SimpleDoc { Name = "Other" }, "docs/other");
                seed.SaveChanges();
            }

            using (var sessionB = store.OpenSession())
            {
                sessionB.Load<SimpleDoc>("docs/other").Name = "Edited";
                sessionB.Advanced.RegisterForConcurrencyCheck("docs/missing", string.Empty);
                sessionB.SaveChanges();
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void RegisterForConcurrencyCheck_NullChangeVectorDisablesCheckForId()
        {
            using var store = GetDocumentStore();

            using (var seed = store.OpenSession())
            {
                seed.Store(new SimpleDoc { Name = "Original" }, "docs/watched");
                seed.Store(new SimpleDoc { Name = "Other" }, "docs/other");
                seed.SaveChanges();
            }

            using (var sessionB = store.OpenSession(new SessionOptions { OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads }))
            {
                // under WritesAndReads this load is auto-tracked and would normally trigger a concurrency check
                sessionB.Load<SimpleDoc>("docs/watched");
                sessionB.Load<SimpleDoc>("docs/other").Name = "Edited";

                using (var background = store.OpenSession())
                {
                    background.Load<SimpleDoc>("docs/watched").Name = "Changed";
                    background.SaveChanges();
                }

                // explicitly disable the check for the watched document
                sessionB.Advanced.RegisterForConcurrencyCheck("docs/watched", null);

                sessionB.SaveChanges();
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void RegisterForConcurrencyCheck_IsConsumedByASuccessfulSaveChanges_AfterModify()
        {
            using var store = GetDocumentStore();

            using (var seed = store.OpenSession())
            {
                seed.Store(new SimpleDoc { Name = "Original" }, "docs/watched");
                seed.Store(new SimpleDoc { Name = "Other" }, "docs/other");
                seed.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var watched = session.Load<SimpleDoc>("docs/watched");
                session.Advanced.RegisterForConcurrencyCheck("docs/watched", session.Advanced.GetChangeVectorFor(watched));

                watched.Name = "Changed";
                session.SaveChanges(); // the check passes, and this very session advances the change vector

                session.Load<SimpleDoc>("docs/other").Name = "Edited";
                session.SaveChanges(); // must not re-assert the change vector this session already replaced
            }

            using (var verify = store.OpenSession())
            {
                Assert.Equal("Changed", verify.Load<SimpleDoc>("docs/watched").Name);
                Assert.Equal("Edited", verify.Load<SimpleDoc>("docs/other").Name);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void RegisterForConcurrencyCheck_IsConsumedByASuccessfulSaveChanges_AfterDelete()
        {
            using var store = GetDocumentStore();

            using (var seed = store.OpenSession())
            {
                seed.Store(new SimpleDoc { Name = "Original" }, "docs/watched");
                seed.Store(new SimpleDoc { Name = "Other" }, "docs/other");
                seed.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var watched = session.Load<SimpleDoc>("docs/watched");
                session.Advanced.RegisterForConcurrencyCheck("docs/watched", session.Advanced.GetChangeVectorFor(watched));

                session.Delete(watched);
                session.SaveChanges(); // the check passes, and this very session deletes the document

                session.Load<SimpleDoc>("docs/other").Name = "Edited";
                session.SaveChanges(); // must not assert the old change vector against this session's own tombstone
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void RegisterForConcurrencyCheck_IsKeptWhenSaveChangesFails()
        {
            using var store = GetDocumentStore();

            using (var seed = store.OpenSession())
            {
                seed.Store(new SimpleDoc { Name = "Original" }, "docs/watched");
                seed.Store(new SimpleDoc { Name = "Other" }, "docs/other");
                seed.SaveChanges();
            }

            string staleChangeVector;
            using (var sessionA = store.OpenSession())
            {
                staleChangeVector = sessionA.Advanced.GetChangeVectorFor(sessionA.Load<SimpleDoc>("docs/watched"));
            }

            using (var background = store.OpenSession())
            {
                background.Load<SimpleDoc>("docs/watched").Name = "Changed";
                background.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                session.Load<SimpleDoc>("docs/other").Name = "Edited";
                session.Advanced.RegisterForConcurrencyCheck("docs/watched", staleChangeVector);

                Assert.Throws<ConcurrencyException>(() => session.SaveChanges());

                // a failed save must not consume the registration, so a retry fails the same way
                Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void RegisterForConcurrencyCheck_ExplicitChangeVector_IsNotOverwrittenByALaterLoad()
        {
            using var store = GetDocumentStore();

            using (var seed = store.OpenSession())
            {
                seed.Store(new SimpleDoc { Name = "Original" }, "docs/watched");
                seed.Store(new SimpleDoc { Name = "Other" }, "docs/other");
                seed.SaveChanges();
            }

            string staleChangeVector;
            using (var sessionA = store.OpenSession())
            {
                staleChangeVector = sessionA.Advanced.GetChangeVectorFor(sessionA.Load<SimpleDoc>("docs/watched"));
            }

            using (var background = store.OpenSession())
            {
                background.Load<SimpleDoc>("docs/watched").Name = "Changed";
                background.SaveChanges();
            }

            using (var session = store.OpenSession(new SessionOptions { OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads }))
            {
                session.Advanced.RegisterForConcurrencyCheck("docs/watched", staleChangeVector);

                // loading the watched document must not replace the explicitly registered change vector
                session.Load<SimpleDoc>("docs/watched");

                session.Load<SimpleDoc>("docs/other").Name = "Edited";

                var e = Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
                Assert.Equal("docs/watched", e.Id);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void RegisterForConcurrencyCheck_DisabledCheck_SurvivesALaterLoad()
        {
            using var store = GetDocumentStore();

            using (var seed = store.OpenSession())
            {
                seed.Store(new SimpleDoc { Name = "Original" }, "docs/watched");
                seed.Store(new SimpleDoc { Name = "Other" }, "docs/other");
                seed.SaveChanges();
            }

            using (var session = store.OpenSession(new SessionOptions { OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads }))
            {
                // disable the check first, then load - the outcome must not depend on this order
                session.Advanced.RegisterForConcurrencyCheck("docs/watched", null);
                session.Load<SimpleDoc>("docs/watched");

                using (var background = store.OpenSession())
                {
                    background.Load<SimpleDoc>("docs/watched").Name = "Changed";
                    background.SaveChanges();
                }

                session.Load<SimpleDoc>("docs/other").Name = "Edited";

                session.SaveChanges();
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void RegisterForConcurrencyCheck_IsHonored_WhenDocumentIsAlsoWrittenWithItsOwnConcurrencyCheck()
        {
            using var store = GetDocumentStore();

            using (var seed = store.OpenSession())
            {
                seed.Store(new SimpleDoc { Name = "Original" }, "docs/watched");
                seed.SaveChanges();
            }

            string staleChangeVector;
            using (var sessionA = store.OpenSession())
            {
                staleChangeVector = sessionA.Advanced.GetChangeVectorFor(sessionA.Load<SimpleDoc>("docs/watched"));
            }

            using (var background = store.OpenSession())
            {
                background.Load<SimpleDoc>("docs/watched").Name = "Changed";
                background.SaveChanges();
            }

            using (var session = store.OpenSession(new SessionOptions { OptimisticConcurrencyMode = OptimisticConcurrencyMode.Writes }))
            {
                var watched = session.Load<SimpleDoc>("docs/watched"); // tracked with the current change vector
                session.Advanced.RegisterForConcurrencyCheck("docs/watched", staleChangeVector);

                watched.Name = "Edited";

                // the write carries its own check, which uses the current change vector and would pass,
                // so only the explicitly registered (stale) check can fail this save
                Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void RegisterForConcurrencyCheck_LastRegistrationForAnIdWins()
        {
            using var store = GetDocumentStore();

            using (var seed = store.OpenSession())
            {
                seed.Store(new SimpleDoc { Name = "Original" }, "docs/watched");
                seed.Store(new SimpleDoc { Name = "Other" }, "docs/other");
                seed.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                session.Advanced.RegisterForConcurrencyCheck("docs/watched", "A:1-aaaaaaaaaaaaaaaaaaaaaa");
                session.Advanced.RegisterForConcurrencyCheck("docs/watched", null); // cancels the check

                session.Load<SimpleDoc>("docs/other").Name = "Edited";
                session.SaveChanges();
            }

            using (var verify = store.OpenSession())
            {
                Assert.Equal("Edited", verify.Load<SimpleDoc>("docs/other").Name);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void RegisterForConcurrencyCheck_IsClearedByAdvancedClear()
        {
            using var store = GetDocumentStore();

            using (var seed = store.OpenSession())
            {
                seed.Store(new SimpleDoc { Name = "Original" }, "docs/watched");
                seed.Store(new SimpleDoc { Name = "Other" }, "docs/other");
                seed.SaveChanges();
            }

            string staleChangeVector;
            using (var sessionA = store.OpenSession())
            {
                staleChangeVector = sessionA.Advanced.GetChangeVectorFor(sessionA.Load<SimpleDoc>("docs/watched"));
            }

            using (var background = store.OpenSession())
            {
                background.Load<SimpleDoc>("docs/watched").Name = "Changed";
                background.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                session.Advanced.RegisterForConcurrencyCheck("docs/watched", staleChangeVector);
                session.Advanced.Clear();

                session.Load<SimpleDoc>("docs/other").Name = "Edited";
                session.SaveChanges();
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void RegisterForConcurrencyCheck_OnItsOwn_SendsExactlyOneRequest()
        {
            using var store = GetDocumentStore();

            using (var seed = store.OpenSession())
            {
                seed.Store(new SimpleDoc { Name = "Original" }, "docs/watched");
                seed.SaveChanges();
            }

            string changeVector;
            using (var sessionA = store.OpenSession())
            {
                changeVector = sessionA.Advanced.GetChangeVectorFor(sessionA.Load<SimpleDoc>("docs/watched"));
            }

            using (var session = store.OpenSession())
            {
                session.Advanced.RegisterForConcurrencyCheck("docs/watched", changeVector);
                session.SaveChanges();

                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void RegisterForConcurrencyCheck_DisabledOnly_SendsNoRequest()
        {
            using var store = GetDocumentStore();
            using var session = store.OpenSession();

            session.Advanced.RegisterForConcurrencyCheck("docs/watched", "A:1-aaaaaaaaaaaaaaaaaaaaaa");
            session.Advanced.RegisterForConcurrencyCheck("docs/watched", null);

            session.SaveChanges();

            Assert.Equal(0, session.Advanced.NumberOfRequests);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task RegisterForConcurrencyCheck_WorksInAnAsyncSession()
        {
            using var store = GetDocumentStore();

            using (var seed = store.OpenAsyncSession())
            {
                await seed.StoreAsync(new SimpleDoc { Name = "Original" }, "docs/watched");
                await seed.StoreAsync(new SimpleDoc { Name = "Other" }, "docs/other");
                await seed.SaveChangesAsync();
            }

            string staleChangeVector;
            using (var sessionA = store.OpenAsyncSession())
            {
                staleChangeVector = sessionA.Advanced.GetChangeVectorFor(await sessionA.LoadAsync<SimpleDoc>("docs/watched"));
            }

            using (var background = store.OpenAsyncSession())
            {
                (await background.LoadAsync<SimpleDoc>("docs/watched")).Name = "Changed";
                await background.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                (await session.LoadAsync<SimpleDoc>("docs/other")).Name = "Edited";
                session.Advanced.RegisterForConcurrencyCheck("docs/watched", staleChangeVector);

                var e = await Assert.ThrowsAsync<ConcurrencyException>(() => session.SaveChangesAsync());
                Assert.Equal("docs/watched", e.Id);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void RegisterForConcurrencyCheck_IsNotSupportedInANoTrackingSession()
        {
            // a NoTracking session rejects SaveChanges as soon as the batch holds any command, so a registered
            // check can never reach the server from such a session
            using var store = GetDocumentStore();

            using (var session = store.OpenSession(new SessionOptions { NoTracking = true }))
            {
                session.Advanced.RegisterForConcurrencyCheck("docs/watched", "A:1-aaaaaaaaaaaaaaaaaaaaaa");

                Assert.Throws<InvalidOperationException>(() => session.SaveChanges());
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void RegisterForConcurrencyCheck_IsNotSupportedInAClusterWideSession()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                session.Advanced.RegisterForConcurrencyCheck("docs/watched", "A:1-aaaaaaaaaaaaaaaaaaaaaa");

                var e = Assert.Throws<NotSupportedException>(() => session.SaveChanges());
                Assert.Contains(nameof(IAdvancedDocumentSessionOperations.RegisterForConcurrencyCheck), e.Message);
            }
        }
    }
}
