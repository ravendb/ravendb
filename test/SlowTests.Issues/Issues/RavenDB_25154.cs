using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Sharding;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public partial class RavenDB_25154 : RavenTestBase
    {
        public RavenDB_25154(ITestOutputHelper output) : base(output)
        {
        }

        private class Employee
        {
            public string FirstName { get; set; }
            public Address Address { get; set; }
        }

        internal static IEnumerable<object[]> SessionActions()
        {
            yield return [(Action<IDocumentSession>)ModifyEgorInSession];
            yield return [(Action<IDocumentSession>)ModifyEgorInPatchAfterLoad];
            yield return [(Action<IDocumentSession>)ModifyEgorInPatchNoLoad];
            yield return [(Action<IDocumentSession>)ModifyEgorWithStoreOverwriteAfterLoadAndEvict];
            yield return [(Action<IDocumentSession>)ModifyEgorWithStoreOverwriteWithoutLoad];
            yield return [(Action<IDocumentSession>)ModifyEgorWithLoadAndDeleteById];


            yield return [(Action<IDocumentSession>)ModifyEgorWithLoadAndDeleteByEntity];
            yield return [(Action<IDocumentSession>)ModifyEgorWithMultiplePatches];
            yield return [(Action<IDocumentSession>)ModifyEgorByReplacingAddress];
            yield return [(Action<IDocumentSession>)ModifyEgorWithAttachmentWithLoad];
            yield return [(Action<IDocumentSession>)ModifyEgorWithAttachmentNoLoad];
            yield return [(Action<IDocumentSession>)ModifyEgorWithTimeSeriesNoLoad];
            yield return [(Action<IDocumentSession>)ModifyEgorWithTimeSeriesWithLoad];
            yield return [(Action<IDocumentSession>)ModifyEgorWithCounterWithLoad];
            yield return [(Action<IDocumentSession>)ModifyEgorWithCounterNoLoad];
            yield return [(Action<IDocumentSession>)ModifyEgorWithRevisionNoLoad];
            yield return [(Action<IDocumentSession>)ModifyEgorWithRevisionWithLoad];
        }

        internal static void ModifyEgorInPatchAfterLoad(IDocumentSession session)
        {
            var egor = session.Load<Employee>("employees/2-A");
            session.Advanced.Patch(egor, e => e.Address.Street, "Mul HaHof Village");
        }

        internal static void ModifyEgorInPatchNoLoad(IDocumentSession session)
        {
            session.Advanced.Patch<Employee, string>("employees/2-A", e => e.Address.Street, "Mul HaHof Village");
        }

        internal static void ModifyEgorInSession(IDocumentSession session)
        {
            var egor = session.Load<Employee>("employees/2-A");
            egor.Address = new Address()
            {
                Street = "Mul HaHof Village"
            };
        }

        internal static void ModifyEgorWithStoreOverwriteAfterLoadAndEvict(IDocumentSession session)
        {
            var egor = session.Load<Employee>("employees/2-A");

            // tracked entity is evicted from the session, so its not in the trackedEntities list anymore
            session.Advanced.Evict(egor);
            // store after evict is treated as _new_ document, so it will be added to the trackedEntities list again with null change vector, and this should not cause concurrency exception because the original entity is evicted !
            session.Store(new Employee 
            {
                FirstName = "Egor",
                Address = new Address()
                {
                    Street = "Mul HaHof Village"
                }
            }, "employees/2-A");
        }

        internal static void ModifyEgorWithStoreOverwriteWithoutLoad(IDocumentSession session)
        {
            session.Store(new Employee
            {
                FirstName = "Egor",
                Address = new Address()
                {
                    Street = "Mul HaHof Village"
                }
            }, "employees/2-A");
        }

        internal static void ModifyEgorWithLoadAndDeleteById(IDocumentSession session)
        {
            var egor = session.Load<Employee>("employees/2-A");
            session.Delete("employees/2-A");
        }

        internal static void ModifyEgorWithLoadAndDeleteByEntity(IDocumentSession session)
        {
            var egor = session.Load<Employee>("employees/2-A");

            session.Delete(egor);
        }

        internal static void ModifyEgorWithMultiplePatches(IDocumentSession session)
        {
            session.Advanced.Patch<Employee, string>("employees/2-A", e => e.Address.Street, "Mul HaHof Village");
            session.Advanced.Patch<Employee, string>("employees/2-A", e => e.FirstName, "Egor");
        }

        internal static void ModifyEgorByReplacingAddress(IDocumentSession session)
        {
            var egor = session.Load<Employee>("employees/2-A");
            session.Advanced.Patch(egor, e => e.Address, new Address()
            {
                Street = "Mul HaHof Village",
                City = "Hadera"
            });
        }

        internal static void ModifyEgorWithAttachmentWithLoad(IDocumentSession session)
        {
            var egor = session.Load<Employee>("employees/2-A");
            session.Advanced.Attachments.Store(egor, "profile-picture", new MemoryStream("image data"u8.ToArray()));
        }

        internal static void ModifyEgorWithAttachmentNoLoad(IDocumentSession session)
        {
            session.Advanced.Attachments.Store("employees/2-A", "profile-picture", new MemoryStream("image data"u8.ToArray()));
        }

        internal static void ModifyEgorWithTimeSeriesNoLoad(IDocumentSession session)
        {
            session.TimeSeriesFor("employees/2-A", "profile-picture-likes")
                .Append(DateTime.UtcNow, new[] { 322d }, "super-like");
        }

        internal static void ModifyEgorWithTimeSeriesWithLoad(IDocumentSession session)
        {
            var egor = session.Load<Employee>("employees/2-A");
            session.TimeSeriesFor(egor, "profile-picture-likes")
                .Append(DateTime.UtcNow, new[] { 322d }, "super-like");
        }

        internal static void ModifyEgorWithCounterWithLoad(IDocumentSession session)
        {
            var egor = session.Load<Employee>("employees/2-A");
            session.CountersFor(egor).Increment("profile-picture-likes");
        }

        internal static void ModifyEgorWithCounterNoLoad(IDocumentSession session)
        {
            session.CountersFor("employees/2-A").Increment("profile-picture-likes");
        }


        internal static void ModifyEgorWithRevisionNoLoad(IDocumentSession session)
        {
            session.Advanced.Revisions.ForceRevisionCreationFor("employees/2-A");
        }

        internal static void ModifyEgorWithRevisionWithLoad(IDocumentSession session)
        {
            var egor = session.Load<Employee>("employees/2-A");
            session.Advanced.Revisions.ForceRevisionCreationFor(egor);
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [MemberData(nameof(SessionActions))]
        public void ShouldThrowConcurrencyException_WhenTrackedEntityWasChangedInBackgroundSession(Action<IDocumentSession> sessionAction)
        {
            using (var store = GetDocumentStore())
            {
                ShouldThrowConcurrencyException_WhenTrackedEntityWasChangedInBackgroundSessionInternal(sessionAction, store);
            }
        }

        private static void ShouldThrowConcurrencyException_WhenTrackedEntityWasChangedInBackgroundSessionInternal(Action<IDocumentSession> sessionAction, DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee
                {
                    FirstName = "Jerry"
                }, "employees/1-A");
                session.Store(new Employee
                {
                    FirstName = "Egor",
                    Address = new Address()
                    {
                        Street = "Ahad Ha'am"
                    }
                }, "employees/2-A");
                session.SaveChanges();

            }

            using (IDocumentSession session = store.OpenSession(new SessionOptions()
                   {
                       OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                   }))
            {
                var jerry = session.Load<Employee>("employees/1-A");

                sessionAction.Invoke(session);

                var expected = session.Advanced.GetChangeVectorFor(jerry);
                Assert.NotEmpty(expected);
                Assert.NotNull(expected);

                var actual = string.Empty;
                using (var s = store.OpenSession())
                {
                    var j = s.Load<Employee>("employees/1-A");
                    j.Address = new Address()
                    {
                        City = "Hadera"
                    };
                    s.SaveChanges();

                    actual = s.Advanced.GetChangeVectorFor(j);
                }

                    Assert.NotEmpty(actual);
                    Assert.NotNull(actual);

                    // this should throw concurrency exception for jerry
                    var e = Assert.Throws<Raven.Client.Exceptions.ConcurrencyException>(() => session.SaveChanges());

                    Assert.Contains("Document 'employees/1-A' has been modified", e.Message);
                    Assert.Equal("employees/1-A", e.Id);
                    Assert.Equal(actual, e.ActualChangeVector);
                Assert.Equal(expected, e.ExpectedChangeVector);
            }

            using (var session = store.OpenSession(new SessionOptions()
                   {
                       OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                   }))
            {
                var egor = session.Load<Employee>("employees/2-A");

                Assert.Equal("Egor", egor.FirstName);
                Assert.Equal("Ahad Ha'am", egor.Address.Street);

                var j = session.Load<Employee>("employees/1-A");
                Assert.Equal("Hadera", j.Address.City);
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldThrowConcurrencyException_WhenNoCommandsInSessionButTrackedEntityWasChangedInBackgroundSession(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                    session.SaveChanges();

                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    var jerry = session.Load<Employee>("employees/1-A");

                    ModifyEgorInSession(session);

                    var expected = session.Advanced.GetChangeVectorFor(jerry);
                    Assert.NotEmpty(expected);
                    Assert.NotNull(expected);

                    session.SaveChanges();

                    var actual = string.Empty;
                    using (var s = store.OpenSession())
                    {
                        var j = s.Load<Employee>("employees/1-A");
                        j.Address = new Address()
                        {
                            City = "Hadera"
                        };
                        s.SaveChanges();

                        actual = s.Advanced.GetChangeVectorFor(j);
                    }

                    Assert.NotEmpty(actual);
                    Assert.NotNull(actual);

                    // this should throw concurrency exception for jerry
                    var e = Assert.Throws<Raven.Client.Exceptions.ConcurrencyException>(() => session.SaveChanges());

                    Assert.Contains("Document 'employees/1-A' has been modified", e.Message);
                    Assert.Equal("employees/1-A", e.Id);
                    Assert.Equal(actual, e.ActualChangeVector);
                    Assert.Equal(expected, e.ExpectedChangeVector);
                }

                using (var session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                }))
                {
                    var egor = session.Load<Employee>("employees/2-A");

                    Assert.Equal("Egor", egor.FirstName);
                    Assert.Equal("Mul HaHof Village", egor.Address.Street);

                    var j = session.Load<Employee>("employees/1-A");
                    Assert.Equal("Hadera", j.Address.City);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldThrowConcurrencyException_WhenTrackedEntityIsNullButThenWasAddedInBackgroundSession(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                    session.SaveChanges();

                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    var jerry = session.Load<Employee>("employees/1-A");
                    Assert.Null(jerry);

                    ModifyEgorInSession(session);

                    var actual = string.Empty;
                    using (var s = store.OpenSession())
                    {
                        var j = new Employee
                        {
                            FirstName = "Jerry",
                            Address = new Address()
                            {
                                City = "Hadera"
                            }
                        };
                        s.Store(j, "employees/1-A");

                        s.SaveChanges();

                        actual = s.Advanced.GetChangeVectorFor(j);
                    }

                    Assert.NotEmpty(actual);
                    Assert.NotNull(actual);

                    var e = Assert.Throws<Raven.Client.Exceptions.ConcurrencyException>(() => session.SaveChanges());

                    Assert.Contains("Document 'employees/1-A' has been modified", e.Message);
                    Assert.Equal("employees/1-A", e.Id);
                    Assert.Equal(actual, e.ActualChangeVector);
                    Assert.Equal(string.Empty, e.ExpectedChangeVector);
                }

                using (var session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                }))
                {
                    var egor = session.Load<Employee>("employees/2-A");

                    Assert.Equal("Egor", egor.FirstName);
                    Assert.Equal("Ahad Ha'am", egor.Address.Street);

                    var j = session.Load<Employee>("employees/1-A");
                    Assert.Equal("Hadera", j.Address.City);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldThrowConcurrencyException_WhenTrackedEntityDeletedByEntityButThenWasEditedInBackgroundSession(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                    session.SaveChanges();

                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    var jerry = session.Load<Employee>("employees/1-A");
                    session.Delete<Employee>(jerry);
                    ModifyEgorInSession(session);

                    var expected = session.Advanced.GetChangeVectorFor(jerry);
                    Assert.NotEmpty(expected);
                    Assert.NotNull(expected);

                    var actual = string.Empty;
                    using (var s = store.OpenSession())
                    {
                        var j = s.Load<Employee>("employees/1-A");
                        j.Address = new Address()
                        {
                            City = "Hadera"
                        };
                        s.SaveChanges();

                        actual = s.Advanced.GetChangeVectorFor(j);
                    }

                    Assert.NotEmpty(actual);
                    Assert.NotNull(actual);

                    // this should throw concurrency exception for jerry via the DELETE command's own CV check
                    var e = Assert.Throws<Raven.Client.Exceptions.ConcurrencyException>(() => session.SaveChanges());

                    Assert.Contains($"Document employees/1-A has change vector {actual}, but Delete was called with change vector '{expected}'", e.Message);
                    Assert.Equal("employees/1-A", e.Id);
                    Assert.Equal(actual, e.ActualChangeVector);
                    Assert.Equal(expected, e.ExpectedChangeVector);
                }

                using (var session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                }))
                {
                    var egor = session.Load<Employee>("employees/2-A");

                    Assert.Equal("Egor", egor.FirstName);
                    Assert.Equal("Ahad Ha'am", egor.Address.Street);

                    var j = session.Load<Employee>("employees/1-A");
                    Assert.Equal("Hadera", j.Address.City);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldNotThrowConcurrencyException_WhenTrackedEntityDeletedByIdWithoutChangeVectorButThenWasNotEditedInBackgroundSession(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                    session.SaveChanges();

                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    var jerry = session.Load<Employee>("employees/1-A");
                    session.Delete("employees/1-A");
                    ModifyEgorInSession(session);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                }))
                {
                    var egor = session.Load<Employee>("employees/2-A");

                    Assert.Equal("Egor", egor.FirstName);
                    Assert.Equal("Mul HaHof Village", egor.Address.Street);

                    var j = session.Load<Employee>("employees/1-A");
                    Assert.Null(j);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldNotThrowConcurrencyException_WhenTrackedEntityDeletedByIdWithChangeVectorButThenWasNotEditedInBackgroundSession2(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                    session.SaveChanges();

                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    var jerry = session.Load<Employee>("employees/1-A");
                    var actual = session.Advanced.GetChangeVectorFor(jerry);
                    session.Delete("employees/1-A", "test");
                    ModifyEgorInSession(session);

                    var e = Assert.Throws<Raven.Client.Exceptions.ConcurrencyException>(() => session.SaveChanges());

                    Assert.Contains($"Document {e.Id} has change vector {e.ActualChangeVector}, but Delete was called with change vector '{e.ExpectedChangeVector}'. Optimistic concurrency violation, transaction will be aborted.", e.Message);


                    Assert.Equal("employees/1-A", e.Id);
                    Assert.Equal(actual, e.ActualChangeVector);
                    Assert.Equal("test", e.ExpectedChangeVector);
                }

                using (var session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                }))
                {
                    var egor = session.Load<Employee>("employees/2-A");

                    Assert.Equal("Egor", egor.FirstName);
                    Assert.Equal("Ahad Ha'am", egor.Address.Street);

                    var j = session.Load<Employee>("employees/1-A");
                    Assert.Equal("Jerry", j.FirstName);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldThrowConcurrencyException_WhenTrackedEntityDeletedByIdButThenWasEditedInBackgroundSession(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    var jerry = session.Load<Employee>("employees/1-A");
                    var expected = session.Advanced.GetChangeVectorFor(jerry);
                    session.Delete("employees/1-A");
                    ModifyEgorInSession(session);

                    var actual = string.Empty;
                    using (var s = store.OpenSession())
                    {
                        var j = s.Load<Employee>("employees/1-A");
                        j.Address = new Address()
                        {
                            City = "Hadera"
                        };
                        s.SaveChanges();

                        actual = s.Advanced.GetChangeVectorFor(j);
                    }

                    Assert.NotEmpty(actual);
                    Assert.NotNull(actual);

                    // this should throw concurrency exception for jerry
                    var e = Assert.Throws<Raven.Client.Exceptions.ConcurrencyException>(() => session.SaveChanges());

                    Assert.Contains("Document 'employees/1-A' has been modified", e.Message);
                    Assert.Equal("employees/1-A", e.Id);
                    Assert.Equal(actual, e.ActualChangeVector);
                    Assert.Equal(expected, e.ExpectedChangeVector);
                }

                using (var session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                }))
                {
                    var egor = session.Load<Employee>("employees/2-A");

                    Assert.Equal("Egor", egor.FirstName);
                    Assert.Equal("Ahad Ha'am", egor.Address.Street);

                    var j = session.Load<Employee>("employees/1-A");
                    Assert.Equal("Hadera", j.Address.City);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [MemberData(nameof(SessionActions))]
        public void ShouldThrowConcurrencyException_WhenTrackedEntityIncludedBySessionButThenWasEditedInBackgroundSession(Action<IDocumentSession> sessionAction)
        {
            using (var store = GetDocumentStore())
            {
                ShouldThrowConcurrencyException_WhenTrackedEntityIncludedBySessionButThenWasEditedInBackgroundSessionInternal(sessionAction, store);
            }
        }

        private static void ShouldThrowConcurrencyException_WhenTrackedEntityIncludedBySessionButThenWasEditedInBackgroundSessionInternal(Action<IDocumentSession> sessionAction, DocumentStore store)
        {
            string addressId;
            using (var session = store.OpenSession())
            {
                var address = new Address()
                {
                    City = "Harish",
                    Street = "Erets Rd"
                };
                session.Store(address);
                addressId = session.Advanced.GetDocumentId(address);
                Assert.NotNull(addressId);
                address.Id = addressId;
                session.Store(new Employee
                {
                    FirstName = "Jerry",
                    Address = address
                }, "employees/1-A");
                session.Store(new Employee
                {
                    FirstName = "Egor",
                    Address = new Address()
                    {
                        Street = "Ahad Ha'am"
                    }
                }, "employees/2-A");
                session.SaveChanges();

            }

            using (IDocumentSession session = store.OpenSession(new SessionOptions()
                   {
                       OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                   }))
            {
                var jerry = session.Include<Employee>(x => x.Address.Id).Load("employees/1-A");

                var numOfRequests = session.Advanced.NumberOfRequests;

                var address = session.Load<Address>(jerry.Address.Id);

                Assert.NotNull(address);
                Assert.Equal(numOfRequests, session.Advanced.NumberOfRequests);

                sessionAction.Invoke(session);

                var expected = session.Advanced.GetChangeVectorFor(address);
                Assert.NotEmpty(expected);
                Assert.NotNull(expected);

                var actual = string.Empty;
                using (var s = store.OpenSession())
                {
                    var a = s.Load<Address>(addressId);
                    a.City = "Hadera";
                    s.SaveChanges();

                    actual = s.Advanced.GetChangeVectorFor(a);
                }

                Assert.NotEmpty(actual);
                Assert.NotNull(actual);

                // this should throw concurrency exception for jerry
                var e = Assert.Throws<Raven.Client.Exceptions.ConcurrencyException>(() => session.SaveChanges());

                Assert.Contains($"Document '{addressId}' has been modified", e.Message);
                Assert.Equal(addressId, e.Id);
                Assert.Equal(actual, e.ActualChangeVector);
                Assert.Equal(expected, e.ExpectedChangeVector);
            }

            using (var session = store.OpenSession(new SessionOptions()
                   {
                       OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                   }))
            {
                var egor = session.Load<Employee>("employees/2-A");

                Assert.Equal("Egor", egor.FirstName);
                Assert.Equal("Ahad Ha'am", egor.Address.Street);

                var j = session.Load<Address>(addressId);
                Assert.Equal("Hadera", j.City);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void ShouldThrowConcurrencyException_WhenTrackedEntityEvictedFromTheSessionAndThenAddedBack()
        {
            // WritesAndReads mode includes write concurrency checks.
            // After evicting and re-storing with the same ID, the entity is treated as new (empty CV),
            // so the server rejects the put because the document already exists.
            using (var store = GetDocumentStore(Options.ForMode(RavenDatabaseMode.Single)))
            {
                using (var session = store.OpenSession())
                {
                    var address = new Address()
                    {
                        City = "Harish",
                        Street = "Erets Rd"
                    };
                    session.Store(address);
                    session.Store(new Employee
                    {
                        FirstName = "Jerry",
                        Address = address
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                       {
                           OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                       }))
                {
                    var jerry = session.Include<Employee>(x => x.Address.Id).Load("employees/1-A");
                    var address = session.Load<Address>(jerry.Address.Id);
                    Assert.NotNull(address);

                    ModifyEgorWithStoreOverwriteAfterLoadAndEvict(session);

                    // WritesAndReads enables write concurrency, evict+store treats entity as new → ConcurrencyException
                    Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void ShouldThrowConcurrencyException_WhenTrackedEntityReStored()
        {
            // WritesAndReads mode includes write concurrency checks.
            // Storing a new entity with an existing document ID (without loading first)
            // sends an empty change vector, which the server rejects because the document already exists.
            using (var store = GetDocumentStore(Options.ForMode(RavenDatabaseMode.Single)))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Mul HaHof Village"
                        }
                    }, "employees/2-A");

                    // WritesAndReads enables write concurrency, blind store (without load) → ConcurrencyException
                    Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldThrowConcurrencyException_WhenNonExistsEntityIncludedBySessionButThenWasAddedInBackgroundSession(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                string addressId = "addresses/1-A";
                var address = new Address()
                {
                    City = "Harish",
                    Street = "Erets Rd",
                    Id = addressId
                };

                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry",
                        Address = address
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                    session.SaveChanges();

                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    // this should put the include into missing ids
                    var jerry = session.Include<Employee>(x => x.Address.Id).Load("employees/1-A");

                    var numOfRequests = session.Advanced.NumberOfRequests;

                    Assert.Null(session.Load<Address>(jerry.Address.Id));
                    Assert.Equal(numOfRequests, session.Advanced.NumberOfRequests);

                    ModifyEgorInSession(session);

                    var actual = string.Empty;
                    using (var s = store.OpenSession())
                    {
                        s.Store(address, addressId);
                        s.SaveChanges();

                        actual = s.Advanced.GetChangeVectorFor(address);
                    }

                    Assert.NotEmpty(actual);
                    Assert.NotNull(actual);

                    // this should throw concurrency exception for jerry
                    var e = Assert.Throws<Raven.Client.Exceptions.ConcurrencyException>(() => session.SaveChanges());

                    Assert.Contains("Document 'addresses/1-A' has been modified", e.Message);
                    Assert.Equal(addressId, e.Id);
                    Assert.Equal(actual, e.ActualChangeVector);
                    Assert.Equal(string.Empty, e.ExpectedChangeVector);
                }

                using (var session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                }))
                {
                    var egor = session.Load<Employee>("employees/2-A");

                    Assert.Equal("Egor", egor.FirstName);
                    Assert.Equal("Ahad Ha'am", egor.Address.Street);

                    var j = session.Load<Address>(addressId);
                    Assert.NotNull(j);
                    Assert.Equal("Harish", j.City);
                }
            }
        }


        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldThrowConcurrencyException_WhenNonExistsEntityIncludedBySessionButThenWasEditedInBackgroundSession(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                string addressId = "addresses/1-A";
                var address = new Address()
                {
                    City = "Harish",
                    Street = "Erets Rd",
                    Id = addressId
                };
                using (var session = store.OpenSession())
                {
                    session.Store(address);
                    Assert.Equal(addressId, session.Advanced.GetDocumentId(address));
                    session.Store(new Employee
                    {
                        FirstName = "Jerry",
                        Address = address
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                    session.SaveChanges();

                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    // this should put the include into missing ids
                    var jerry = session.Include<Employee>(x => x.Address.Id).Load("employees/1-A");

                    var numOfRequests = session.Advanced.NumberOfRequests;
                    var adr = session.Load<Address>(jerry.Address.Id);
                    Assert.NotNull(adr);
                    Assert.Equal(numOfRequests, session.Advanced.NumberOfRequests);

                    ModifyEgorInSession(session);

                    var expected = session.Advanced.GetChangeVectorFor(adr);
                    Assert.NotEmpty(expected);
                    Assert.NotNull(expected);

                    var actual = string.Empty;
                    using (var s = store.OpenSession())
                    {
                        var adrInternal = s.Load<Address>(addressId);
                        adrInternal.City = "Hadera";
                        s.SaveChanges();

                        actual = s.Advanced.GetChangeVectorFor(adrInternal);
                    }

                    Assert.NotEmpty(actual);
                    Assert.NotNull(actual);

                    // this should throw concurrency exception for jerry
                    var e = Assert.Throws<Raven.Client.Exceptions.ConcurrencyException>(() => session.SaveChanges());

                    Assert.Contains("Document 'addresses/1-A' has been modified", e.Message);
                    Assert.Equal(addressId, e.Id);
                    Assert.Equal(actual, e.ActualChangeVector);
                    Assert.Equal(expected, e.ExpectedChangeVector);
                }

                using (var session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                }))
                {
                    var egor = session.Load<Employee>("employees/2-A");

                    Assert.Equal("Egor", egor.FirstName);
                    Assert.Equal("Ahad Ha'am", egor.Address.Street);

                    var j = session.Load<Address>(addressId);
                    Assert.NotNull(j);
                    Assert.Equal("Hadera", j.City);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldThrowConcurrencyException_WhenEntityIncludedByIdInSessionButThenWasEditedInBackgroundSession(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                string addressId;
                using (var session = store.OpenSession())
                {
                    var address = new Address()
                    {
                        City = "Harish",
                        Street = "Erets Rd"
                    };
                    session.Store(address);
                    addressId = session.Advanced.GetDocumentId(address);
                    Assert.NotNull(addressId);
                    address.Id = addressId;
                    session.Store(new Employee
                    {
                        FirstName = "Jerry",
                        Address = address
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    var jerry = session.Include<Employee>(x => x.Address.Id).Load("employees/1-A");
                    ModifyEgorInSession(session);

                    var inMemSes = ((InMemoryDocumentSessionOperations)session);
                    inMemSes.IncludedDocumentsById.TryGetValue(addressId, out var included);
                    Assert.NotNull(included);
                    var expected = included.ChangeVector;
                    Assert.NotEmpty(expected);
                    Assert.NotNull(expected);

                    var actual = string.Empty;
                    using (var s = store.OpenSession())
                    {
                        var a = s.Load<Address>(addressId);
                        a.City = "Hadera";
                        s.SaveChanges();

                        actual = s.Advanced.GetChangeVectorFor(a);
                    }

                    Assert.NotEmpty(actual);
                    Assert.NotNull(actual);

                    // this should throw concurrency exception for jerry
                    var e = Assert.Throws<Raven.Client.Exceptions.ConcurrencyException>(() => session.SaveChanges());

                    Assert.Contains("Document 'addresses/1-A' has been modified", e.Message);
                    Assert.Equal(addressId, e.Id);
                    Assert.Equal(actual, e.ActualChangeVector);
                    Assert.Equal(expected, e.ExpectedChangeVector);
                }

                using (var session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                }))
                {
                    var egor = session.Load<Employee>("employees/2-A");

                    Assert.Equal("Egor", egor.FirstName);
                    Assert.Equal("Ahad Ha'am", egor.Address.Street);

                    var j = session.Load<Address>(addressId);
                    Assert.Equal("Hadera", j.City);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldNotThrowConcurrencyException_WhenNonExistsEntityIncludedBySessionButThenWasNotAddedInBackgroundSession(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                string addressId = "addresses/1-A";
                var address = new Address()
                {
                    City = "Harish",
                    Street = "Erets Rd",
                    Id = addressId
                };

                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry",
                        Address = address
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                    session.SaveChanges();

                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    // this should put the include into missing ids
                    var jerry = session.Include<Employee>(x => x.Address.Id).Load("employees/1-A");

                    var numOfRequests = session.Advanced.NumberOfRequests;
                    Assert.Null(session.Load<Address>(jerry.Address.Id));
                    Assert.Equal(numOfRequests, session.Advanced.NumberOfRequests);

                    ModifyEgorInSession(session);

                   session.SaveChanges();

                }

                using (var session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                }))
                {
                    var egor = session.Load<Employee>("employees/2-A");

                    Assert.Equal("Egor", egor.FirstName);
                    Assert.Equal("Mul HaHof Village", egor.Address.Street);

                    var j = session.Load<Address>(addressId);
                    Assert.Null(j);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldNotThrowConcurrencyException_WhenTrackedEntityEvictedFromSession(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    var jerry = session.Load<Employee>("employees/1-A");
                    var expected = session.Advanced.GetChangeVectorFor(jerry);

                    ModifyEgorInSession(session);

                    // Evict Jerry from the session - this should stop tracking him
                    session.Advanced.Evict(jerry);

                    var actual = string.Empty;
                    using (var s = store.OpenSession())
                    {
                        var j = s.Load<Employee>("employees/1-A");
                        j.Address = new Address()
                        {
                            City = "Hadera"
                        };
                        s.SaveChanges();

                        actual = s.Advanced.GetChangeVectorFor(j);
                    }

                    Assert.NotEmpty(actual);
                    Assert.NotNull(actual);
                    Assert.NotEqual(expected, actual);

                    // Should NOT throw concurrency exception because Jerry was evicted
                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                }))
                {
                    var egor = session.Load<Employee>("employees/2-A");
                    Assert.Equal("Egor", egor.FirstName);
                    Assert.Equal("Mul HaHof Village", egor.Address.Street);

                    var j = session.Load<Employee>("employees/1-A");
                    Assert.Equal("Hadera", j.Address.City);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task ShouldNotThrowConcurrencyException_WhenIncludedDocumentEvictedFromSessionAsync(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                string addressId;
                using (var session = store.OpenAsyncSession(new SessionOptions()
                       {
                           OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                       }))
                {
                    var address = new Address()
                    {
                        City = "Harish",
                        Street = "Erets Rd"
                    };
                 await   session.StoreAsync(address);
                    addressId = session.Advanced.GetDocumentId(address);
                    Assert.NotNull(addressId);
                    address.Id = addressId;
                    await session.StoreAsync(new Employee
                    {
                        FirstName = "Jerry",
                        Address = address
                    }, "employees/1-A");
                    await session.StoreAsync(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                 await   session.SaveChangesAsync();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    var jerry = session.Include<Employee>(x => x.Address.Id).Load("employees/1-A");

                    var numOfRequests = session.Advanced.NumberOfRequests;
                    var address = session.Load<Address>(jerry.Address.Id);
                    Assert.NotNull(address);
                    Assert.Equal(numOfRequests, session.Advanced.NumberOfRequests);

                    var expected = session.Advanced.GetChangeVectorFor(address);

                    // Evict the included address from tracking
                    session.Advanced.Evict(address);

                    ModifyEgorInSession(session);

                    var actual = string.Empty;
                    using (var s = store.OpenSession())
                    {
                        var a = s.Load<Address>(addressId);
                        a.City = "Hadera";
                        s.SaveChanges();

                        actual = s.Advanced.GetChangeVectorFor(a);
                    }

                    Assert.NotEmpty(actual);
                    Assert.NotNull(actual);
                    Assert.NotEqual(expected, actual);

                    // Should NOT throw concurrency exception because address was evicted
                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                }))
                {
                    var egor = session.Load<Employee>("employees/2-A");
                    Assert.Equal("Egor", egor.FirstName);
                    Assert.Equal("Mul HaHof Village", egor.Address.Street);

                    var j = session.Load<Address>(addressId);
                    Assert.Equal("Hadera", j.City);
                }
            }
        }
        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldNotThrowConcurrencyException_WhenIncludedDocumentEvictedFromSession(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                string addressId;
                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                       {
                           OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                       }))
                {
                    var address = new Address()
                    {
                        City = "Harish",
                        Street = "Erets Rd"
                    };
                    session.Store(address);
                    addressId = session.Advanced.GetDocumentId(address);
                    Assert.NotNull(addressId);
                    address.Id = addressId;
                    session.Store(new Employee
                    {
                        FirstName = "Jerry",
                        Address = address
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    var jerry = session.Include<Employee>(x => x.Address.Id).Load("employees/1-A");

                    var numOfRequests = session.Advanced.NumberOfRequests;
                    var address = session.Load<Address>(jerry.Address.Id);
                    Assert.NotNull(address);
                    Assert.Equal(numOfRequests, session.Advanced.NumberOfRequests);

                    var expected = session.Advanced.GetChangeVectorFor(address);

                    // Evict the included address from tracking
                    session.Advanced.Evict(address);

                    ModifyEgorInSession(session);

                    var actual = string.Empty;
                    using (var s = store.OpenSession())
                    {
                        var a = s.Load<Address>(addressId);
                        a.City = "Hadera";
                        s.SaveChanges();

                        actual = s.Advanced.GetChangeVectorFor(a);
                    }

                    Assert.NotEmpty(actual);
                    Assert.NotNull(actual);
                    Assert.NotEqual(expected, actual);

                    // Should NOT throw concurrency exception because address was evicted
                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                }))
                {
                    var egor = session.Load<Employee>("employees/2-A");
                    Assert.Equal("Egor", egor.FirstName);
                    Assert.Equal("Mul HaHof Village", egor.Address.Street);

                    var j = session.Load<Address>(addressId);
                    Assert.Equal("Hadera", j.City);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldNotThrowConcurrencyException_WhenEntityModifiedInBackgroundSessionButThenRefreshed(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry",
                        Address = new Address()
                        {
                            City = "Hadera"
                        }
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    var jerry = session.Load<Employee>("employees/1-A");
                    var egorFromSession = session.Load<Employee>("employees/2-A");

                    // Modify Jerry in background session
                    using (var s = store.OpenSession())
                    {
                        var j = s.Load<Employee>("employees/1-A");
                        j.Address = new Address()
                        {
                            City = "Tel Aviv"
                        };
                        s.SaveChanges();
                    }

                    // Refresh Jerry to get the latest version
                    session.Advanced.Refresh(jerry);

                    // Verify Jerry has updated data
                    Assert.Equal("Tel Aviv", jerry.Address.City);

                    ModifyEgorInSession(session);

                    //should work since we refreshed jerry after modification in background session
                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                }))
                {
                    var egor = session.Load<Employee>("employees/2-A");
                    Assert.Equal("Egor", egor.FirstName);
                    Assert.Equal("Mul HaHof Village", egor.Address.Street);

                    var j = session.Load<Employee>("employees/1-A");
                    Assert.Equal("Jerry", j.FirstName);
                    Assert.Equal("Tel Aviv", j.Address.City);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldThrowConcurrencyException_WhenRefreshedEntityModifiedInBackgroundSession(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry",
                        Address = new Address()
                        {
                            City = "Hadera"
                        }
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    var jerry = session.Load<Employee>("employees/1-A");
                    var egorFromSession = session.Load<Employee>("employees/2-A");

                    // Modify Jerry in background session
                    using (var s = store.OpenSession())
                    {
                        var j = s.Load<Employee>("employees/1-A");
                        j.Address = new Address()
                        {
                            City = "Tel Aviv"
                        };
                        s.SaveChanges();
                    }

                    // Refresh Jerry to get the latest version
                    session.Advanced.Refresh(jerry);

                    // Verify Jerry has updated data
                    Assert.Equal("Tel Aviv", jerry.Address.City);

                    var expectedJerryCv = session.Advanced.GetChangeVectorFor(jerry);

                    ModifyEgorInSession(session);

                    // Now modify Jerry again in background session
                    var actualJerryCv = string.Empty;
                    using (var s = store.OpenSession())
                    {
                        var j = s.Load<Employee>("employees/1-A");
                        j.FirstName = "Jeremy";
                        s.SaveChanges();

                        actualJerryCv = s.Advanced.GetChangeVectorFor(j);
                    }

                    Assert.NotEmpty(actualJerryCv);
                    Assert.NotNull(actualJerryCv);
                    Assert.NotEqual(expectedJerryCv, actualJerryCv);

                    // Should throw concurrency exception for Jerry (refreshed entity was modified)
                    var e = Assert.Throws<Raven.Client.Exceptions.ConcurrencyException>(() => session.SaveChanges());

                    Assert.Contains("Document 'employees/1-A' has been modified", e.Message);
                    Assert.Equal("employees/1-A", e.Id);
                    Assert.Equal(actualJerryCv, e.ActualChangeVector);
                    Assert.Equal(expectedJerryCv, e.ExpectedChangeVector);
                }

                using (var session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                }))
                {
                    var egor = session.Load<Employee>("employees/2-A");
                    Assert.Equal("Egor", egor.FirstName);
                    Assert.Equal("Ahad Ha'am", egor.Address.Street);

                    var j = session.Load<Employee>("employees/1-A");
                    Assert.Equal("Jeremy", j.FirstName);
                    Assert.Equal("Tel Aviv", j.Address.City);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldUpdateChangeVector_WhenRefreshingMultipleEntities(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee { FirstName = "Jerry" }, "employees/1-A");
                    session.Store(new Employee { FirstName = "Egor" }, "employees/2-A");
                    session.Store(new Employee { FirstName = "Alice" }, "employees/3-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    var jerry = session.Load<Employee>("employees/1-A");
                    var egor = session.Load<Employee>("employees/2-A");
                    var alice = session.Load<Employee>("employees/3-A");

                    var originalJerryCv = session.Advanced.GetChangeVectorFor(jerry);
                    var originalEgorCv = session.Advanced.GetChangeVectorFor(egor);
                    var originalAliceCv = session.Advanced.GetChangeVectorFor(alice);

                    // Modify entities in background session
                    using (var s = store.OpenSession())
                    {
                        var j = s.Load<Employee>("employees/1-A");
                        j.FirstName = "Jeremy";

                        var e = s.Load<Employee>("employees/2-A");
                        e.FirstName = "Greg";

                        s.SaveChanges();
                    }

                    // Refresh multiple entities at once
                    session.Advanced.Refresh<Employee>(new List<Employee>() { jerry, egor, alice });

                    // Verify change vectors updated for modified entities
                    var newJerryCv = session.Advanced.GetChangeVectorFor(jerry);
                    var newEgorCv = session.Advanced.GetChangeVectorFor(egor);
                    var newAliceCv = session.Advanced.GetChangeVectorFor(alice);

                    Assert.NotEqual(originalJerryCv, newJerryCv);
                    Assert.NotEqual(originalEgorCv, newEgorCv);
                    Assert.Equal(originalAliceCv, newAliceCv); // Alice wasn't modified

                    // Verify data was refreshed
                    Assert.Equal("Jeremy", jerry.FirstName);
                    Assert.Equal("Greg", egor.FirstName);
                    Assert.Equal("Alice", alice.FirstName);

                    session.SaveChanges(); // Should not throw
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldThrowException_WhenEvictingEntityDuringOnBeforeStore(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee { FirstName = "Jerry" }, "employees/1-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    var jerry = session.Load<Employee>("employees/1-A");

                    session.Advanced.OnBeforeStore += (sender, args) =>
                    {
                        // This should throw because we can't evict during OnBeforeStore
                        Assert.Throws<InvalidOperationException>(() => session.Advanced.Evict(args.Entity));
                    };

                    jerry.FirstName = "Jeremy";
                    session.SaveChanges();
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldHandleEvictAndReload_WithTrackingEnabled(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry",
                        Address = new Address { City = "Hadera" }
                    }, "employees/1-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    var jerry = session.Load<Employee>("employees/1-A");
                    var originalCv = session.Advanced.GetChangeVectorFor(jerry);

                    // Modify in background
                    using (var s = store.OpenSession())
                    {
                        var j = s.Load<Employee>("employees/1-A");
                        j.Address.City = "Tel Aviv";
                        s.SaveChanges();
                    }

                    // Evict the entity
                    session.Advanced.Evict(jerry);

                    // Reload - should get fresh data from server
                    var jerryReloaded = session.Load<Employee>("employees/1-A");
                    var newCv = session.Advanced.GetChangeVectorFor(jerryReloaded);

                    Assert.NotSame(jerry, jerryReloaded); // Different instances
                    Assert.NotEqual(originalCv, newCv);
                    Assert.Equal("Tel Aviv", jerryReloaded.Address.City);

                    // Make another background change
                    using (var s = store.OpenSession())
                    {
                        var j = s.Load<Employee>("employees/1-A");
                        j.FirstName = "Jeremy";
                        s.SaveChanges();
                    }


                    // The entity tracking should have the new change vector
                    var e = Assert.Throws<Raven.Client.Exceptions.ConcurrencyException>(() => session.SaveChanges());

                    Assert.Contains("Document 'employees/1-A' has been modified", e.Message);
                    Assert.Equal("employees/1-A", e.Id);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldHandleEvictAndReload_WithTrackingEnabled2(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry",
                        Address = new Address { City = "Hadera" }
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    var jerry = session.Load<Employee>("employees/1-A");
                    var originalCv = session.Advanced.GetChangeVectorFor(jerry);
                    ModifyEgorInSession(session);

                    // Modify in background
                    using (var s = store.OpenSession())
                    {
                        var j = s.Load<Employee>("employees/1-A");
                        j.Address.City = "Tel Aviv";
                        s.SaveChanges();
                    }

                    // Evict the entity
                    session.Advanced.Evict(jerry);

                    // Should not throw because entities were evicted
                    session.SaveChanges();
                }
                using (var session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                }))
                {
                    var egor = session.Load<Employee>("employees/2-A");
                    Assert.Equal("Egor", egor.FirstName);
                    Assert.Equal("Mul HaHof Village", egor.Address.Street);

                    var j = session.Load<Employee>("employees/1-A");
                    Assert.Equal("Jerry", j.FirstName);
                    Assert.Equal("Tel Aviv", j.Address.City);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldHandleEvictAndReload_WithTrackingEnabled3(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry",
                        Address = new Address { City = "Hadera" }
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    var jerry = session.Load<Employee>("employees/1-A");
                    var originalCv = session.Advanced.GetChangeVectorFor(jerry);
                    ModifyEgorInSession(session);

                    // Evict the entity
                    session.Advanced.Evict(jerry);

                    // Modify in background
                    using (var s = store.OpenSession())
                    {
                        var j = s.Load<Employee>("employees/1-A");
                        j.Address.City = "Tel Aviv";
                        s.SaveChanges();
                    }

                    // Should not throw because entities were evicted
                    session.SaveChanges();
                }
                using (var session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                }))
                {
                    var egor = session.Load<Employee>("employees/2-A");
                    Assert.Equal("Egor", egor.FirstName);
                    Assert.Equal("Mul HaHof Village", egor.Address.Street);

                    var j = session.Load<Employee>("employees/1-A");
                    Assert.Equal("Jerry", j.FirstName);
                    Assert.Equal("Tel Aviv", j.Address.City);
                }
            }
        }



        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldNotTrack_AfterEvictingAllLoadedEntities(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee { FirstName = "Jerry" }, "employees/1-A");
                    session.Store(new Employee { FirstName = "Egor" }, "employees/2-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    var jerry = session.Load<Employee>("employees/1-A");
                    var egor = session.Load<Employee>("employees/2-A");

                    Assert.Equal(2, ((InMemoryDocumentSessionOperations)session).NumberOfEntitiesInUnitOfWork);

                    // Evict both entities
                    session.Advanced.Evict(jerry);
                    session.Advanced.Evict(egor);

                    Assert.Equal(0, ((InMemoryDocumentSessionOperations)session).NumberOfEntitiesInUnitOfWork);

                    // Modify both in background
                    using (var s = store.OpenSession())
                    {
                        var j = s.Load<Employee>("employees/1-A");
                        j.FirstName = "Jeremy";

                        var e = s.Load<Employee>("employees/2-A");
                        e.FirstName = "Greg";

                        s.SaveChanges();
                    }

                    // Should not throw because entities were evicted
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var jerry = session.Load<Employee>("employees/1-A");
                    var egor = session.Load<Employee>("employees/2-A");

                    Assert.Equal("Jeremy", jerry.FirstName);
                    Assert.Equal("Greg", egor.FirstName);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldRefreshAndMaintainTracking_WhenEntityModifiedLocally(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry",
                        Address = new Address { City = "Hadera" }
                    }, "employees/1-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    var jerry = session.Load<Employee>("employees/1-A");

                    // Modify locally
                    jerry.Address.Street = "Local Street";

                    // Refresh should discard local changes and get server data
                    session.Advanced.Refresh(jerry);

                    // Local changes should be lost
                    Assert.Null(jerry.Address.Street);
                    Assert.Equal("Hadera", jerry.Address.City);

                    // Entity should still be tracked
                    Assert.True(session.Advanced.IsLoaded("employees/1-A"));

                    // Make a change and save
                    jerry.FirstName = "Jeremy";
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var jerry = session.Load<Employee>("employees/1-A");
                    Assert.Equal("Jeremy", jerry.FirstName);
                }
            }
        }


        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldTrackExternalEntity_WhenAddedViaTrackEntityMethod(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    // Load Jerry to get his change vector
                    var jerry = session.Load<Employee>("employees/1-A");
                    var jerryChangeVector = session.Advanced.GetChangeVectorFor(jerry);

                    // Create an external entity (from another session or source)
                    Employee externalEmployee;
                    string externalChangeVector;
                    using (var externalSession = store.OpenSession())
                    {
                        externalEmployee = externalSession.Load<Employee>("employees/2-A");
                        externalChangeVector = externalSession.Advanced.GetChangeVectorFor(externalEmployee);
                    }

                    // Register the external entity using TrackEntity
                    var inMemSession = (InMemoryDocumentSessionOperations)session;
                    var documentInfo = new DocumentInfo
                    {
                        Id = "employees/2-A",
                        Entity = externalEmployee,
                        ChangeVector = externalChangeVector,
                        Document = null,
                        Metadata = inMemSession.Context.ReadObject(new DynamicJsonValue
                        {
                            [Raven.Client.Constants.Documents.Metadata.Collection] = "Employees",
                            [Raven.Client.Constants.Documents.Metadata.ChangeVector] = externalChangeVector
                        }, "employees/2-A")
                    };

                    inMemSession.RegisterExternalLoadedIntoTheSession(documentInfo);

                    // Verify tracking
                    Assert.True(inMemSession.TrackedEntities.TryGetValue("employees/2-A", out var trackedCv));
                    Assert.Equal(externalChangeVector, trackedCv);

                    // Modify Egor in background (the externally tracked entity)
                    var actualEgorCv = string.Empty;
                    using (var s = store.OpenSession())
                    {
                        var e = s.Load<Employee>("employees/2-A");
                        e.Address = new Address() { Street = "Mul HaHof Village" };
                        s.SaveChanges();
                        actualEgorCv = s.Advanced.GetChangeVectorFor(e);
                    }

                    // This should throw concurrency exception for Egor (the externally tracked entity)
                    // Note: external entities have Document=null, so EntityChanged returns true and a PUT is generated.
                    // The concurrency error comes from the PUT command, not the BatchTrackChangesCommand.
                    var ex = Assert.Throws<Raven.Client.Exceptions.ConcurrencyException>(() => session.SaveChanges());

                    Assert.Contains($"Document employees/2-A has change vector {actualEgorCv}, but Put was called with change vector {externalChangeVector}", ex.Message);
                    Assert.Equal("employees/2-A", ex.Id);
                    Assert.Equal(actualEgorCv, ex.ActualChangeVector);
                    Assert.Equal(externalChangeVector, ex.ExpectedChangeVector);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldNotThrowConcurrencyException_WhenExternallyTrackedEntityNotModified(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    // Load Jerry normally
                    var jerry = session.Load<Employee>("employees/1-A");

                    // Create an external entity (from another session)
                    Employee externalEmployee;
                    string externalChangeVector;
                    using (var externalSession = store.OpenSession())
                    {
                        externalEmployee = externalSession.Load<Employee>("employees/2-A");
                        externalChangeVector = externalSession.Advanced.GetChangeVectorFor(externalEmployee);
                    }

                    // Register the external entity using TrackEntity
                    var inMemSession = (InMemoryDocumentSessionOperations)session;
                    var documentInfo = new DocumentInfo
                    {
                        Id = "employees/2-A",
                        Entity = externalEmployee,
                        ChangeVector = externalChangeVector,
                        Document = null,
                        Metadata = inMemSession.Context.ReadObject(new DynamicJsonValue
                        {
                            [Raven.Client.Constants.Documents.Metadata.Collection] = "Employees",
                            [Raven.Client.Constants.Documents.Metadata.ChangeVector] = externalChangeVector
                        }, "employees/2-A")
                    };

                    using var newInstance = inMemSession.JsonConverter.ToBlittable(externalEmployee, null);

                    documentInfo.Document = newInstance;
                    inMemSession.TrackEntity<Employee>(documentInfo);

                    // Modify only Jerry in background
                    using (var s = store.OpenSession())
                    {
                        var j = s.Load<Employee>("employees/1-A");
                        j.Address = new Address() { City = "Hadera" };
                        s.SaveChanges();
                    }

                    // Should throw concurrency exception only for Jerry, not Egor
                    var ex = Assert.Throws<Raven.Client.Exceptions.ConcurrencyException>(() => session.SaveChanges());

                    Assert.Contains("Document 'employees/1-A' has been modified", ex.Message);
                    Assert.Equal("employees/1-A", ex.Id);
                }

                using (var session = store.OpenSession())
                {
                    var jerry = session.Load<Employee>("employees/1-A");
                    var egor = session.Load<Employee>("employees/2-A");

                    Assert.Equal("Hadera", jerry.Address.City);
                    Assert.Equal("Ahad Ha'am", egor.Address.Street);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldThrowException_WhenRegisteringExternalEntityWithDifferentInstanceForSameId(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/1-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    // Load Jerry normally
                    var jerry = session.Load<Employee>("employees/1-A");
                    var jerryChangeVector = session.Advanced.GetChangeVectorFor(jerry);

                    // Try to register a different instance for the same ID
                    var inMemSession = (InMemoryDocumentSessionOperations)session;
                    var documentInfo = new DocumentInfo
                    {
                        Id = "employees/1-A",
                        Entity = new Employee { FirstName = "Different Jerry" },
                        ChangeVector = jerryChangeVector,
                        Document = null,
                        Metadata = inMemSession.Context.ReadObject(new DynamicJsonValue
                        {
                            [Raven.Client.Constants.Documents.Metadata.Collection] = "Employees",
                            [Raven.Client.Constants.Documents.Metadata.ChangeVector] = jerryChangeVector
                        }, "employees/1-A")
                    };

                    // Should throw because we already have a different instance tracked for this ID
                    var ex = Assert.Throws<InvalidOperationException>(() =>
                        inMemSession.RegisterExternalLoadedIntoTheSession(documentInfo));

                    Assert.Contains("is already in the session with a different entity instance", ex.Message);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldAllowReregisteringSameEntityInstance(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/1-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    // Load Jerry normally
                    var jerry = session.Load<Employee>("employees/1-A");
                    var jerryChangeVector = session.Advanced.GetChangeVectorFor(jerry);

                    // Try to re-register the same instance
                    var inMemSession = (InMemoryDocumentSessionOperations)session;
                    var documentInfo = new DocumentInfo
                    {
                        Id = "employees/1-A",
                        Entity = jerry,
                        ChangeVector = jerryChangeVector,
                        Document = null,
                        Metadata = inMemSession.Context.ReadObject(new DynamicJsonValue
                        {
                            [Raven.Client.Constants.Documents.Metadata.Collection] = "Employees",
                            [Raven.Client.Constants.Documents.Metadata.ChangeVector] = jerryChangeVector
                        }, "employees/1-A")
                    };

                    // Should not throw - same instance is ok
                    inMemSession.RegisterExternalLoadedIntoTheSession(documentInfo);

                    // Verify it's still tracked
                    Assert.True(inMemSession.TrackedEntities.TryGetValue("employees/1-A", out var trackedCv));
                    Assert.Equal(jerryChangeVector, trackedCv);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldTrackMultipleExternalEntities_AndDetectConcurrencyViolations(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee { FirstName = "Jerry" }, "employees/1-A");
                    session.Store(new Employee { FirstName = "Egor" }, "employees/2-A");
                    session.Store(new Employee { FirstName = "Alice" }, "employees/3-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    // Load Jerry normally
                    var jerry = session.Load<Employee>("employees/1-A");

                    // Register Egor and Alice as external entities
                    var inMemSession = (InMemoryDocumentSessionOperations)session;

                    // Register Egor
                    Employee egor;
                    string egorCv;
                    using (var externalSession = store.OpenSession())
                    {
                        egor = externalSession.Load<Employee>("employees/2-A");
                        egorCv = externalSession.Advanced.GetChangeVectorFor(egor);
                    }

                    var egorDocInfo = new DocumentInfo
                    {
                        Id = "employees/2-A",
                        Entity = egor,
                        ChangeVector = egorCv,
                        Document = null,
                        Metadata = inMemSession.Context.ReadObject(new DynamicJsonValue
                        {
                            [Raven.Client.Constants.Documents.Metadata.Collection] = "Employees",
                            [Raven.Client.Constants.Documents.Metadata.ChangeVector] = egorCv
                        }, "employees/2-A")
                    };
                    inMemSession.RegisterExternalLoadedIntoTheSession(egorDocInfo);

                    // Register Alice
                    Employee alice;
                    string aliceCv;
                    using (var externalSession = store.OpenSession())
                    {
                        alice = externalSession.Load<Employee>("employees/3-A");
                        aliceCv = externalSession.Advanced.GetChangeVectorFor(alice);
                    }

                    var aliceDocInfo = new DocumentInfo
                    {
                        Id = "employees/3-A",
                        Entity = alice,
                        ChangeVector = aliceCv,
                        Document = null,
                        Metadata = inMemSession.Context.ReadObject(new DynamicJsonValue
                        {
                            [Raven.Client.Constants.Documents.Metadata.Collection] = "Employees",
                            [Raven.Client.Constants.Documents.Metadata.ChangeVector] = aliceCv
                        }, "employees/3-A")
                    };
                    inMemSession.RegisterExternalLoadedIntoTheSession(aliceDocInfo);

                    // Verify all are tracked
                    Assert.True(inMemSession.TrackedEntities.TryGetValue("employees/1-A", out _));
                    Assert.True(inMemSession.TrackedEntities.TryGetValue("employees/2-A", out _));
                    Assert.True(inMemSession.TrackedEntities.TryGetValue("employees/3-A", out _));

                    // Modify Alice in background
                    var actualAliceCv = string.Empty;
                    using (var s = store.OpenSession())
                    {
                        var a = s.Load<Employee>("employees/3-A");
                        a.FirstName = "Alicia";
                        s.SaveChanges();
                        actualAliceCv = s.Advanced.GetChangeVectorFor(a);
                    }

                    // Should throw concurrency exception for Alice
                    // Note: external entities have Document=null, so a PUT is generated and the error comes from the PUT command.
                    var ex = Assert.Throws<Raven.Client.Exceptions.ConcurrencyException>(() => session.SaveChanges());

                    Assert.Contains($"Document employees/3-A has change vector {actualAliceCv}, but Put was called with change vector {aliceCv}", ex.Message);
                    Assert.Equal("employees/3-A", ex.Id);
                    Assert.Equal(actualAliceCv, ex.ActualChangeVector);
                    Assert.Equal(aliceCv, ex.ExpectedChangeVector);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldNotTrackExternalEntity_WhenNoTrackingModeEnabled(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee { FirstName = "Jerry" }, "employees/1-A");
                    session.Store(new Employee { FirstName = "Egor" }, "employees/2-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    NoTracking = true,
                }))
                {
                    // Try to register an external entity in NoTracking session
                    var inMemSession = (InMemoryDocumentSessionOperations)session;

                    Employee egor;
                    string egorCv;
                    using (var externalSession = store.OpenSession())
                    {
                        egor = externalSession.Load<Employee>("employees/2-A");
                        egorCv = externalSession.Advanced.GetChangeVectorFor(egor);
                    }

                    var documentInfo = new DocumentInfo
                    {
                        Id = "employees/2-A",
                        Entity = egor,
                        ChangeVector = egorCv,
                        Document = null,
                        Metadata = inMemSession.Context.ReadObject(new DynamicJsonValue
                        {
                            [Raven.Client.Constants.Documents.Metadata.Collection] = "Employees",
                            [Raven.Client.Constants.Documents.Metadata.ChangeVector] = egorCv
                        }, "employees/2-A")
                    };

                    // Should not track in NoTracking mode
                    inMemSession.RegisterExternalLoadedIntoTheSession(documentInfo);

                    // Verify it's not tracked
                    Assert.False(inMemSession.TrackedEntities.TryGetValue("employees/2-A", out _));

                    // Modify in background - should NOT throw concurrency exception
                    using (var s = store.OpenSession())
                    {
                        var e = s.Load<Employee>("employees/2-A");
                        e.FirstName = "Greg";
                        s.SaveChanges();
                    }

                    session.SaveChanges(); // Should not throw
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldRemoveFromIncluded_WhenRegisteringExternalEntityThatWasIncluded(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                string addressId;
                using (var session = store.OpenSession())
                {
                    var address = new Address { City = "Harish", Street = "Erets Rd" };
                    session.Store(address);
                    addressId = session.Advanced.GetDocumentId(address);
                    address.Id = addressId;

                    session.Store(new Employee
                    {
                        FirstName = "Jerry",
                        Address = address
                    }, "employees/1-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    // Load with include
                    var jerry = session.Include<Employee>(x => x.Address.Id).Load("employees/1-A");

                    var inMemSession = (InMemoryDocumentSessionOperations)session;

                    // Verify address is in included documents
                    Assert.True(inMemSession.IncludedDocumentsById.ContainsKey(addressId));
                    Assert.True(inMemSession.TrackedEntities.TryGetValue(addressId, out _));

                    // Now register the same address as external entity
                    Address externalAddress;
                    string addressCv;
                    using (var externalSession = store.OpenSession())
                    {
                        externalAddress = externalSession.Load<Address>(addressId);
                        addressCv = externalSession.Advanced.GetChangeVectorFor(externalAddress);
                    }

                    var documentInfo = new DocumentInfo
                    {
                        Id = addressId,
                        Entity = externalAddress,
                        ChangeVector = addressCv,
                        Document = null,
                        Metadata = inMemSession.Context.ReadObject(new DynamicJsonValue
                        {
                            [Raven.Client.Constants.Documents.Metadata.Collection] = "Addresses",
                            [Raven.Client.Constants.Documents.Metadata.ChangeVector] = addressCv
                        }, addressId)
                    };

                    inMemSession.RegisterExternalLoadedIntoTheSession(documentInfo);

                    // Verify it's no longer in included documents but is tracked
                    Assert.False(inMemSession.IncludedDocumentsById.ContainsKey(addressId));
                    Assert.True(inMemSession.TrackedEntities.TryGetValue(addressId, out var trackedCv));
                    Assert.Equal(addressCv, trackedCv);

                    // Modify address in background
                    var actualAddressCv = string.Empty;
                    using (var s = store.OpenSession())
                    {
                        var a = s.Load<Address>(addressId);
                        a.City = "Hadera";
                        s.SaveChanges();
                        actualAddressCv = s.Advanced.GetChangeVectorFor(a);
                    }

                    // Should throw concurrency exception for the address
                    // Note: external entities have Document=null, so a PUT is generated and the error comes from the PUT command.
                    var ex = Assert.Throws<Raven.Client.Exceptions.ConcurrencyException>(() => session.SaveChanges());

                    Assert.Contains($"Document {addressId} has change vector {actualAddressCv}, but Put was called with change vector {addressCv}", ex.Message);
                    Assert.Equal(addressId, ex.Id);
                    Assert.Equal(actualAddressCv, ex.ActualChangeVector);
                    Assert.Equal(addressCv, ex.ExpectedChangeVector);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldThrowConcurrencyException_WhenTrackedEntityLoadedButThenWasDeletedInBackgroundSession(Options options)
        {
            ShouldThrowConcurrencyException_WhenTrackedEntityLoadedButThenWasDeletedInternal(options, deleteById: false);
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public void ShouldThrowConcurrencyException_WhenTrackedEntityLoadedButThenWasDeletedByIdInBackgroundSession(Options options)
        {
            ShouldThrowConcurrencyException_WhenTrackedEntityLoadedButThenWasDeletedInternal(options, deleteById: true);
        }

        private void ShouldThrowConcurrencyException_WhenTrackedEntityLoadedButThenWasDeletedInternal(Options options, bool deleteById)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                       {
                           OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                       }))
                {
                    var jerry = session.Load<Employee>("employees/1-A");
                    //session.Delete("employees/1-A");
                    // delete / delete by id (both here and in background)

                    var expected = session.Advanced.GetChangeVectorFor(jerry);
                    using (var s = store.OpenSession())
                    {
                        if (deleteById == false)
                        {
                            var j = s.Load<Employee>("employees/1-A");
                            s.Delete(j);
                        }
                        else
                        {
                            s.Delete("employees/1-A");
                        }

                        s.SaveChanges();
                    }

                    // this should throw concurrency exception for jerry
                    var e = Assert.Throws<Raven.Client.Exceptions.ConcurrencyException>(() => session.SaveChanges());

                    Assert.Contains($"Document 'employees/1-A' has been modified since it was loaded. The expected change vector '{expected}' does not match the current change vector 'string.Empty'.", e.Message);
                    Assert.Equal("employees/1-A", e.Id);
                    Assert.Empty(e.ActualChangeVector);
                    Assert.Equal(expected, e.ExpectedChangeVector);
                }

                using (var session = store.OpenSession(new SessionOptions()
                       {
                           OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                       }))
                {
                    var egor = session.Load<Employee>("employees/2-A");

                    Assert.Equal("Egor", egor.FirstName);
                    Assert.Equal("Ahad Ha'am", egor.Address.Street);

                    var j = session.Load<Employee>("employees/1-A");
                    Assert.Null(j);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [true, true])]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [true, false])]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [false, true])]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [false, false])]
        public void ShouldThrowConcurrencyException_WhenTrackedEntityLoadedButThenWasDeletedInBackgroundSession2(Options options, bool deleteById, bool deleteByIdInBackgroundSession)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                    session.SaveChanges();
                }

                using (IDocumentSession session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
                }))
                {
                    string expected = null;
                    if (deleteByIdInBackgroundSession == false)
                    {
                        var jerry = session.Load<Employee>("employees/1-A");
                        expected = session.Advanced.GetChangeVectorFor(jerry);

                        session.Delete(jerry);
                    }
                    else
                    {
                        session.Delete("employees/1-A");
                    }

                    using (var s = store.OpenSession())
                    {
                        if (deleteById == false)
                        {
                            var j = s.Load<Employee>("employees/1-A");
                            s.Delete(j);
                        }
                        else
                        {
                            s.Delete("employees/1-A");
                        }

                        s.SaveChanges();
                    }

                    if (deleteByIdInBackgroundSession == false)
                    {
                        // this should throw concurrency exception for jerry since it was tracked in session when deleted
                        // The DELETE command detects the doc was already deleted (tombstone) and throws with the expected CV
                        var e = Assert.Throws<Raven.Client.Exceptions.ConcurrencyException>(() => session.SaveChanges());
                        Assert.Contains("does not exist, but delete was called with change vector", e.Message);
                        Assert.Equal("employees/1-a", e.Id); // tombstone stores lowercase ID
                        Assert.Null(e.ActualChangeVector); // doc no longer exists, so no actual CV
                        Assert.Equal(expected, e.ExpectedChangeVector);
                    }
                    else
                    {
                        // this should not throw concurrency exception for jerry
                        session.SaveChanges();
                    }
                }

                using (var session = store.OpenSession(new SessionOptions()
                {
                    OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                }))
                {
                    var egor = session.Load<Employee>("employees/2-A");

                    Assert.Equal("Egor", egor.FirstName);
                    Assert.Equal("Ahad Ha'am", egor.Address.Street);

                    var j = session.Load<Employee>("employees/1-A");
                    Assert.Null(j);
                }
            }
        }

        internal static IEnumerable<object[]> SessionAsyncActions()
        {
            yield return [(Func<IAsyncDocumentSession, Task>)ModifyEgorInSessionAsync];
            yield return [(Func<IAsyncDocumentSession, Task>)ModifyEgorInPatchAfterLoadAsync];
            yield return [(Func<IAsyncDocumentSession, Task>)(s => { ModifyEgorInPatchNoLoad(s); return Task.CompletedTask; })];
            yield return [(Func<IAsyncDocumentSession, Task>)ModifyEgorWithStoreOverwriteAfterLoadAndEvictAsync];
            yield return [(Func<IAsyncDocumentSession, Task>)ModifyEgorWithStoreOverwriteWithoutLoadAsync];
            yield return [(Func<IAsyncDocumentSession, Task>)ModifyEgorWithLoadAndDeleteByIdAsync];
            yield return [(Func<IAsyncDocumentSession, Task>)ModifyEgorWithLoadAndDeleteByEntityAsync];
            yield return [(Func<IAsyncDocumentSession, Task>)(s => { ModifyEgorWithMultiplePatchesAsync(s); return Task.CompletedTask; })];
            yield return [(Func<IAsyncDocumentSession, Task>)ModifyEgorByReplacingAddressAsync];
            yield return [(Func<IAsyncDocumentSession, Task>)ModifyEgorWithAttachmentWithLoadAsync];
            yield return [(Func<IAsyncDocumentSession, Task>)(s => { ModifyEgorWithAttachmentNoLoadAsync(s); return Task.CompletedTask; })];
            yield return [(Func<IAsyncDocumentSession, Task>)(s => { ModifyEgorWithTimeSeriesNoLoadAsync(s); return Task.CompletedTask; })];
            yield return [(Func<IAsyncDocumentSession, Task>)ModifyEgorWithTimeSeriesWithLoadAsync];
            yield return [(Func<IAsyncDocumentSession, Task>)ModifyEgorWithCounterWithLoadAsync];
            yield return [(Func<IAsyncDocumentSession, Task>)(s => { ModifyEgorWithCounterNoLoadAsync(s); return Task.CompletedTask; })];
            yield return [(Func<IAsyncDocumentSession, Task>)(s => { ModifyEgorWithRevisionNoLoadAsync(s); return Task.CompletedTask; })];
            yield return [(Func<IAsyncDocumentSession, Task>)ModifyEgorWithRevisionWithLoadAsync];
        }
        internal static async Task ModifyEgorInPatchAfterLoadAsync(IAsyncDocumentSession session)
        {
            var egor = await session.LoadAsync<Employee>("employees/2-A");
            session.Advanced.Patch(egor, e => e.Address.Street, "Mul HaHof Village");
        }

        internal static void ModifyEgorInPatchNoLoad(IAsyncDocumentSession session)
        {
            session.Advanced.Patch<Employee, string>("employees/2-A", e => e.Address.Street, "Mul HaHof Village");
        }

        internal static async Task ModifyEgorInSessionAsync(IAsyncDocumentSession session)
        {
            var egor = await session.LoadAsync<Employee>("employees/2-A");
            egor.Address = new Address()
            {
                Street = "Mul HaHof Village"
            };
        }

        internal static async Task ModifyEgorWithStoreOverwriteAfterLoadAndEvictAsync(IAsyncDocumentSession session)
        {
            var egor = await session.LoadAsync<Employee>("employees/2-A");

            // tracked entity is evicted from the session, so its not in the trackedEntities list anymore
            session.Advanced.Evict(egor);
            // store after evict is treated as _new_ document, so it will be added to the trackedEntities list again with null change vector, and this should not cause concurrency exception because the original entity is evicted !
            await session.StoreAsync(new Employee
            {
                FirstName = "Egor",
                Address = new Address()
                {
                    Street = "Mul HaHof Village"
                }
            }, "employees/2-A");
        }

        internal static async Task ModifyEgorWithStoreOverwriteWithoutLoadAsync(IAsyncDocumentSession session)
        {
            await session.StoreAsync(new Employee
            {
                FirstName = "Egor",
                Address = new Address()
                {
                    Street = "Mul HaHof Village"
                }
            }, "employees/2-A");
        }

        internal static async Task ModifyEgorWithLoadAndDeleteByIdAsync(IAsyncDocumentSession session)
        {
            var egor = await session.LoadAsync<Employee>("employees/2-A");
            session.Delete("employees/2-A");
        }

        internal static async Task ModifyEgorWithLoadAndDeleteByEntityAsync(IAsyncDocumentSession session)
        {
            var egor = await session.LoadAsync<Employee>("employees/2-A");
            session.Delete(egor);
        }

        internal static void ModifyEgorWithMultiplePatchesAsync(IAsyncDocumentSession session)
        {
            session.Advanced.Patch<Employee, string>("employees/2-A", e => e.Address.Street, "Mul HaHof Village");
            session.Advanced.Patch<Employee, string>("employees/2-A", e => e.FirstName, "Egor");
        }

        internal static async Task ModifyEgorByReplacingAddressAsync(IAsyncDocumentSession session)
        {
            var egor = await session.LoadAsync<Employee>("employees/2-A");
            session.Advanced.Patch(egor, e => e.Address, new Address()
            {
                Street = "Mul HaHof Village",
                City = "Hadera"
            });
        }

        internal static async Task ModifyEgorWithAttachmentWithLoadAsync(IAsyncDocumentSession session)
        {
            var egor = await session.LoadAsync<Employee>("employees/2-A");
            session.Advanced.Attachments.Store(egor, "profile-picture", new MemoryStream("image data"u8.ToArray()));
        }

        internal static void ModifyEgorWithAttachmentNoLoadAsync(IAsyncDocumentSession session)
        {
            session.Advanced.Attachments.Store("employees/2-A", "profile-picture", new MemoryStream("image data"u8.ToArray()));
        }

        internal static void ModifyEgorWithTimeSeriesNoLoadAsync(IAsyncDocumentSession session)
        {
            session.TimeSeriesFor("employees/2-A", "profile-picture-likes")
                .Append(DateTime.UtcNow, new[] { 322d }, "super-like");
        }

        internal static async Task ModifyEgorWithTimeSeriesWithLoadAsync(IAsyncDocumentSession session)
        {
            var egor = await session.LoadAsync<Employee>("employees/2-A");
            session.TimeSeriesFor(egor, "profile-picture-likes")
                .Append(DateTime.UtcNow, new[] { 322d }, "super-like");
        }

        internal static async Task ModifyEgorWithCounterWithLoadAsync(IAsyncDocumentSession session)
        {
            var egor = await session.LoadAsync<Employee>("employees/2-A");
            session.CountersFor(egor).Increment("profile-picture-likes");
        }

        internal static void ModifyEgorWithCounterNoLoadAsync(IAsyncDocumentSession session)
        {
            session.CountersFor("employees/2-A").Increment("profile-picture-likes");
        }

        internal static void ModifyEgorWithRevisionNoLoadAsync(IAsyncDocumentSession session)
        {
            session.Advanced.Revisions.ForceRevisionCreationFor("employees/2-A");
        }

        internal static async Task ModifyEgorWithRevisionWithLoadAsync(IAsyncDocumentSession session)
        {
            var egor = await session.LoadAsync<Employee>("employees/2-A");
            session.Advanced.Revisions.ForceRevisionCreationFor(egor);
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [MemberData(nameof(SessionAsyncActions))]
        public async Task ShouldThrowConcurrencyException_WhenTrackedEntityWasChangedInBackgroundSessionAsync(Func<IAsyncDocumentSession, Task> sessionAction)
        {
            using (var store = GetDocumentStore())
            {
                await ShouldThrowConcurrencyException_WhenTrackedEntityWasChangedInBackgroundSessionInternalAsync(sessionAction, store);
            }
        }

        private static async Task ShouldThrowConcurrencyException_WhenTrackedEntityWasChangedInBackgroundSessionInternalAsync(Func<IAsyncDocumentSession, Task> sessionAction, DocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Employee
                {
                    FirstName = "Jerry"
                }, "employees/1-A");
                await session.StoreAsync(new Employee
                {
                    FirstName = "Egor",
                    Address = new Address()
                    {
                        Street = "Ahad Ha'am"
                    }
                }, "employees/2-A");
                await session.SaveChangesAsync();
            }

            using (IAsyncDocumentSession session = store.OpenAsyncSession(new SessionOptions()
            {
                OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads,
            }))
            {
                var jerry = await session.LoadAsync<Employee>("employees/1-A");

                await sessionAction.Invoke(session);

                var expected = session.Advanced.GetChangeVectorFor(jerry);
                Assert.NotEmpty(expected);
                Assert.NotNull(expected);

                var actual = string.Empty;
                using (var s = store.OpenSession())
                {
                    var j = s.Load<Employee>("employees/1-A");
                    j.Address = new Address()
                    {
                        City = "Hadera"
                    };
                    s.SaveChanges();

                    actual = s.Advanced.GetChangeVectorFor(j);
                }

                Assert.NotEmpty(actual);
                Assert.NotNull(actual);

                // this should throw concurrency exception for jerry
                var e = await Assert.ThrowsAsync<Raven.Client.Exceptions.ConcurrencyException>(() => session.SaveChangesAsync());

                Assert.Contains("Document 'employees/1-A' has been modified", e.Message);
                Assert.Equal("employees/1-A", e.Id);
                Assert.Equal(actual, e.ActualChangeVector);
                Assert.Equal(expected, e.ExpectedChangeVector);
            }

            using (var session = store.OpenSession(new SessionOptions()
            {
                OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
            }))
            {
                var egor = session.Load<Employee>("employees/2-A");

                Assert.Equal("Egor", egor.FirstName);
                Assert.Equal("Ahad Ha'am", egor.Address.Street);

                var j = session.Load<Employee>("employees/1-A");
                Assert.Equal("Hadera", j.Address.City);
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [MemberData(nameof(SessionActions))]
        public void ShouldThrowConcurrencyException_WhenOptimisticConcurrencyModeIsSetViaConventions(Action<IDocumentSession> sessionAction)
        {
            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDocumentStore = s => s.Conventions.OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                   }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/1-A");
                    session.Store(new Employee
                    {
                        FirstName = "Egor",
                        Address = new Address()
                        {
                            Street = "Ahad Ha'am"
                        }
                    }, "employees/2-A");
                    session.SaveChanges();
                }

                // open session without explicit SessionOptions - should inherit from conventions
                using (IDocumentSession session = store.OpenSession())
                {
                    var jerry = session.Load<Employee>("employees/1-A");

                    sessionAction.Invoke(session);

                    var expected = session.Advanced.GetChangeVectorFor(jerry);
                    Assert.NotEmpty(expected);
                    Assert.NotNull(expected);

                    using (var s = store.OpenSession())
                    {
                        var j = s.Load<Employee>("employees/1-A");
                        j.Address = new Address()
                        {
                            City = "Hadera"
                        };
                        s.SaveChanges();
                    }

                    // this should throw concurrency exception for jerry because conventions set WritesAndReads
                    var e = Assert.Throws<ConcurrencyException>(() => session.SaveChanges());

                    Assert.Contains("Document 'employees/1-A' has been modified", e.Message);
                    Assert.Equal("employees/1-A", e.Id);
                }

                using (var session = store.OpenSession())
                {
                    var egor = session.Load<Employee>("employees/2-A");

                    Assert.Equal("Egor", egor.FirstName);
                    Assert.Equal("Ahad Ha'am", egor.Address.Street);

                    var j = session.Load<Employee>("employees/1-A");
                    Assert.Equal("Hadera", j.Address.City);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void SessionOptions_ShouldOverride_Conventions()
        {
            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDocumentStore = s => s.Conventions.OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                   }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "Jerry"
                    }, "employees/1-A");
                    session.SaveChanges();
                }

                // session explicitly sets None - should override the convention
                using (var session = store.OpenSession(new SessionOptions
                       {
                           OptimisticConcurrencyMode = OptimisticConcurrencyMode.None
                       }))
                {
                    var jerry = session.Load<Employee>("employees/1-A");

                    using (var s = store.OpenSession())
                    {
                        var j = s.Load<Employee>("employees/1-A");
                        j.Address = new Address { City = "Hadera" };
                        s.SaveChanges();
                    }

                    // should NOT throw because session overrides convention with None
                    session.SaveChanges();
                }
            }
        }
        [RavenFact(RavenTestCategory.ClientApi)]
        public void WritesAndReads_ShouldStillDetectConcurrencyOnReadOnlyEntity_WhenModifiedEntityIsExcludedFromTrackCommand()
        {
            // This test verifies that when a document is modified (PUT), it is excluded from the
            // BatchTrackChangesCommand (since the PUT already checks concurrency), but read-only
            // tracked documents are still verified by the BatchTrackChangesCommand.
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee { FirstName = "Jerry" }, "employees/1-A");
                    session.Store(new Employee { FirstName = "Egor", Address = new Address { Street = "Ahad Ha'am" } }, "employees/2-A");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions
                       {
                           OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                       }))
                {
                    // load both - both are tracked
                    var jerry = session.Load<Employee>("employees/1-A");  // read-only (not modified)
                    var egor = session.Load<Employee>("employees/2-A");   // will be modified

                    // modify only egor - this will generate a PUT command with change vector
                    egor.Address = new Address { Street = "New Street" };

                    // background-modify jerry (the read-only entity)
                    using (var s = store.OpenSession())
                    {
                        var j = s.Load<Employee>("employees/1-A");
                        j.Address = new Address { City = "Hadera" };
                        s.SaveChanges();
                    }

                    // SaveChanges should throw for jerry via BatchTrackChangesCommand
                    // (egor's PUT has its own concurrency check, jerry is only checked via the track command)
                    var e = Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
                    Assert.Contains("employees/1-A", e.Message);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void WritesAndReads_ShouldStillDetectConcurrencyOnModifiedEntity_ViaPutCommand()
        {
            // This test verifies that even though the modified document is excluded from the
            // BatchTrackChangesCommand, the PUT command itself still checks concurrency.
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee { FirstName = "Jerry" }, "employees/1-A");
                    session.Store(new Employee { FirstName = "Egor", Address = new Address { Street = "Ahad Ha'am" } }, "employees/2-A");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions
                       {
                           OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                       }))
                {
                    // load both - both are tracked
                    var jerry = session.Load<Employee>("employees/1-A");  // read-only
                    var egor = session.Load<Employee>("employees/2-A");   // will be modified

                    // modify egor
                    egor.Address = new Address { Street = "New Street" };

                    // background-modify egor (the entity we're also modifying)
                    using (var s = store.OpenSession())
                    {
                        var e2 = s.Load<Employee>("employees/2-A");
                        e2.Address = new Address { Street = "Background Street" };
                        s.SaveChanges();
                    }

                    // SaveChanges should throw for egor via the PUT command's own concurrency check
                    var e = Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
                    Assert.Contains("employees/2-A", e.Message);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void WritesAndReads_ModifiedEntityShouldBeExcludedFromBatchTrackChangesCommand()
        {
            // This test directly verifies that when PrepareForSaveChanges builds the batch,
            // the modified entity's ID is in IdsAlreadyCheckedForConcurrency (used as idsToSkip),
            // ensuring the server's BatchTrackChangesCommand does not redundantly check it.
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee { FirstName = "Jerry" }, "employees/1-A");
                    session.Store(new Employee { FirstName = "Egor" }, "employees/2-A");
                    session.Store(new Employee { FirstName = "Dana" }, "employees/3-A");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions
                       {
                           OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                       }))
                {
                    var internalSession = (InMemoryDocumentSessionOperations)session;

                    // load 3 docs - all tracked
                    var jerry = session.Load<Employee>("employees/1-A");   // read-only
                    var egor = session.Load<Employee>("employees/2-A");    // will be modified
                    var dana = session.Load<Employee>("employees/3-A");    // read-only

                    // modify only egor
                    egor.FirstName = "Egor Modified";

                    var saveChangesData = internalSession.PrepareForSaveChanges();

                    // egor's ID should be in the skip set (because the PUT command already checks concurrency for it)
                    Assert.Contains("employees/2-A", saveChangesData.IdsAlreadyCheckedForConcurrency);

                    // jerry and dana should NOT be in the skip set (they are read-only, only checked by BatchTrackChanges)
                    Assert.DoesNotContain("employees/1-A", saveChangesData.IdsAlreadyCheckedForConcurrency);
                    Assert.DoesNotContain("employees/3-A", saveChangesData.IdsAlreadyCheckedForConcurrency);

                    // the BatchTrackChangesCommandData should exist
                    Assert.NotNull(saveChangesData.TrackChangesCommandData);

                    // verify TrackedEntities contains all 3 docs
                    Assert.True(saveChangesData.TrackChangesCommandData.TrackedEntities.ContainsKey("employees/1-A"));
                    Assert.True(saveChangesData.TrackChangesCommandData.TrackedEntities.ContainsKey("employees/2-A"));
                    Assert.True(saveChangesData.TrackChangesCommandData.TrackedEntities.ContainsKey("employees/3-A"));

                    // now verify the JSON output - the serialized command should NOT include the modified entity
                    using (var context = JsonOperationContext.ShortTermSingleUse())
                    {
                        var json = saveChangesData.TrackChangesCommandData.ToJson(store.Conventions, context);
                        using (var blittable = context.ReadObject(json, "test"))
                        {
                            var trackedEntities = blittable["TrackedEntities"] as BlittableJsonReaderObject;
                            Assert.NotNull(trackedEntities);

                            var propertyNames = trackedEntities.GetPropertyNames();

                            // jerry and dana (read-only) should be in the JSON sent to server
                            Assert.Contains("employees/1-A", propertyNames);
                            Assert.Contains("employees/3-A", propertyNames);

                            // egor (modified) should NOT be in the JSON sent to server - no duplicate check
                            Assert.DoesNotContain("employees/2-A", propertyNames);
                        }
                    }
                }
            }
        }
        [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
        public void WritesAndReads_ShouldThrowOnShardedDatabase(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee { FirstName = "Jerry" }, "employees/1-A");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions
                       {
                           OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                       }))
                {
                    var jerry = session.Load<Employee>("employees/1-A");

                    Assert.Throws<NotSupportedInShardingException>(() => session.SaveChanges());
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void WritesAndReads_DeferredDeleteById_IsStillCheckedByBatchTrackChangesCommand()
        {
            // Verifies that Delete(string id) via Defer path keeps the entity in BatchTrackChangesCommand
            // (the deferred delete also checks concurrency, so the check is redundant but correct)
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee { FirstName = "Jerry" }, "employees/1-A");
                    session.Store(new Employee { FirstName = "Egor" }, "employees/2-A");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions
                       {
                           OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                       }))
                {
                    var internalSession = (InMemoryDocumentSessionOperations)session;

                    var jerry = session.Load<Employee>("employees/1-A");   // read-only
                    var egor = session.Load<Employee>("employees/2-A");    // will be deleted by ID

                    // delete by string ID — goes through Defer path (not PrepareForEntitiesDeletion)
                    session.Delete("employees/2-A");

                    var saveChangesData = internalSession.PrepareForSaveChanges();

                    // egor's deferred delete is NOT in the skip set (only session commands from
                    // PrepareForEntitiesPuts/PrepareForEntitiesDeletion add to IdsAlreadyCheckedForConcurrency)
                    Assert.DoesNotContain("employees/2-A", saveChangesData.IdsAlreadyCheckedForConcurrency);

                    // verify BatchTrackChangesCommand still includes both entities
                    Assert.NotNull(saveChangesData.TrackChangesCommandData);
                    using (var context = JsonOperationContext.ShortTermSingleUse())
                    {
                        var json = saveChangesData.TrackChangesCommandData.ToJson(store.Conventions, context);
                        using (var blittable = context.ReadObject(json, "test"))
                        {
                            var trackedEntities = blittable["TrackedEntities"] as BlittableJsonReaderObject;
                            Assert.NotNull(trackedEntities);

                            var propertyNames = trackedEntities.GetPropertyNames();
                            Assert.Contains("employees/1-A", propertyNames);
                            Assert.Contains("employees/2-A", propertyNames); // still checked by BatchTrackChanges
                        }
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void WritesFromConventions_ShouldBeInherited_AndNotTrackEntities()
        {
            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDocumentStore = s => s.Conventions.OptimisticConcurrencyMode = OptimisticConcurrencyMode.Writes
                   }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee { FirstName = "Jerry" }, "employees/1-A");
                    session.SaveChanges();
                }

                // open session without explicit OptimisticConcurrencyMode - should inherit Writes from conventions
                using (var session = store.OpenSession())
                {
                    var internalSession = (InMemoryDocumentSessionOperations)session;
                    Assert.Equal(OptimisticConcurrencyMode.Writes, internalSession.OptimisticConcurrencyMode);

                    var jerry = session.Load<Employee>("employees/1-A");

                    // Writes mode should NOT track entities (only WritesAndReads does)
                    Assert.False(internalSession.TrackedEntities.TryGetValue("employees/1-A", out _));

                    // but writes should still be concurrency-checked
                    jerry.FirstName = "Modified";

                    using (var s = store.OpenSession())
                    {
                        var j = s.Load<Employee>("employees/1-A");
                        j.FirstName = "Background";
                        s.SaveChanges();
                    }

                    // should throw because the PUT has a change vector (Writes mode)
                    var e = Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
                    Assert.Contains("employees/1-A", e.Message);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void WritesAndReads_ConcurrencyCheckModeDisabled_ShouldStillBeTracked()
        {
            // When ConcurrencyCheckMode.Disabled is used, the PUT won't check concurrency,
            // but WritesAndReads tracking should still catch changes via BatchTrackChangesCommand
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee { FirstName = "Jerry" }, "employees/1-A");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions
                       {
                           OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                       }))
                {
                    var jerry = session.Load<Employee>("employees/1-A");

                    // Store with null changeVector → ConcurrencyCheckMode.Disabled
                    session.Store(new Employee { FirstName = "NewEmployee" }, changeVector: null, id: "employees/2-A");

                    // background-modify jerry (read-only entity)
                    using (var s = store.OpenSession())
                    {
                        var j = s.Load<Employee>("employees/1-A");
                        j.FirstName = "Background";
                        s.SaveChanges();
                    }

                    // should throw for jerry via BatchTrackChangesCommand
                    var e = Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
                    Assert.Contains("employees/1-A", e.Message);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task WritesAndReads_AsyncSession_ShouldThrowConcurrencyException()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Employee { FirstName = "Jerry" }, "employees/1-A");
                    await session.StoreAsync(new Employee { FirstName = "Egor", Address = new Address { Street = "Ahad Ha'am" } }, "employees/2-A");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(new SessionOptions
                       {
                           OptimisticConcurrencyMode = OptimisticConcurrencyMode.WritesAndReads
                       }))
                {
                    var jerry = await session.LoadAsync<Employee>("employees/1-A");
                    var egor = await session.LoadAsync<Employee>("employees/2-A");

                    // modify egor so we have a write
                    egor.Address = new Address { Street = "New Street" };

                    // background-modify jerry (read-only)
                    using (var s = store.OpenSession())
                    {
                        var j = s.Load<Employee>("employees/1-A");
                        j.Address = new Address { City = "Hadera" };
                        s.SaveChanges();
                    }

                    // should throw for jerry via BatchTrackChangesCommand
                    var e = await Assert.ThrowsAsync<ConcurrencyException>(() => session.SaveChangesAsync());
                    Assert.Contains("employees/1-A", e.Message);
                }
            }
        }
    }
}
