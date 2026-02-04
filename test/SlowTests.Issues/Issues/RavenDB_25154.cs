using System;
using System.Collections.Generic;
using System.IO;
using FastTests;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_25154 : RavenTestBase
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
            //yield return [(Action<IDocumentSession>)ModifyEgorWithDelete];
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
            session.Advanced.Evict(egor);
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

        // TODO: egor this is not tracked in sessions, so we don't care!
        internal static void ModifyEgorWithDelete(IDocumentSession session)
        {
            session.Delete("employees/2-A");

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
        ////TODO: doesn twork
        //internal static void ModifyEgorWithDeleteAndStore(IDocumentSession session)
        //{
        //    session.Delete("employees/2-A");
        //    session.Store(new Employee
        //    {
        //        FirstName = "Egor",
        //        Address = new Address()
        //        {
        //            Street = "Mul HaHof Village"
        //        }
        //    }, "employees/2-A");
        //}

        ////TODO: doesn twork
        //internal static void ModifyEgorWithLoadDeleteAndStore(IDocumentSession session)
        //{
        //    var egor = session.Load<Employee>("employees/2-A");
        //    session.Delete(egor);
        //    session.Store(new Employee
        //    {
        //        FirstName = "Egor",
        //        Address = new Address()
        //        {
        //            Street = "Mul HaHof Village"
        //        }
        //    }, "employees/2-A");
        //}

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
                           TrackingMode = TrackingMode.TrackAllEntities,
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

                    Assert.Contains("Document change vector mismatch", e.Message);
                    Assert.Equal("employees/1-A", e.Id);
                    Assert.Equal(actual, e.ActualChangeVector);
                    Assert.Equal(expected, e.ExpectedChangeVector);
                }

                using (var session = store.OpenSession(new SessionOptions()
                       {
                           TrackingMode = TrackingMode.TrackAllEntities
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

        //TODO: egor what happends when I load the document but its null, then I try to SaveChanges but thsi document was added meanwhile but another session
        [RavenFact(RavenTestCategory.ClientApi)]
        public void ShouldThrowConcurrencyException_WhenTrackedEntityIsNullButThenWasAddedInBackgroundSession()
        {
            using (var store = GetDocumentStore())
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
                           TrackingMode = TrackingMode.TrackAllEntities,
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

                    Assert.Contains("Document change vector mismatch", e.Message);
                    Assert.Equal("employees/1-A", e.Id);
                    Assert.Equal(actual, e.ActualChangeVector);
                    Assert.Equal(string.Empty, e.ExpectedChangeVector);
                }

                using (var session = store.OpenSession(new SessionOptions()
                       {
                           TrackingMode = TrackingMode.TrackAllEntities
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

        //TODO: egor what happends when I delete the document , then I try to SaveChanges but thsi document was added meanwhile but another session
        [RavenFact(RavenTestCategory.ClientApi)]
        public void ShouldThrowConcurrencyException_WhenTrackedEntityDeletedByEntityButThenWasEditedInBackgroundSession()
        {
            using (var store = GetDocumentStore())
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
                           TrackingMode = TrackingMode.TrackAllEntities,
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

                    // this should throw concurrency exception for jerry
                    var e = Assert.Throws<Raven.Client.Exceptions.ConcurrencyException>(() => session.SaveChanges());

                    Assert.Contains("Document change vector mismatch", e.Message);
                    Assert.Equal("employees/1-A", e.Id);
                    Assert.Equal(actual, e.ActualChangeVector);
                    Assert.Equal(expected, e.ExpectedChangeVector);
                }

                using (var session = store.OpenSession(new SessionOptions()
                       {
                           TrackingMode = TrackingMode.TrackAllEntities
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

        [RavenFact(RavenTestCategory.ClientApi)]
        public void ShouldNotThrowConcurrencyException_WhenTrackedEntityDeletedByIdWithoutChangeVectorButThenWasNotEditedInBackgroundSession()
        {
            using (var store = GetDocumentStore())
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
                    TrackingMode = TrackingMode.TrackAllEntities,
                }))
                {
                    var jerry = session.Load<Employee>("employees/1-A");
                    session.Delete("employees/1-A");
                    ModifyEgorInSession(session);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions()
                {
                    TrackingMode = TrackingMode.TrackAllEntities
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

        [RavenFact(RavenTestCategory.ClientApi)]
        public void ShouldNotThrowConcurrencyException_WhenTrackedEntityDeletedByIdWithChangeVectorButThenWasNotEditedInBackgroundSession2()
        {
            using (var store = GetDocumentStore())
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
                    TrackingMode = TrackingMode.TrackAllEntities,
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
                    TrackingMode = TrackingMode.TrackAllEntities
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

        [RavenFact(RavenTestCategory.ClientApi)]
        public void ShouldThrowConcurrencyException_WhenTrackedEntityDeletedByIdButThenWasEditedInBackgroundSession()
        {
            using (var store = GetDocumentStore())
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
                    TrackingMode = TrackingMode.TrackAllEntities,
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

                    Assert.Contains("Document change vector mismatch", e.Message);
                    Assert.Equal("employees/1-A", e.Id);
                             Assert.Equal(actual, e.ActualChangeVector);
                    Assert.Equal(expected, e.ExpectedChangeVector);
                }

                using (var session = store.OpenSession(new SessionOptions()
                {
                    TrackingMode = TrackingMode.TrackAllEntities
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
                           TrackingMode = TrackingMode.TrackAllEntities,
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

                    Assert.Contains("Document change vector mismatch", e.Message);
                    Assert.Equal(addressId, e.Id);
                    Assert.Equal(actual, e.ActualChangeVector);
                    Assert.Equal(expected, e.ExpectedChangeVector);
                }

                using (var session = store.OpenSession(new SessionOptions()
                       {
                           TrackingMode = TrackingMode.TrackAllEntities
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

        [RavenFact(RavenTestCategory.ClientApi)]
        public void ShouldThrowConcurrencyException_WhenNonExistsEntityIncludedBySessionButThenWasAddedInBackgroundSession()
        {
            using (var store = GetDocumentStore())
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
                    TrackingMode = TrackingMode.TrackAllEntities,
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

                    Assert.Contains("Document change vector mismatch", e.Message);
                    Assert.Equal(addressId, e.Id);
                    Assert.Equal(actual, e.ActualChangeVector);
                    Assert.Equal(string.Empty, e.ExpectedChangeVector);
                }

                using (var session = store.OpenSession(new SessionOptions()
                {
                    TrackingMode = TrackingMode.TrackAllEntities
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


        [RavenFact(RavenTestCategory.ClientApi)]
        public void ShouldThrowConcurrencyException_WhenNonExistsEntityIncludedBySessionButThenWasEditedInBackgroundSession()
        {
            using (var store = GetDocumentStore())
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
                    TrackingMode = TrackingMode.TrackAllEntities,
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

                    Assert.Contains("Document change vector mismatch", e.Message);
                    Assert.Equal(addressId, e.Id);
                    Assert.Equal(actual, e.ActualChangeVector);
                    Assert.Equal(expected, e.ExpectedChangeVector);
                }

                using (var session = store.OpenSession(new SessionOptions()
                {
                    TrackingMode = TrackingMode.TrackAllEntities
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
    }
}
