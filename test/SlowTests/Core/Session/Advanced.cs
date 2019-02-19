using System;
using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Exceptions;
using Sparrow.Json.Parsing;
using Xunit;

using Company = SlowTests.Core.Utils.Entities.Company;
using User = SlowTests.Core.Utils.Entities.User;
using Raven.Server.Documents.Replication;

namespace SlowTests.Core.Session
{
    public class Advanced : RavenTestBase
    {
        [Fact]
        public void CanGetChangesInformation()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    Assert.False(session.Advanced.HasChanges);

                    var user = new User { Id = "users/1", Name = "John" };
                    session.Store(user);

                    Assert.True(session.Advanced.HasChanged(user));
                    Assert.True(session.Advanced.HasChanges);

                    session.SaveChanges();

                    Assert.False(session.Advanced.HasChanged(user));
                    Assert.False(session.Advanced.HasChanges);

                    user.AddressId = "addresses/1";
                    Assert.True(session.Advanced.HasChanged(user));
                    Assert.True(session.Advanced.HasChanges);

                    var whatChanged = session.Advanced.WhatChanged();
                    Assert.Equal("AddressId", whatChanged["users/1"][0].FieldName);
                    Assert.Equal(null, whatChanged["users/1"][0].FieldOldValue);
                    Assert.True(whatChanged["users/1"][0].FieldNewValue.Equals("addresses/1"));

                    session.Advanced.Clear();
                    Assert.False(session.Advanced.HasChanges);

                    var user2 = new User { Id = "users/2", Name = "John" };
                    session.Store(user2);
                    session.Delete(user2);

                    Assert.True(session.Advanced.HasChanged(user2));
                    Assert.True(session.Advanced.HasChanges);
                }
            }
        }

        [Fact]
        public void CanUseEvict()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Id = "users/1", Name = "John" };

                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(0, session.Advanced.NumberOfRequests);

                    session.Load<User>("users/1");

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var user = session.Load<User>("users/1");

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.Advanced.Evict(user);

                    session.Load<User>("users/1");

                    Assert.Equal(2, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void CanUseClear()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Id = "users/1", Name = "John" };

                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(0, session.Advanced.NumberOfRequests);

                    session.Load<User>("users/1");

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.Load<User>("users/1");

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.Advanced.Clear();

                    session.Load<User>("users/1");

                    Assert.Equal(2, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void CanUseIsLoaded()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Id = "users/1", Name = "John" };

                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.False(session.Advanced.IsLoaded("users/1"));

                    session.Load<User>("users/1");

                    Assert.True(session.Advanced.IsLoaded("users/1"));
                    Assert.False(session.Advanced.IsLoaded("users/2"));

                    session.Advanced.Clear();

                    Assert.False(session.Advanced.IsLoaded("users/1"));
                }
            }
        }

        [Fact]
        public void CanUseRefresh()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Id = "users/1", Name = "John" };

                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.NotNull(user);
                    Assert.Equal("John", user.Name);

                    using (var otherSession = store.OpenSession())
                    {
                        var u = otherSession.Load<User>("users/1");
                        u.Name = "Jonathan";

                        otherSession.SaveChanges();
                    }

                    using (var otherSession = store.OpenSession())
                    {
                        var u = otherSession.Load<User>("users/1");
                        Assert.Equal("Jonathan", u.Name);
                    }

                    user = session.Load<User>("users/1");

                    Assert.NotNull(user);
                    Assert.Equal("John", user.Name);

                    session.Advanced.Refresh(user);

                    Assert.NotNull(user);
                    Assert.Equal("Jonathan", user.Name);
                }
            }
        }

        [Fact]
        public void CanUseOptmisticConcurrency()
        {
            const string entityId = "users/1";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    Assert.False(session.Advanced.UseOptimisticConcurrency);
                    session.Advanced.UseOptimisticConcurrency = true;

                    session.Store(new User { Id = entityId, Name = "User1" });
                    session.SaveChanges();

                    using (var otherSession = store.OpenSession())
                    {
                        var otherUser = otherSession.Load<User>(entityId);
                        otherUser.Name = "OtherName";
                        otherSession.Store(otherUser);
                        otherSession.SaveChanges();
                    }

                    var user = session.Load<User>("users/1");
                    user.Name = "Name";
                    session.Store(user);
                    var e = Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
                    Assert.Contains($"Optimistic concurrency violation, transaction will be aborted.", e.Message);
                }
            }
        }

        [Fact]
        public void CanGetDocumentMetadata()
        {
            const string companyId = "companies/1";
            const string attrKey = "SetDocumentMetadataTestKey";
            const string attrVal = "SetDocumentMetadataTestValue";

            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Put(companyId, null, new Company { Id = companyId }, new Dictionary<string, object> { { attrKey, attrVal } });
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>(companyId);
                    var result = session.Advanced.GetMetadataFor(company);
                    Assert.NotNull(result);
                    Assert.Equal(attrVal, result[attrKey]);
                }
            }
        }

        [Fact]
        public void CanUseNumberOfRequests()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    Assert.Equal(0, session.Advanced.NumberOfRequests);

                    var company = new Company();
                    company.Name = "NumberOfRequestsTest";

                    session.Store(company);
                    session.SaveChanges();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var company2 = session.Load<Company>(company.Id);
                    company2.Name = "NumberOfRequestsTest2";
                    session.Store(company2);
                    session.SaveChanges();
                    Assert.Equal(2, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void CanUseMaxNumberOfRequestsPerSession()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Advanced.MaxNumberOfRequestsPerSession = 2;

                    var company = new Company();
                    session.Store(company);
                    session.SaveChanges();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    company.Name = "1";
                    session.Store(company);
                    session.SaveChanges();
                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    try
                    {
                        company.Name = "2";
                        session.Store(company);
                        session.SaveChanges();
                        Assert.False(true, "I expected InvalidOperationException to be thrown here.");
                    }
                    catch (InvalidOperationException)
                    {
                    }
                }
            }
        }

        [Fact]
        public void CanGetEtagFor()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Put("companies/1", null, new Company { Id = "companies/1" }, null);
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    Assert.Equal(1, session.Advanced.GetChangeVectorFor(company).ToChangeVector()[0].Etag);
                }
            }
        }

        [Fact]
        public void CanLazilyLoadEntity()
        {
            const string COMPANY1_ID = "companies/1";
            const string COMPANY2_ID = "companies/2";

            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Put(COMPANY1_ID, null, new Company { Id = COMPANY1_ID }, null);
                    commands.Put(COMPANY2_ID, null, new Company { Id = COMPANY2_ID }, null);
                }

                using (var session = store.OpenSession())
                {
                    Lazy<Company> lazyOrder = session.Advanced.Lazily.Load<Company>(COMPANY1_ID);
                    Assert.False(lazyOrder.IsValueCreated);
                    var order = lazyOrder.Value;
                    Assert.Equal(COMPANY1_ID, order.Id);

                    var lazyOrders = session.Advanced.Lazily.Load<Company>(new[] { COMPANY1_ID, COMPANY2_ID });
                    Assert.False(lazyOrders.IsValueCreated);
                    var orders = lazyOrders.Value;
                    Assert.Equal(2, orders.Count);
                    Assert.Equal(COMPANY1_ID, orders[COMPANY1_ID].Id);
                    Assert.Equal(COMPANY2_ID, orders[COMPANY2_ID].Id);
                }
            }
        }

        [Fact]
        public void CanExecuteAllPendingLazyOperations()
        {
            const string COMPANY1_ID = "companies/1";
            const string COMPANY2_ID = "companies/2";

            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Put(COMPANY1_ID, null, new Company { Id = COMPANY1_ID }, null);
                    commands.Put(COMPANY2_ID, null, new Company { Id = COMPANY2_ID }, null);
                }

                using (var session = store.OpenSession())
                {
                    Company company1 = null;
                    Company company2 = null;

                    session.Advanced.Lazily.Load<Company>(COMPANY1_ID, x => company1 = x);
                    session.Advanced.Lazily.Load<Company>(COMPANY2_ID, x => company2 = x);
                    Assert.Null(company1);
                    Assert.Null(company2);

                    session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
                    Assert.NotNull(company1);
                    Assert.NotNull(company2);
                    Assert.Equal(COMPANY1_ID, company1.Id);
                    Assert.Equal(COMPANY2_ID, company2.Id);
                }
            }
        }

        [Fact]
        public void CanUseDefer()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var commands = new ICommandData[]
                    {
                        new PutCommandData("company1", null, new DynamicJsonValue { ["Name"] = "company 1" }),
                        new PutCommandData("company2", null, new DynamicJsonValue { ["Name"] = "company 2" })
                    };

                    session.Advanced.Defer(commands);
                    session.Advanced.Defer(new DeleteCommandData("company1", null));

                    session.SaveChanges();

                    Assert.Null(session.Load<Company>("company1"));
                    Assert.NotNull(session.Load<Company>("company2"));
                }
            }
        }

        [Fact]
        public void CanAggressivelyCacheFor()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Id = "users/1", Name = "Name" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(0, session.Advanced.NumberOfRequests);
                    session.Load<User>("users/1");
                    Assert.Equal(1, session.Advanced.RequestExecutor.Cache.NumberOfItems);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }

                using (var session = store.OpenSession())
                {
                    session.Load<User>("users/1");
                    Assert.Equal(1, session.Advanced.RequestExecutor.Cache.NumberOfItems);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    for (var i = 0; i <= 20; i++)
                    {
                        using (store.AggressivelyCacheFor(TimeSpan.FromSeconds(30)))
                        {
                            session.Load<User>("users/1");
                        }
                    }

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }
    }
}
