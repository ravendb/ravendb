using System;
using System.Collections.Generic;
using NewClientTests;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Documents;
using Raven.NewClient.Client.Exceptions;
using Raven.NewClient.Json.Linq;
using Sparrow.Json.Parsing;
using Xunit;

using Company = SlowTests.Core.Utils.Entities.Company;
using User = SlowTests.Core.Utils.Entities.User;

namespace NewClientTests.NewClient.Raven.Tests.Core.Session
{
    public class Advanced : RavenTestBase
    {
        [Fact]
        public void CanGetChangesInformation()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    Assert.False(session.Advanced.HasChanges);

                    var user = new User { Id = "users/1", Name = "John" };
                    session.Store(user);

                    Assert.True(session.Advanced.HasChanged(user));
                    var x = session.Advanced.HasChanges;
                    Assert.True(session.Advanced.HasChanges);

                    session.SaveChanges();

                    Assert.False(session.Advanced.HasChanged(user));
                    Assert.False(session.Advanced.HasChanges);

                    user.AddressId = "addresses/1";
                    Assert.True(session.Advanced.HasChanged(user));
                    Assert.True(session.Advanced.HasChanges);

                    var whatChanged = session.Advanced.WhatChanged();
                    Assert.Equal("AddressId", ((DocumentsChanges[])whatChanged["users/1"])[0].FieldName);
                    Assert.Equal(null, ((DocumentsChanges[])whatChanged["users/1"])[0].FieldOldValue);
                    Assert.Equal("addresses/1", ((DocumentsChanges[])whatChanged["users/1"])[0].FieldNewValue.ToString());

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
                using (var session = store.OpenNewSession())
                {
                    var user = new User { Id = "users/1", Name = "John" };

                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
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

                using (var session = store.OpenNewSession())
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
                using (var session = store.OpenNewSession())
                {
                    var user = new User { Id = "users/1", Name = "John" };

                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
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
                using (var session = store.OpenNewSession())
                {
                    var user = new User { Id = "users/1", Name = "John" };

                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.NotNull(user);
                    Assert.Equal("John", user.Name);

                    InMemoryDocumentSessionOperations.DocumentInfo documentInfo;
                    var document = GetCommand(session, new[] { "users/1" }, out documentInfo);

                    documentInfo.Entity = session.ConvertToEntity(typeof(User), "users/1", document);
                    ((User)documentInfo.Entity).Name = "Jonathan";
                    PutCommand(session, documentInfo.Entity, "users/1");

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
                using (var session = store.OpenNewSession())
                {
                    Assert.False(session.Advanced.UseOptimisticConcurrency);
                    session.Advanced.UseOptimisticConcurrency = true;

                    session.Store(new User { Id = entityId, Name = "User1" });
                    session.SaveChanges();

                    using (var otherSession = store.OpenNewSession())
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
                }
            }
        }

        [Fact( Skip = "TODO: GetDocumentUrl Not Implemented")]
        public void CanGetDocumentUrl()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    session.Store(new Company { Id = "companies/1" });
                    session.SaveChanges();

                    var company = session.Load<Company>("companies/1");
                    Assert.NotNull(company);
                    var uri = new Uri(session.Advanced.GetDocumentUrl(company));
                    Assert.Equal("/databases/" + store.DefaultDatabase + "/docs/companies/1", uri.AbsolutePath);
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
                store.DatabaseCommands.Put(
                    companyId,
                    null,
                    RavenJObject.FromObject(new Company { Id = companyId }),
                    new RavenJObject { { attrKey, attrVal } }
                    );

                using (var session = store.OpenNewSession())
                {
                    var company = session.Load<Company>(companyId);
                    var result = session.Advanced.GetMetadataFor<Company>(company);
                    Assert.NotNull(result);
                    //Assert.Equal(attrVal, result.Value<string>(attrKey));
                }
            }
        }

        [Fact]
        public void CanMarkReadOnly()
        {
            const string categoryName = "MarkReadOnlyTest";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    session.Store(new Company { Id = "companies/1" });
                    session.SaveChanges();

                    var company = session.Load<Company>("companies/1");
                    session.Advanced.MarkReadOnly(company);
                    company.Name = categoryName;
                    Assert.True(session.Advanced.HasChanges);

                    session.Store(company);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    Assert.Equal("true", session.Advanced.GetMetadataFor<Company>(company)["Raven-Read-Only"]);
                }
            }
        }

        [Fact]
        public void CanUseNumberOfRequests()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
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
                using (var session = store.OpenNewSession())
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
                using (var session = store.OpenNewSession())
                {
                    PutCommand(session, new Company { Id = "companies/1" }, "companies/1");
                    var company = session.Load<Company>("companies/1");
                    Assert.Equal(1, session.Advanced.GetEtagFor<Company>(company));
                }
            }
        }

        [Fact (Skip = "TODO:Lazy Not Implemented")]
        public void CanLazilyLoadEntity()
        {
            const string COMPANY1_ID = "companies/1";
            const string COMPANY2_ID = "companies/2";

            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.Put(
                    COMPANY1_ID,
                    null,
                    RavenJObject.FromObject(new Company { Id = COMPANY1_ID }),
                    new RavenJObject()
                    );
                store.DatabaseCommands.Put(
                    COMPANY2_ID,
                    null,
                    RavenJObject.FromObject(new Company { Id = COMPANY2_ID }),
                    new RavenJObject()
                    );

                using (var session = store.OpenSession())
                {
                    Lazy<Company> lazyOrder = session.Advanced.Lazily.Load<Company>(COMPANY1_ID);
                    Assert.False(lazyOrder.IsValueCreated);
                    var order = lazyOrder.Value;
                    Assert.Equal(COMPANY1_ID, order.Id);

                    Lazy<Company[]> lazyOrders = session.Advanced.Lazily.Load<Company>(new String[] { COMPANY1_ID, COMPANY2_ID });
                    Assert.False(lazyOrders.IsValueCreated);
                    Company[] orders = lazyOrders.Value;
                    Assert.Equal(2, orders.Length);
                    Assert.Equal(COMPANY1_ID, orders[0].Id);
                    Assert.Equal(COMPANY2_ID, orders[1].Id);
                }
            }
        }

        [Fact(Skip = "TODO: Lazy Not Implemented")]
        public void CanExecuteAllPendingLazyOperations()
        {
            const string COMPANY1_ID = "companies/1";
            const string COMPANY2_ID = "companies/2";

            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.Put(
                    COMPANY1_ID,
                    null,
                    RavenJObject.FromObject(new Company { Id = COMPANY1_ID }),
                    new RavenJObject()
                    );
                store.DatabaseCommands.Put(
                    COMPANY2_ID,
                    null,
                    RavenJObject.FromObject(new Company { Id = COMPANY2_ID }),
                    new RavenJObject()
                    );

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
                using (var session = store.OpenNewSession())
                {
                    var commands = new []
                    {
                        new Dictionary<string, object>()
                        {
                            [Constants.Command.Key] = "company1",
                            [Constants.Command.Method] = "PUT",
                            [Constants.Command.Document] = new DynamicJsonValue() { ["Name"] = "company 1" },
                            [Constants.Command.Etag] = null
                        },
                        new Dictionary<string, object>()
                        {
                            [Constants.Command.Key] = "company2",
                            [Constants.Command.Method] = "PUT",
                            [Constants.Command.Document] = new DynamicJsonValue() { ["Name"] = "company 2" },
                            [Constants.Command.Etag] = null
                        },
                        new Dictionary<string, object>()
                        {
                            [Constants.Command.Key] = "company1",
                            [Constants.Command.Method] = "DELETE",
                            [Constants.Command.Document] = null,
                            [Constants.Command.Etag] = null
                        }
                    };

                    session.Advanced.Defer(commands);

                    session.SaveChanges();
                    Assert.Null(session.Load<Company>("company1"));
                    Assert.NotNull(session.Load<Company>("company2"));
                }
            }
        }

        [Fact(Skip = "TODO: AggressivelyCacheFor Not Implemented without json")]
        public void CanAggressivelyCacheFor()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    session.Store(new User { Id = "users/1", Name = "Name" });
                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    Assert.Equal(0, session.Advanced.NumberOfRequests);
                    session.Load<User>("users/1");
                    //Assert.Equal(0, store.JsonRequestFactory.NumberOfCachedRequests);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }

                using (var session = store.OpenNewSession())
                {
                    session.Load<User>("users/1");
                    //Assert.Equal(1, store.JsonRequestFactory.NumberOfCachedRequests);
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
