using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Client.Connection.Profiling;
using Raven.Json.Linq;
using Raven.Tests.Core.Utils.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Core.Session
{
    public class Advanced : RavenCoreTestBase
    {
#if DNXCORE50
        public Advanced(TestServerFixture fixture)
            : base(fixture)
        {

        }
#endif

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
                    Assert.Equal("AddressId", ((DocumentsChanges[])whatChanged["users/1"])[0].FieldName);
                    Assert.Equal("", ((DocumentsChanges[])whatChanged["users/1"])[0].FieldOldValue);
                    Assert.Equal("addresses/1", ((DocumentsChanges[])whatChanged["users/1"])[0].FieldNewValue);

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

                    var u = store.DatabaseCommands.Get("users/1");
                    u.DataAsJson["Name"] = "Jonathan";
                    store.DatabaseCommands.Put("users/1", u.Etag, u.DataAsJson, u.Metadata);

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
        public void CanGetDocumentUrl()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
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

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>(companyId);
                    var result = session.Advanced.GetMetadataFor<Company>(company);
                    Assert.NotNull(result);
                    Assert.Equal(attrVal, result.Value<string>(attrKey));
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
        public void CanMarkReadOnly()
        {
            const string categoryName = "MarkReadOnlyTest";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
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
                    Assert.True(session.Advanced.GetMetadataFor<Company>(company).Value<bool>("Raven-Read-Only"));
                }
            }
        }

        [Fact]
        public void CanGetEtagFor()
        {
            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.Put(
                    "companies/1",
                    null,
                    RavenJObject.FromObject(new Company { Id = "companies/1" }),
                    new RavenJObject()
                    );

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    Assert.Equal("01000000-0000-0001-0000-000000000001", session.Advanced.GetEtagFor<Company>(company).ToString());
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

        [Fact]
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
                using (var session = store.OpenSession())
                {
                    var commands = new ICommandData[]
                    {
                        new PutCommandData
                        {
                            Document =
                                RavenJObject.FromObject(new Company {Name = "company 1"}),
                            Etag = null,
                            Key = "company1",
                            Metadata = new RavenJObject(),
                        },
                        new PutCommandData
                        {
                            Document =
                                RavenJObject.FromObject(new Company {Name = "company 2"}),
                            Etag = null,
                            Key = "company2",
                            Metadata = new RavenJObject(),
                        }
                    };

                    session.Advanced.Defer(commands);
                    session.Advanced.Defer(new DeleteCommandData { Key = "company1" });

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

#if !DNXCORE50
                Server.Server.ResetNumberOfRequests();
#endif

                using (var session = store.OpenSession())
                {
                    Assert.Equal(0, session.Advanced.NumberOfRequests);
                    session.Load<User>("users/1");
                    Assert.Equal(0, store.JsonRequestFactory.NumberOfCachedRequests);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }

                using (var session = store.OpenSession())
                {
                    session.Load<User>("users/1");
                    Assert.Equal(1, store.JsonRequestFactory.NumberOfCachedRequests);
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
