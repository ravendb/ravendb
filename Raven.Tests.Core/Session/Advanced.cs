using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
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
                    Assert.Equal("PUT attempted on document '" + entityId + "' using a non current etag", e.Message);
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
                    Assert.Equal("/databases/"+store.DefaultDatabase+"/docs/companies/1", uri.AbsolutePath);
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
    }
}
