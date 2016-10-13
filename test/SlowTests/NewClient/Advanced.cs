using System;
using System.Threading.Tasks;

using FastTests;

using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Client.Exceptions;
using Raven.Json.Linq;

using Xunit;

using Company = SlowTests.Core.Utils.Entities.Company;
using User = SlowTests.Core.Utils.Entities.User;

namespace SlowTests.NewClient
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
                    Assert.True(session.Advanced.GetMetadataFor<Company>(company).Value<bool>("Raven-Read-Only"));
                }
            }
        }
    }
}
