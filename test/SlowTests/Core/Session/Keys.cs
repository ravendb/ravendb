// -----------------------------------------------------------------------
//  <copyright file="Keys.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;

using FastTests;
using Xunit;

using Address = SlowTests.Core.Utils.Entities.Address;
using Company = SlowTests.Core.Utils.Entities.Company;
using TShirt = SlowTests.Core.Utils.Entities.TShirt;
using User = SlowTests.Core.Utils.Entities.User;
using UserWithoutId = SlowTests.Core.Utils.Entities.UserWithoutId;

namespace SlowTests.Core.Session
{
    public class Keys : RavenTestBase
    {
        [Fact]
        public void GetDocumentId()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new UserWithoutId { Name = "John" };
                    session.Store(user);

                    var id = session.Advanced.GetDocumentId(user);
                    Assert.Equal("UserWithoutIds/1-A", id);
                }

                using (var session = store.OpenSession())
                {
                    var user = new UserWithoutId { Name = "John" };
                    Assert.Null(session.Advanced.GetDocumentId(user));
                }
            }
        }

        [Fact]
        public async Task KeyGeneration()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.RegisterAsyncIdConvention<User>((databaseName, entity) => Task.FromResult("def/" + entity.Name));
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "John" };
                    session.Store(user);

                    Assert.Equal("def/John", user.Id);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var user = new User { Name = "Bob" };
                    await session.StoreAsync(user);

                    Assert.Equal("def/Bob", user.Id);
                }

                Assert.Equal("def/", store.Conventions.GenerateDocumentId(store.Database, new User()));
                Assert.Equal("def/", await store.Conventions.GenerateDocumentIdAsync(store.Database, new User()));

                Assert.Equal("addresses/1-A", store.Conventions.GenerateDocumentId(store.Database, new Address()));
                Assert.Equal("companies/1-A", await store.Conventions.GenerateDocumentIdAsync(store.Database, new Company()));
            }
        }

        [Fact]
        public void KeyGenerationOnLoad()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.RegisterAsyncIdConvention<TShirt>((databaseName, entity) => Task.FromResult("ts/" + entity.ReleaseYear));
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    var shirt = new TShirt { Manufacturer = "Test1", ReleaseYear = 1999 };
                    session.Store(shirt);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var shirt = session.Load<TShirt>("ts/1999");
                    Assert.Equal(shirt.Manufacturer, "Test1");
                    Assert.Equal(shirt.ReleaseYear, 1999);
                }
            }
        }
    }
}
