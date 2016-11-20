using System.Threading.Tasks;
using NewClientTests;
using Raven.NewClient.Abstractions.Util;
using SlowTests.Core.Utils.Entities;
using Xunit;

using Company = SlowTests.Core.Utils.Entities.Company;
using User = SlowTests.Core.Utils.Entities.User;

namespace SlowTests.NewClient.Raven.Tests.Core.Session
{
    public class Keys : RavenTestBase
    {
#if DNXCORE50
        public Keys(TestServerFixture fixture)  
            : base(fixture)
        {

        }
#endif

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
                    Assert.Equal("UserWithoutIds/1", id);
                }

                using (var session = store.OpenSession())
                {
                    var user = new UserWithoutId { Name = "John" };
                    Assert.Null(session.Advanced.GetDocumentId(user));
                }
            }
        }

        [Fact(Skip = "TODO: use IDatabaseCommands")]
        public async Task KeyGeneration()
        {
            using (var store = GetDocumentStore())
            {
                store.Conventions.RegisterIdConvention<User>((databaseName, commands, entity) => "abc");
                store.Conventions.RegisterAsyncIdConvention<User>((databaseName, commands, entity) => new CompletedTask<string>("def"));

                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "John" };
                    session.Store(user);

                    Assert.Equal("abc", user.Id);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var user = new User { Name = "John" };
                    await session.StoreAsync(user);

                    Assert.Equal("def", user.Id);
                }

                Assert.Equal("abc", store.Conventions.GenerateDocumentKey(store.DefaultDatabase, store.DatabaseCommands, new User()));
                Assert.Equal("def", await store.Conventions.GenerateDocumentKeyAsync(store.DefaultDatabase, store.AsyncDatabaseCommands, new User()));

                Assert.Equal("addresses/1", store.Conventions.GenerateDocumentKey(store.DefaultDatabase, store.DatabaseCommands, new Address()));
                Assert.Equal("companies/1", await store.Conventions.GenerateDocumentKeyAsync(store.DefaultDatabase, store.AsyncDatabaseCommands, new Company()));
            }
        }

        [Fact(Skip = "TODO: use IDatabaseCommands")]
        public void KeyGenerationOnLoad()
        {
            using (var store = GetDocumentStore())
            {
                store.Conventions.RegisterIdConvention<TShirt>((databaseName, commands, entity) => "ts/" + entity.ReleaseYear);
                store.Conventions.RegisterIdLoadConvention<TShirt>(id => "ts/" + id);

                using (var session = store.OpenSession())
                {
                    var shirt = new TShirt { Manufacturer = "Test1", ReleaseYear = 1999 };
                    session.Store(shirt);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var shirt = session.Load<TShirt>(1999);
                    Assert.Equal(shirt.Manufacturer, "Test1");
                    Assert.Equal(shirt.ReleaseYear, 1999);
                }
            }
        }
    }
}
