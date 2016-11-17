using System;
using System.Dynamic;
using System.Threading.Tasks;
using Raven.NewClient.Client.Exceptions;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Abstractions.Util;
using SlowTests.Core.Utils.Entities;
using SlowTests.Core.Utils.Transformers;
using Xunit;

namespace NewClientTests.NewClient.Raven.Tests.Core.Session
{
    public class Crud : RavenTestBase
    {
#if DNXCORE50
        public Crud(TestServerFixture fixture)
            : base(fixture)
        {

        }
#endif

        [Fact(Skip = "NotImplementedException")]
        public async Task CanSaveAndLoad()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" });
                    await session.StoreAsync(new User { Name = "Arek" }, "users/arek");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenNewAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.NotNull(user);
                    Assert.Equal("Fitzchak", user.Name);

                    user = await session.LoadAsync<User>("users/arek");
                    Assert.NotNull(user);
                    Assert.Equal("Arek", user.Name);
                }
            }
        }

        [Fact]
        public async Task CanSaveAndLoadDynamicDocuments()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewAsyncSession())
                {
                    dynamic user = new ExpandoObject();
                    user.Id = "users/1";
                    user.Name = "Arek";

                    await session.StoreAsync(user);

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenNewAsyncSession())
                {
                    var user = await session.LoadAsync<dynamic>("users/1");

                    Assert.NotNull(user);
                    Assert.Equal("Arek", user.Name);
                }
            }
        }

        [Fact]
        public Task CanLoadWithTransformer()
        {
            using (var store = GetDocumentStore())
            {
                new PostWithContentTransformer().Execute(store);

                using (var session = store.OpenNewSession())
                {
                    session.Store(new Post
                    {
                        Id = "posts/1"
                    });

                    session.Store(new PostContent
                    {
                        Id = "posts/1/content",
                        Text = "Lorem ipsum..."
                    });

                    session.SaveChanges();

                    var result = session.Load<PostWithContentTransformer, PostWithContentTransformer.Result>("posts/1");

                    Assert.Equal("Lorem ipsum...", result.Content);
                }
            }
            return new CompletedTask();
        }

        [Fact(Skip = "NotImplementedException")]
        public async Task CanDelete()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewAsyncSession())
                {
                    var entity1 = new User { Name = "Andy A" };
                    var entity2 = new User { Name = "Andy B" };
                    var entity3 = new User { Name = "Andy C" };

                    await session.StoreAsync(entity1);
                    await session.StoreAsync(entity2);
                    await session.StoreAsync(entity3);

                    await session.SaveChangesAsync();

                    session.Delete(entity1);
                    session.Delete("users/2");
                    session.Delete<User>(3);

                    await session.SaveChangesAsync();

                    var users = await session.LoadAsync<User>(new[] { "users/1", "users/2", "users/3" });

                    users.ForEach(Assert.Null);
                }
            }
        }

        [Fact(Skip = "NotImplementedException")]
        public async Task CanLoadWithInclude()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewAsyncSession())
                {
                    var address = new Address { City = "London", Country = "UK" };
                    await session.StoreAsync(address);
                    await session.StoreAsync(new User { Name = "Adam", AddressId = session.Advanced.GetDocumentId(address) });

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenNewAsyncSession())
                {
                    var user = await session.Include<User>(x => x.AddressId).LoadAsync<User>("users/1");

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var address = await session.LoadAsync<Address>(user.AddressId);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.NotNull(address);
                    Assert.Equal("London", address.City);
                }

                using (var session = store.OpenNewAsyncSession())
                {
                    var user = await session.Include("AddressId").LoadAsync<User>("users/1");

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var address = await session.LoadAsync<Address>(user.AddressId);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.NotNull(address);
                    Assert.Equal("London", address.City);
                }
            }
        }

        [Fact]
        public void DeletingEntityThatIsNotTrackedShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    var e = Assert.Throws<InvalidOperationException>(() => session.Delete(new User()));
                    Assert.Equal("SlowTests.Core.Utils.Entities.User is not associated with the session, cannot delete unknown entity instance", e.Message);
                }
            }
        }

        [Fact]
        public void DeletingEntityByIdThatIsNotTrackedShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    session.Store(new User { Id = "users/1", Name = "John" });
                    session.Store(new User { Id = "users/2", Name = "Jonathan" });
                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    session.Delete("users/1");
                    session.Delete<User>(2);
                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    Assert.Null(session.Load<User>("users/1"));
                    Assert.Null(session.Load<User>("users/2"));
                }
            }
        }

        [Fact]
        public void StoringDocumentWithTheSameIdInTheSameSessionShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    var user = new User { Id = "users/1", Name = "User1" };

                    session.Store(user);
                    session.SaveChanges();

                    user = new User { Id = "users/1", Name = "User2" };

                    var e = Assert.Throws<NonUniqueObjectException>(() => session.Store(user));
                    Assert.Equal("Attempted to associate a different object with id 'users/1'.", e.Message);
                }
            }
        }
    }
}
