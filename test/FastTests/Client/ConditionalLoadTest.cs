using System;
using System.Threading.Tasks;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class ConditionalLoadTest : RavenTestBase
    {
        public ConditionalLoadTest(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ConditionalLoad_CanGetDocumentById()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.SaveChanges();
                }

                string cv;
                using (var newSession = store.OpenSession())
                {
                    var user = newSession.Load<User>("users/1");
                    cv = newSession.Advanced.GetChangeVectorFor(user);
                    Assert.NotNull(user);
                    Assert.Equal(user.Name, "RavenDB");
                    user.Name = "RavenDB 5.1";
                    newSession.SaveChanges();
                }

                using (var newestSession = store.OpenSession())
                {
                    var user = newestSession.Advanced.ConditionalLoad<User>("users/1", cv);
                    Assert.Equal(user.Entity.Name, "RavenDB 5.1");
                    Assert.NotNull(user.ChangeVector);
                    Assert.NotEqual(cv, user.ChangeVector);
                }
            }
        }

        [Fact]
        public void ConditionalLoad_GetNotModifiedDocumentByIdShouldReturnNull()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.SaveChanges();
                }

                string cv;
                using (var newSession = store.OpenSession())
                {
                    var user = newSession.Load<User>("users/1");
                    Assert.NotNull(user);
                    Assert.Equal(user.Name, "RavenDB");
                    user.Name = "RavenDB 5.1";
                    newSession.SaveChanges();
                    cv = newSession.Advanced.GetChangeVectorFor(user);
                }

                using (var newestSession = store.OpenSession())
                {
                    var user = newestSession.Advanced.ConditionalLoad<User>("users/1", cv);
                    Assert.Equal(default, user.Entity);
                    Assert.Equal(cv, user.ChangeVector);
                }
            }
        }

        [Fact]
        public void ConditionalLoad_NonExistsDocumentShouldReturnNull()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.SaveChanges();
                }

                string cv;
                using (var newSession = store.OpenSession())
                {
                    var user = newSession.Load<User>("users/1");
                    cv = newSession.Advanced.GetChangeVectorFor(user);
                    Assert.NotNull(user);
                    Assert.Equal(user.Name, "RavenDB");
                    user.Name = "RavenDB 5.1";
                    newSession.SaveChanges();
                }

                using (var newestSession = store.OpenSession())
                {
                    Assert.Throws<InvalidOperationException>(() => newestSession.Advanced.ConditionalLoad<User>("users/2", null));
                    Assert.Equal(default, newestSession.Advanced.ConditionalLoad<User>("users/2", cv));
                    
                    Assert.True(newestSession.Advanced.IsLoaded("users/2"));
                    
                    var expected = newestSession.Advanced.NumberOfRequests;
                    _ = newestSession.Load<User>("users/2");
                    Assert.Equal(expected, newestSession.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task ConditionalLoadAsync_CanGetDocumentById()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "RavenDB" }, "users/1");
                    await session.SaveChangesAsync();
                }

                string cv;
                using (var newSession = store.OpenAsyncSession())
                {
                    var user = await newSession.LoadAsync<User>("users/1");
                    cv = newSession.Advanced.GetChangeVectorFor(user);
                    Assert.NotNull(user);
                    Assert.Equal(user.Name, "RavenDB");
                    user.Name = "RavenDB 5.1";
                    await newSession.SaveChangesAsync();
                }

                using (var newestSession = store.OpenAsyncSession())
                {
                    var user = await newestSession.Advanced.ConditionalLoadAsync<User>("users/1", cv);
                    Assert.Equal(user.Entity.Name, "RavenDB 5.1");
                    Assert.NotNull(user.ChangeVector);
                    Assert.NotEqual(cv, user.ChangeVector);
                }
            }
        }

        [Fact]
        public async Task ConditionalLoadAsync_GetNotModifiedDocumentByIdShouldReturnNull()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "RavenDB" }, "users/1");
                    await session.SaveChangesAsync();
                }

                string cv;
                using (var newSession = store.OpenAsyncSession())
                {
                    var user = await newSession.LoadAsync<User>("users/1");
                    Assert.NotNull(user);
                    Assert.Equal(user.Name, "RavenDB");
                    user.Name = "RavenDB 5.1";
                    await newSession.SaveChangesAsync();
                    cv = newSession.Advanced.GetChangeVectorFor(user);
                }

                using (var newestSession = store.OpenAsyncSession())
                {
                    var user = await newestSession.Advanced.ConditionalLoadAsync<User>("users/1", cv);
                    Assert.Equal(default, user.Entity);
                    Assert.Equal(cv, user.ChangeVector);
                }
            }
        }

        [Fact]
        public async Task ConditionalLoadAsync_NonExistsDocumentShouldReturnNull()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "RavenDB" }, "users/1");
                    await session.SaveChangesAsync();
                }

                string cv;
                using (var newSession = store.OpenAsyncSession())
                {
                    var user = await newSession.LoadAsync<User>("users/1");
                    cv = newSession.Advanced.GetChangeVectorFor(user);
                    Assert.NotNull(user);
                    Assert.Equal(user.Name, "RavenDB");
                    user.Name = "RavenDB 5.1";
                    await newSession.SaveChangesAsync();
                }

                using (var newestSession = store.OpenAsyncSession())
                {
                    await Assert.ThrowsAsync<InvalidOperationException>(async () => await newestSession.Advanced.ConditionalLoadAsync<User>("users/2", null));
                    Assert.Equal(default, await newestSession.Advanced.ConditionalLoadAsync<User>("users/2", cv));
                    
                    Assert.True(newestSession.Advanced.IsLoaded("users/2"));
                    var expected = newestSession.Advanced.NumberOfRequests;
                    _ = await newestSession.LoadAsync<User>("users/2");
                    Assert.Equal(expected, newestSession.Advanced.NumberOfRequests);
                }
            }
        }
    }
}
