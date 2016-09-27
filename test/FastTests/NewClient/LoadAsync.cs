using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.NewClient
{
    public class LoadAsync : RavenTestBase
    {
        [Fact]
        public async Task Load_Document_By_id_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenNewAsyncSession())
                {
                    var user = await newSession.LoadAsync<User>("users/1");
                    Assert.NotNull(user);
                    Assert.Equal(user.Name, "RavenDB");
                }
            }
        }

        [Fact]
        public async Task Load_Documents_By_ids_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.Store(new User { Name = "Hibernating Rhinos" }, "users/2");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenNewAsyncSession())
                {
                    var user = await newSession.LoadAsync<User>(new[] { "users/1", "users/2" });
                    Assert.Equal(user.Length, 2);
                }
            }
        }

        [Fact]
        public async Task Load_Document_By_ValueType_id_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.Store(new User { Name = "Hibernating Rhinos" }, "users/2");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenNewAsyncSession())
                {
                    var user = await newSession.LoadAsync<User>(2);
                    Assert.NotNull(user);
                    Assert.Equal(user.Name, "Hibernating Rhinos");
                }
            }
        }

        [Fact]
        public async Task Load_Documents_By_ValueType_ids_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.Store(new User { Name = "Hibernating Rhinos" }, "users/2");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenNewAsyncSession())
                {
                    var users = await newSession.LoadAsync<User>(CancellationToken.None, 1,2);
                    Assert.Equal(users.Length, 2);
                }
            }
        }

        [Fact]
        public async Task Load_Documents_By_IEnumerable_ValueType_ids_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.Store(new User { Name = "Hibernating Rhinos" }, "users/2");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenNewAsyncSession())
                {
                    var users = await newSession.LoadAsync<User>(new List<System.ValueType> { 1, 2 });
                    Assert.Equal(users.Length, 2);

                }
            }
        }
    }
}