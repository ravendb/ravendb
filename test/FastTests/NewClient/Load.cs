using System.Collections.Generic;
using System.Linq;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.NewClient
{
    public class Load : RavenTestBase
    {
        [Fact]
        public void Load_Document_By_id()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenNewSession())
                {
                    var user = newSession.Load<User>("users/1");
                    Assert.NotNull(user);
                    Assert.Equal(user.Name, "RavenDB");
                }
            }
        }

        [Fact]
        public void Load_Documents_By_ids()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.Store(new User { Name = "Hibernating Rhinos" }, "users/2");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenNewSession())
                {
                    var user = newSession.Load<User>(new[] { "users/1", "users/2" });
                    Assert.Equal(user.Length, 2);
                }
            }
        }

        [Fact]
        public void Load_Document_By_ValueType_id()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.Store(new User { Name = "Hibernating Rhinos" }, "users/2");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenNewSession())
                {
                    var user = newSession.Load<User>(2);
                    Assert.NotNull(user);
                    Assert.Equal(user.Name, "Hibernating Rhinos");
                }
            }
        }

        [Fact]
        public void Load_Documents_By_ValueType_ids()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.Store(new User { Name = "Hibernating Rhinos" }, "users/2");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenNewSession())
                {
                    var users = newSession.Load<User>(1, 2);
                    Assert.Equal(users.Length, 2);
                }
            }
        }

        [Fact]
        public void Load_Documents_By_IEnumerable_ValueType_ids()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.Store(new User { Name = "Hibernating Rhinos" }, "users/2");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenNewSession())
                {
                    var users = newSession.Load<User>(new List<System.ValueType> { 1, 2 });
                    Assert.Equal(users.Length, 2);

                }
            }
        }
    }
}
