using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace NewClientTests.NewClient
{
    public class Store : RavenTestBase
    {
        [Fact]
        public void Store_Document()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User { Name = "RavenDB" }, "users/1");
                    newSession.SaveChanges();
                    var user = newSession.Load<User>("users/1");
                    Assert.NotNull(user);
                    Assert.Equal(user.Name, "RavenDB");
                }
            }
        }

        [Fact]
        public void Store_Documents()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User { Name = "RavenDB" }, "users/1");
                    newSession.Store(new User { Name = "Hibernating Rhinos" }, "users/2");
                    newSession.SaveChanges();
                    var user = newSession.Load<User>(new[] { "users/1", "users/2" });
                    Assert.Equal(user.Length, 2);
                }
            }
        }

       /* [Fact]
        public void Store_Document_without_id_prop()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    var user1 = new UserName { Name = "RavenDB"};
                    newSession.Store(user1);
                    newSession.SaveChanges();
                   
                }
            }
        }

        public class UserName
        {
            public string Name { get; set; }
        }*/
    }
}

