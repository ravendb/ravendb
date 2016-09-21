using System.Collections.Generic;
using System.Linq;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.NewClient
{
    public class Store : RavenTestBase
    {
        [Fact]
        public void Store_Document()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenNewSession())
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
                using (var newSession = store.OpenNewSession())
                {
                    newSession.Store(new User { Name = "RavenDB" }, "users/1");
                    newSession.Store(new User { Name = "Hibernating Rhinos" }, "users/2");
                    newSession.SaveChanges();
                    var user = newSession.Load<User>(new[] { "users/1", "users/2" });
                    Assert.Equal(user.Length, 2);
                }
            }
        }
    }
}

