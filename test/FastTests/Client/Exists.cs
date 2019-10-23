using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class Exists : RavenTestBase
    {
        public Exists(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CheckIfDocumentExists()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Idan" }, "users/1");
                    session.Store(new User { Name = "Shalom" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.True(session.Advanced.Exists("users/1"));
                    Assert.False(session.Advanced.Exists("users/10"));
                    session.Load<User>("users/2");
                    Assert.True(session.Advanced.Exists("users/2"));
                }
            }
        }
    }
}
