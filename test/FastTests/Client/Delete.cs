using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class Delete : RavenTestBase
    {
        public Delete(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Delete_Document_By_entity(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User { Name = "RavenDB" }, "users/1");
                    newSession.SaveChanges();
                    var user = newSession.Load<User>("users/1");
                    Assert.NotNull(user);
                    newSession.Delete(user);
                    newSession.SaveChanges();
                    var nullUser = newSession.Load<User>("users/1");
                    Assert.Null(nullUser);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Delete_Documents_By_id(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User {Name = "RavenDB"}, "users/1");
                    newSession.SaveChanges();
                    var user = newSession.Load<User>("users/1");
                    Assert.NotNull(user);
                    newSession.Delete("users/1");
                    newSession.SaveChanges();
                    var nullUser = newSession.Load<User>("users/1");
                    Assert.Null(nullUser);

                }
            }
        }
    }
}
