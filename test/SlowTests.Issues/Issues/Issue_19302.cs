using System.Threading.Tasks;
using FastTests;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class Issue_19302 : RavenTestBase
    {
        public Issue_19302(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ConditionalLoadAsync_InSameSession(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.SaveChanges();
                }

                string cv;
                using (var newSession = store.OpenAsyncSession())
                {
                    var user = await newSession.LoadAsync<User>("users/1");
                    cv = newSession.Advanced.GetChangeVectorFor(user);
                    Assert.NotNull(user);
                    Assert.Equal(user.Name, "RavenDB");
                    user.Name = "RavenDB Async";
                    await newSession.SaveChangesAsync();

                    var conditional = await newSession.Advanced.ConditionalLoadAsync<User>("users/1", cv);

                    Assert.Equal(conditional.Entity.Name, "RavenDB Async");
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void ConditionalLoad_InSameSession(Options options)
        {
            using (var store = GetDocumentStore(options))
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
                    user.Name = "RavenDB Sync";
                    newSession.SaveChanges();

                    var conditional = newSession.Advanced.ConditionalLoad<User>("users/1", cv);

                    Assert.Equal(conditional.Entity.Name, "RavenDB Sync");
                }
            }
        }
    }
}
