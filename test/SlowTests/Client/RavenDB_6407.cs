using FastTests;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client
{
    public class RavenDB_6407 : RavenTestBase
    {
        public RavenDB_6407(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Name;
            public string Email;
            public int Votes;
        }
        
        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void WillPreserverRemovedPropertiesAcrossSaves(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Email = "user@example.com",
                        Name = "Arava Eini"
                    }, "users/1");
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    session.Advanced.Defer(new PatchCommandData("users/1", null, new PatchRequest
                    {
                        Script = @"
var parts= this.Name.split(' ');
this.FirstName = parts[0];
this.LastName = parts[1];
"
                    }, null));
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    user.Votes++;
                    session.SaveChanges();
                }
                WaitForUserToContinueTheTest(store);
                using (var session = store.OpenSession())
                {
                    var users = session.Advanced.RawQuery<User>("from Users where FirstName = 'Arava'").Count();
                    Assert.Equal(1, users);
                }
            }
        }
    }
}
