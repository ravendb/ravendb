using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Documents.Patching
{
    public class PatchAndDeleteByCollection : RavenTestBase
    {
        [Fact]
        public void CanDeleteCollection()
        {
            using (var store = GetDocumentStore())
            {
                using (var x = store.OpenSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        x.Store(new User { }, "users/");
                    }
                    x.SaveChanges();
                }
                store.DatabaseCommands.CreateRequest("/collections/docs?name=users", HttpMethod.Delete).ExecuteRequest();
                var sp = Stopwatch.StartNew();

                var timeout = Debugger.IsAttached ? 60 * 10000 : 10000;

                while (sp.ElapsedMilliseconds < timeout)
                {
                    var databaseStatistics = store.DatabaseCommands.GetStatistics();
                    if (databaseStatistics.CountOfDocuments == 0)
                        return;

                    Thread.Sleep(25);
                }
                Assert.False(true, "There are still documents after 1 second");
            }
        }

        [Fact]
        public void CanPatchCollection()
        {
            using (var store = GetDocumentStore())
            {
                using (var x = store.OpenSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        x.Store(new User { }, "users/");
                    }
                    x.SaveChanges();
                }
                var httpJsonRequest = store.DatabaseCommands.CreateRequest("/collections/docs?name=users", new HttpMethod("PATCH"));
                httpJsonRequest.WriteAsync(@"
{
    'Script': 'this.Name = __document_id'
}
").Wait();
                httpJsonRequest.ExecuteRequest();
                var sp = Stopwatch.StartNew();

                var timeout = Debugger.IsAttached ? 60 * 10000 : 10000;

                while (sp.ElapsedMilliseconds < timeout)
                {
                    var databaseStatistics = store.DatabaseCommands.GetStatistics();
                    if (databaseStatistics.LastDocEtag >= 200)
                        break;
                    Thread.Sleep(25);
                }
                Assert.Equal(100, store.DatabaseCommands.GetStatistics().CountOfDocuments);
                using (var x = store.OpenSession())
                {
                    var users = x.Load<User>(Enumerable.Range(1, 100).Select(i => "users/" + i));
                    Assert.Equal(100, users.Length);

                    foreach (var user in users)
                    {
                        Assert.NotNull(user.Name);
                    }
                }
            }
        }
    }
}