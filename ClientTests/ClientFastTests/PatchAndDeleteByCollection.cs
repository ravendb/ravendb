using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading;
using NewClientTests;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace NewClientTests.NewClient.FastTests.Patching
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

                JsonOperationContext context;
                store.GetRequestExecuter(store.DefaultDatabase).ContextPool.AllocateOperationContext(out context);
                var Command = new DeleteByCollectionCommand()
                {
                    CollectionName = "users"
                };
                store.GetRequestExecuter(store.DefaultDatabase).Execute(Command, context);

                var sp = Stopwatch.StartNew();

                var timeout = Debugger.IsAttached ? 60 * 10000 : 10000;

                while (sp.ElapsedMilliseconds < timeout)
                {
                    var getStatsCommand = new GetStatisticsCommand();
                    store.GetRequestExecuter(store.DefaultDatabase).Execute(getStatsCommand, context);
                    var databaseStatistics = getStatsCommand.Result;
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

                JsonOperationContext context;
                store.GetRequestExecuter(store.DefaultDatabase).ContextPool.AllocateOperationContext(out context);

                var patchByCollectionOperation = new PatchByCollectionOperation(context);
                var patchCommand = patchByCollectionOperation.CreateRequest("users", 
                    new PatchRequest { Script = " this.Name = __document_id;" }, store);
                if (patchCommand != null)
                    store.GetRequestExecuter(store.DefaultDatabase).Execute(patchCommand, context);

                var sp = Stopwatch.StartNew();

                var timeout = Debugger.IsAttached ? 60 * 10000 : 10000;

                GetStatisticsCommand getStatsCommand;
                DatabaseStatistics databaseStatistics;
                while (sp.ElapsedMilliseconds < timeout)
                {
                    getStatsCommand = new GetStatisticsCommand();
                    store.GetRequestExecuter(store.DefaultDatabase).Execute(getStatsCommand, context);
                    databaseStatistics = getStatsCommand.Result;
                    if (databaseStatistics.LastDocEtag >= 200)
                        break;
                    Thread.Sleep(25);
                }

                getStatsCommand = new GetStatisticsCommand();
                store.GetRequestExecuter(store.DefaultDatabase).Execute(getStatsCommand, context);
                databaseStatistics = getStatsCommand.Result;

                Assert.Equal(100, databaseStatistics.CountOfDocuments);
                using (var x = store.OpenSession())
                {
                    var users = x.Load<User>(Enumerable.Range(1, 100).Select(i => "users/" + i));
                    Assert.Equal(100, users.Count);

                    foreach (var user in users.Values)
                    {
                        Assert.NotNull(user.Name);
                    }
                }
            }
        }
    }
}