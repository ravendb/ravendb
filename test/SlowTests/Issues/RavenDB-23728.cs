using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.ServerWide;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_23728 : RavenTestBase
    {
        public RavenDB_23728(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task EnforceRevisionsConfigurationLastsForever()
        {
            using var store = GetDocumentStore();

            // 1 Document

            var configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration()
            };
            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);

            for (int i = 0; i < 2; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = i.ToString() }, "Users/1");
                    await session.SaveChangesAsync();
                }
            }

            await AssertRevisionsCountAsync(store, "Users/1", 2);

            configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration()
                {
                    MinimumRevisionsToKeep = 1
                }
            };

            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);
            var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            database.DocumentsStorage.RevisionsStorage.SizeLimitInBytes = 0;

            await WaitWithTimeoutAsync( () => EnforceConfiguration(store), timeout: TimeSpan.FromSeconds(15));

            await AssertRevisionsCountAsync(store, "Users/1", 1);



            // 2 Document

            configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration()
            };
            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);

            for (int i = 2; i <= 4; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = i.ToString() }, "Users/1");
                    await session.SaveChangesAsync();
                }
            }
            
            for (int i = 0; i < 2; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = i.ToString() }, "Users/2");
                    await session.SaveChangesAsync();
                }
            }

            await AssertRevisionsCountAsync(store, "Users/1", 4);
            await AssertRevisionsCountAsync(store, "Users/2", 2);


            configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration()
                {
                    MinimumRevisionsToKeep = 1
                }
            };
            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);

            await WaitWithTimeoutAsync(() => EnforceConfiguration(store), timeout: TimeSpan.FromSeconds(15));

            await AssertRevisionsCountAsync(store, "Users/1", 1);
            await AssertRevisionsCountAsync(store, "Users/2", 1);
        }

        private static async Task WaitWithTimeoutAsync(Func<Task> act, TimeSpan timeout)
        {
            using (var cancellationTokenSource = new CancellationTokenSource())
            {
                var task = act();
                var timeoutTask = Task.Delay(timeout, cancellationTokenSource.Token);
                if (await Task.WhenAny(task, timeoutTask) == task)
                {
                    cancellationTokenSource.Cancel(); // Cancel delay task if operation completes within timeout
                    await task; // Propagate any exceptions thrown by the task
                }
                else
                {
                    throw new TimeoutException("The operation has timed out.");
                }
            }
        }

        private static async Task AssertRevisionsCountAsync(DocumentStore store, string id, int expectedCount)
        {
            using (var session = store.OpenAsyncSession())
            {
                var count = await session.Advanced.Revisions.GetCountForAsync(id);
                Assert.Equal(expectedCount, count);
            }
        }

        private async Task EnforceConfiguration(DocumentStore store, long timeout = 15_000)
        {
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            using (var token = new OperationCancelToken(TimeSpan.FromMilliseconds(timeout), db.DatabaseShutdown, CancellationToken.None))
                await db.DocumentsStorage.RevisionsStorage.EnforceConfigurationAsync(_ => { }, includeForceCreated: false, token: token);
        }


        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
