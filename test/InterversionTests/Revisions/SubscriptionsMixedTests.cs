using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace InterversionTests.Revisions
{
    public class SubscriptionsMixedTests : InterversionTestBase
    {
        public SubscriptionsMixedTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Subscriptions | RavenTestCategory.Interversion, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task RevisionsSubscription_OnOldServer_StreamsToCurrentClient()
        {
            var customSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false",
                [RavenConfiguration.GetKey(x => x.Licensing.EulaAccepted)] = "true",
            };

            var oldNode = await GetServerAsync(Versions.PrePRv62, customSettings: customSettings);

            var dbName = GetDatabaseName() + "-subs";
            using var store = new DocumentStore
            {
                Urls = new[] { oldNode.Url },
                Database = dbName
            };
            store.Initialize();
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(dbName)
            {
                Settings = { [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false" }
            }));

            await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 100,
                    PurgeOnDelete = false
                }
            }));

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "v0" }, "users/1");
                await session.SaveChangesAsync();
            }
            for (int i = 1; i <= 2; i++)
            {
                using var session = store.OpenAsyncSession();
                var u = await session.LoadAsync<User>("users/1");
                u.Name = "v" + i;
                await session.SaveChangesAsync();
            }

            var subsName = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions<Revision<User>>());

            using var worker = store.Subscriptions.GetSubscriptionWorker<Revision<User>>(new SubscriptionWorkerOptions(subsName)
            {
                TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(1)
            });

            var receivedCvs = new System.Collections.Concurrent.ConcurrentBag<string>();
            var tcs = new TaskCompletionSource<bool>();
            var task = worker.Run(batch =>
            {
                foreach (var item in batch.Items)
                {
                    var cv = item.ChangeVector;
                    if (cv != null)
                        receivedCvs.Add(cv);
                }
                if (receivedCvs.Count >= 3)
                    tcs.TrySetResult(true);
            });

            var done = await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(30)));
            Assert.Same(tcs.Task, done);

            Assert.Equal(3, receivedCvs.Count);
            Assert.Equal(3, receivedCvs.Distinct().Count());
        }
    }
}
