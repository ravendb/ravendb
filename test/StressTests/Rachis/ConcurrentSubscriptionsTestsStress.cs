using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

// ReSharper disable ClassNeverInstantiated.Local
// ReSharper disable CollectionNeverUpdated.Local
#pragma warning disable CS0649
#pragma warning disable CS0169

namespace StressTests.Rachis
{
    public class ConcurrentSubscriptionsTestsStress : ReplicationTestBase
    {
        public ConcurrentSubscriptionsTestsStress(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task ShouldClearSubscriptionInfoFromStorageAfterDatabaseDeletion()
        {
            DoNotReuseServer();
            const int expectedNumberOfDocsToResend = 7;

            string databaseName = GetDatabaseName();

            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            await Backup.HoldBackupExecutionIfNeededAndInvoke(ts: null, async () =>
            {
                using (var store = GetDocumentStore(new Options { ModifyDatabaseName = _ => databaseName }))
                {
                    var subscriptionId = await store.Subscriptions.CreateAsync<User>();
                    await using var subscriptionWorker = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(subscriptionId)
                    {
                        Strategy = SubscriptionOpeningStrategy.Concurrent,
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(2),
                        MaxDocsPerBatch = expectedNumberOfDocsToResend
                    });

                    using (var session = store.OpenSession())
                    {
                        for (int i = 0; i < 10; i++)
                            session.Store(new User { Name = $"UserNo{i}" });

                        session.SaveChanges();
                    }

                    _ = subscriptionWorker.Run(async x =>
                    {
                        await tcs.Task;
                    });

                    await AssertWaitForValueAsync(() =>
                    {
                        List<SubscriptionStorage.ResendItem> items;

                        using (Server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                        using (context.OpenReadTransaction())
                            items = SubscriptionStorage.GetResendItemsForDatabase(context, store.Database).ToList();

                        return Task.FromResult(items.Count);
                    }, expectedNumberOfDocsToResend);
                }

                // Upon disposing of the store, the database gets deleted.
                // Then we recreate the database to ensure no leftover subscription data from the previous instance.
                using (var _ = GetDocumentStore(new Options { ModifyDatabaseName = _ => databaseName }))
                {
                    List<SubscriptionStorage.ResendItem> items;

                    using (Server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                    using (context.OpenReadTransaction())
                        items = SubscriptionStorage.GetResendItemsForDatabase(context, databaseName).ToList();

                    Assert.Equal(0, items.Count);
                }
            }, tcs);
        }

        private class User
        {
            public string Name;
            public int Age;
        }
    }
}
