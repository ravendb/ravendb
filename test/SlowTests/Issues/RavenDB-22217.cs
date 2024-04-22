using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Nito.AsyncEx;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Util;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;
public class RavenDB_22217: RavenTestBase
{
    public RavenDB_22217(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task Index_Update_With_Subscription_Update_In_The_Correct_Order()
    {
        DoNotReuseServer();

        using (var store = GetDocumentStore())
        {
            var mreBeforeAcknowledgeSubscriptionBatch = new AsyncManualResetEvent(false);
            var mreBeforeExecutingDatabaseRecordChange = new AsyncManualResetEvent(false);

            var testingStuff = Server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly();
            testingStuff.DelayNotifyFeaturesAboutStateChange = () =>
            {
                mreBeforeAcknowledgeSubscriptionBatch.Set();
                mreBeforeExecutingDatabaseRecordChange.Wait();
            };

            var subscriptionCreationParams = new SubscriptionCreationOptions
            {
                Query = "from People"
            };

            var subName = await store.Subscriptions.CreateAsync(subscriptionCreationParams);
            var subscriptionState = await store.Subscriptions.GetSubscriptionStateAsync(subName);

            var indexDefinition = new Index();
            var saveIndexTask = indexDefinition.ExecuteAsync(store);

            await mreBeforeAcknowledgeSubscriptionBatch.WaitAsync();
            
            var database = await GetDatabase(store.Database);
            var command = new AcknowledgeSubscriptionBatchCommand(store.Database, RaftIdGenerator.NewId())
            {
                ChangeVector = "A:1",
                NodeTag = Server.ServerStore.NodeTag,
                HasHighlyAvailableTasks = true,
                SubscriptionId = subscriptionState.SubscriptionId,
                SubscriptionName = subscriptionState.SubscriptionName,
                LastTimeServerMadeProgressWithDocuments = DateTime.UtcNow,
                LastKnownSubscriptionChangeVector = null,
                DatabaseName = database.Name,
            };

            var (etag, _) = await Server.ServerStore.SendToLeaderAsync(command);
            await database.RachisLogIndexNotifications.WaitForIndexNotification(etag, Server.ServerStore.Engine.OperationTimeout);

            try
            {
                var delayTask = Task.Delay(200);
                var task = await Task.WhenAny(saveIndexTask, delayTask);

                Assert.Equal(delayTask, task);

                Assert.True(saveIndexTask.IsCompleted == false);

                var index = database.IndexStore.GetIndex(indexDefinition.IndexName);
                Assert.Null(index);

                mreBeforeExecutingDatabaseRecordChange.Set();
                await saveIndexTask;

                index = database.IndexStore.GetIndex(indexDefinition.IndexName);
                Assert.NotNull(index);
            }
            finally
            {
                mreBeforeExecutingDatabaseRecordChange.Set();
            }
        }
    }
}

