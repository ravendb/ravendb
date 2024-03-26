using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Util;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_22008 : RavenTestBase
    {
        public RavenDB_22008(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        public async Task Index_Update_With_Subscription_Update()
        {
            using (var store = GetDocumentStore())
            {
                var indexDefinition = new Index();
                await indexDefinition.ExecuteAsync(store);

                var database = await GetDatabase(store.Database);
                var index = database.IndexStore.GetIndex(indexDefinition.IndexName);

                var subscriptionCreationParams = new SubscriptionCreationOptions
                {
                    Query = "from People"
                };

                var subName = await store.Subscriptions.CreateAsync(subscriptionCreationParams);
                var subscriptionState = await store.Subscriptions.GetSubscriptionStateAsync(subName);

                var indexDisposeMre = new ManualResetEventSlim(false);
                var mreBeforeSendingCommand = new ManualResetEventSlim(false);

                var mreWasSet = false;

                using (index.ForTestingPurposesOnly().CallDuringFinallyOfExecuteIndexing(() =>
                {
                    // simulating a long indexing batch completion
                    mreWasSet = indexDisposeMre.Wait(Server.ServerStore.Configuration.Cluster.OperationTimeout.AsTimeSpan * 2);
                }))
                {
                    database.IndexStore.ForTestingPurposesOnly().BeforeHandleDatabaseRecordChange = () =>
                    {
                        mreBeforeSendingCommand.Set();
                    };

                    var deleteIndexTask = store.Maintenance.SendAsync(new DeleteIndexOperation(indexDefinition.IndexName));

                    mreBeforeSendingCommand.Wait(Server.ServerStore.Configuration.Cluster.OperationTimeout.AsTimeSpan * 2);

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

                    indexDisposeMre.Set();
                    await deleteIndexTask;

                    Assert.True(mreWasSet);
                }
            }
        }
    }
}
