using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_8464 : RavenTestBase
    {
        public RavenDB_8464(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(6);

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task AfterAckShouldHappenAfterTheEndOfBatchRun(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var subsId = store.Subscriptions.Create<User>(x => true);

                string cv;
                
                using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(subsId))
                {
                    var amre = new AsyncManualResetEvent();


                    subscription.AfterAcknowledgment += batch =>
                    {
                        amre.Set();

                        return Task.CompletedTask;
                    };

                    using (var session = store.OpenSession())
                    {
                        var entity = new User();
                        session.Store(entity, "users/1");
                        session.SaveChanges();
                        cv = (string)session.Advanced.GetMetadataFor(entity)[Constants.Documents.Metadata.ChangeVector];
                    }

                    var task = subscription.Run(x => { });


                    Assert.True(await amre.WaitAsync(_reasonableWaitTime));
                }

                using (store.GetRequestExecutor().ContextPool.AllocateOperationContext(out var context))
                {
                    store.GetRequestExecutor().Execute(new CreateSubscriptionCommand(store.Conventions, new SubscriptionCreationOptions()
                    {
                        ChangeVector = "DoNotChange",
                        Name = subsId,
                        Query = "From Shipments"
                    }, subsId), context);
                }
                using (Server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (context.OpenReadTransaction())
                {
                    if (options.DatabaseMode == RavenDatabaseMode.Sharded)
                    {
                        var orch = Sharding.GetOrchestrator(store.Database);
                        var s = orch.SubscriptionsStorage.GetSubscriptionByName(context, subsId);
                        var docShard = await Sharding.GetShardNumberForAsync(store, "users/1");
                        Assert.Equal(cv, s.ShardingState.ChangeVectorForNextBatchStartingPointPerShard[ShardHelper.ToShardName(store.Database, docShard)]);
                        Assert.Equal("From Shipments", s.Query);
                    }
                    else
                    {
                        var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                        var s = db.SubscriptionStorage.GetSubscriptionWithDataByNameFromServerStore(context, subsId, history: false, running: false);
                        Assert.Equal(cv, s.ChangeVectorForNextBatchStartingPoint);
                        Assert.Equal("From Shipments", s.Query);
                    }
                }
            }
        }
    }
}
