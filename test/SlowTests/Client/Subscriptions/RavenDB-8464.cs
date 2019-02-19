using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Xunit;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_8464 : RavenTestBase
    {
        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(6);

        [Fact]
        public async Task AfterAckShouldHappenAfterTheEndOfBatchRun()
        {
            using (var store = GetDocumentStore())
            {
                var subsId = store.Subscriptions.Create<User>(x => true);

                var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
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
                        session.Store(entity);
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

                using (db.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var s = db.SubscriptionStorage.GetSubscription(context, null, subsId, false);
                    Assert.Equal(cv, s.ChangeVectorForNextBatchStartingPoint);
                    Assert.Equal("From Shipments", s.Query);
                }
            }
        }
    }
}
