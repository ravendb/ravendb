using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_3917 : RavenTestBase
    {
        [Fact]
        public async Task SmugglerShouldNotExportImportSubscribtionIdentities()
        {
            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = x => x + "store1"
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = x => x + "store2"
            }))
            {
                var subscriptionId = store1.Subscriptions.Create<User>(new SubscriptionCreationOptions<User>()
                {
                    Name = "Foo"
                });

                var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), store2.Smuggler);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var subscription = store2.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(subscriptionId)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                });
                await Assert.ThrowsAsync<SubscriptionDoesNotExistException>(() => subscription.Run(x => { }));
            }
        }
    }
}
