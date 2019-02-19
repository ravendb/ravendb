using System;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Xunit;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_11250:RavenTestBase
    {
        [Fact] 
        public async Task SubscriptionShouldNotAllowStartWorkingIfItsStoreIsNotInitialized()
        {
            using (var store = GetDocumentStore())
            {                
                using (var unInitializedStore = new DocumentStore
                {
                    Urls= store.Urls,
                    Database = store.Database
                })
                {
                    await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        await unInitializedStore.Subscriptions.CreateAsync<Order>(x => x.Lines.Count > 5);
                    });                    
                }

                var subsName = await store.Subscriptions.CreateAsync<Order>(x => x.Lines.Count > 5);

                using (var unInitializedStore = new DocumentStore
                {
                    Urls = store.Urls,
                    Database = store.Database
                })
                {
                    Assert.Throws<InvalidOperationException>(() =>
                    {
                        unInitializedStore.Subscriptions.GetSubscriptionWorker<Order>(subsName);
                    });
                }

                using (var unInitializedStore = new DocumentStore
                {
                    Urls = store.Urls,
                    Database = store.Database
                })
                {
                    await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        await unInitializedStore.Subscriptions.DeleteAsync(subsName);
                    });
                }

                using (var unInitializedStore = new DocumentStore
                {
                    Urls = store.Urls,
                    Database = store.Database
                })
                {
                    await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        await unInitializedStore.Subscriptions.GetSubscriptionStateAsync(subsName);
                    });
                }

                using (var unInitializedStore = new DocumentStore
                {
                    Urls = store.Urls,
                    Database = store.Database
                })
                {
                    await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        await unInitializedStore.Subscriptions.GetSubscriptionsAsync(0,1);
                    });
                }

                using (var unInitializedStore = new DocumentStore
                {
                    Urls = store.Urls,
                    Database = store.Database
                })
                {
                    await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        await unInitializedStore.Subscriptions.DropConnectionAsync(subsName);
                    });
                }
            }
        }
    }
}
