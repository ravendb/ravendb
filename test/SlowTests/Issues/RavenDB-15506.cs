using System;
using System.Threading.Tasks;
using FastTests;
using Tests.Infrastructure;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15506 : RavenTestBase
    {
        public RavenDB_15506(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
        }

        private class User
        {

        }

        private class Dog
        {

        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanUseInOnMetadataInSubscription(Options options)
        {
            using var store = GetDocumentStore(options);
            using (var s = store.OpenAsyncSession())
            {
                await s.StoreAsync(new Item());
                await s.StoreAsync(new User());
                await s.StoreAsync(new Dog());
                await s.SaveChangesAsync();
            }

            await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions()
            {
                Name = "TestSub",
                Query = @"from @all_docs as doc
where doc.'@metadata'.'@collection' in ('Items','Users')"
            });

            var worker = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions("TestSub")
            {
                CloseWhenNoDocsLeft = true
            });
            var items = 0;
            try
            {
                await worker.Run(batch =>
                {
                    items += batch.Items.Count;
                });
            }
            catch (SubscriptionClosedException)
            {
            }
            Assert.Equal(2, items);
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanUseMetadataUsingOr(Options options)
        {
            using var store = GetDocumentStore(options);
            using (var s = store.OpenAsyncSession())
            {
                await s.StoreAsync(new Item());
                await s.StoreAsync(new User());
                await s.StoreAsync(new Dog());
                await s.SaveChangesAsync();
            }

            await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions()
            {
                Name = "TestSub",
                Query = @"from @all_docs as doc
where doc.'@metadata'.'@collection' == 'Items'  or doc.'@metadata'.'@collection' == 'Users'"
            });

            var worker = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions("TestSub")
            {
                CloseWhenNoDocsLeft = true
            });
            var items = 0;
            try
            {
                await worker.Run(batch =>
                {
                    items += batch.Items.Count;
                });
            }
            catch (SubscriptionClosedException)
            {
            }
            Assert.Equal(2, items);
        }
    }
}
