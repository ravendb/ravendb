using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_8383:RavenTestBase
    {
        public RavenDB_8383(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(10);

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task RunningSubscriptionOnNonExistentCollectionShould_NOT_Throw(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var subscriptionName = await store.Subscriptions.CreateAsync<User>();
                var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new Raven.Client.Documents.Subscriptions.SubscriptionWorkerOptions(subscriptionName)
                {
                    CloseWhenNoDocsLeft = true,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                });
                await Assert.ThrowsAsync<SubscriptionClosedException>(() => subscription.Run(x => { }).WaitAsync(_reasonableWaitTime));
            }
        }
    }
}
