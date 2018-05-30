using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_8383:RavenTestBase
    {
        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(10);
        [Fact]
        public async Task RunningSubscriptionOnNonExistantCollectionShould_NOT_Throw()
        {
            using (var store = GetDocumentStore())
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
