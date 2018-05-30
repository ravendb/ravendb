using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_9512:RavenTestBase
    {
        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(6);
        
        [Fact]
        public async Task AbortWhenNoDocsLeft()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User());
                    await session.SaveChangesAsync();
                }
                
                var sn = await store.Subscriptions.CreateAsync<User>();
                var worker = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(sn)
                {
                    CloseWhenNoDocsLeft = true,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                });
                
                var st = worker.Run(x => { });
                
                Assert.True(await Assert.ThrowsAsync<SubscriptionClosedException>(()=>st).WaitAsync(_reasonableWaitTime));}
        }
        
    }
}
