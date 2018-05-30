using System;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Extensions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB8384 : RavenTestBase
    {
        [Fact]
        public async Task CanUseLoadInSubscriptions()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Employee
                    {
                        FirstName = "Arava",
                        Id = "dogs/arava",
                        LastName = "Eini",
                    });
                    await session.StoreAsync(new Employee
                    {
                        FirstName = "Oscar",
                        ReportsTo = "dogs/arava",
                        LastName = "Eini",
                        Id = "dogs/oscar"
                    });
                    await session.SaveChangesAsync();
                }

                var id = await store.Subscriptions.CreateAsync(options: new SubscriptionCreationOptions
                {
                    Query = @"
from Employees as e
where e.ReportsTo != null
load e.ReportsTo as r
select {
    Name: e.FirstName,
    Manager: r.FirstName
}
"
                });

                var sub = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                });
                
                bool done = await sub.Run(batch =>
                {
                    Assert.Equal(1, batch.Items.Count);
                    Assert.Equal("Oscar", batch.Items[0].Result.Name);
                    Assert.Equal("Arava", batch.Items[0].Result.Manager);
                    sub.Dispose();
                }).WaitWithTimeout(TimeSpan.FromSeconds(30));

                Assert.True(done);
            }
        }
        
    }
}
