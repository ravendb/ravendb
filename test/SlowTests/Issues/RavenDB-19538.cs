using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_19538 : RavenTestBase
    {
        private int _reasonableWaitTime = 3000;

        public RavenDB_19538(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanModifyMetadataInSubscriptionBatch()
        {
            using (var store = GetDocumentStore())
            {
                var sub = store.Subscriptions.Create(new SubscriptionCreationOptions<User> { Filter = user => user.Count > 0 });

                var subscription = store.Subscriptions.GetSubscriptionWorker<User>(
                    new SubscriptionWorkerOptions(sub) { TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5), MaxDocsPerBatch = 2 });

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 2; i++)
                    {
                        var user = new User { Count = 1, Id = $"Users/{i}" };
                        session.Store(user);
                    }

                    session.SaveChanges();
                }

                var docs = new CountdownEvent(2);
                var date1 = DateTime.UtcNow.AddHours(1).ToString();
                var date2 = DateTime.UtcNow.AddHours(2).ToString();

                _ = subscription.Run(x =>
                {
                    using (var session = x.OpenSession())
                    {
                        foreach (var item in x.Items)
                        {
                            var meta = session.Advanced.GetMetadataFor(item.Result);
                            meta["Test1"] = date1;
                            item.Metadata["Test2"] = date2;
                        }

                        session.SaveChanges();
                        docs.Signal(x.NumberOfItemsInBatch);
                    }
                });

                docs.Wait(_reasonableWaitTime);
                for (int i = 0; i < 2; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var u = await session.LoadAsync<User>($"Users/{i}");
                        var meta = session.Advanced.GetMetadataFor(u);
                        var metaDate1 = meta["Test1"];
                        var metaDate2 = meta["Test2"];
                        Assert.Equal(date1, metaDate1);
                        Assert.Equal(date2, metaDate2);
                    }
                }
            }
        }
    }
}
