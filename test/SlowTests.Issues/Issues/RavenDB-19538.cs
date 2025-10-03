using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_19538 : RavenTestBase
    {
        private readonly int _reasonableWaitTimeInMs = 15_000;

        public RavenDB_19538(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanModifyMetadataInSubscriptionBatch(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var sub = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions<User> { Filter = user => user.Count > 0 });

                var subscription = store.Subscriptions.GetSubscriptionWorker<User>(
                    new SubscriptionWorkerOptions(sub) { TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(1), MaxDocsPerBatch = 2 });

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
                        var c = 0;
                        foreach (var item in x.Items)
                        {
                            var meta = session.Advanced.GetMetadataFor(item.Result);
                            if (meta.Count == 5)
                            {
                                // update only if we didn't update it before
                                meta.Add("Test1", date1);
                                item.Metadata["Test2"] = date2;
                                c++;
                            }
                        }

                        session.SaveChanges();
                        if (c > 0)
                            docs.Signal(x.NumberOfItemsInBatch);
                    }
                });

                Assert.True(docs.Wait(_reasonableWaitTimeInMs));
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
