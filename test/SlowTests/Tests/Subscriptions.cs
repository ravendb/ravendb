using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Document;
using Raven.Tests.Notifications;
using Xunit;

namespace SlowTests.Tests
{
    public class Subscriptions: RavenTestBase
    {
        private async Task AsyncSpin(Func<bool> action, int amount)
        {
            var sp = Stopwatch.StartNew();

            while (sp.ElapsedMilliseconds < amount && action() == false)
            {
                await Task.Delay(50).ConfigureAwait(false);
            }
        }

        [Fact]
        public async Task BasicSusbscriptionTest()
        {
            using (var store = await GetDocumentStore().ConfigureAwait(false))
            using (var subscriptionManager = new DocumentSubscriptions(store))
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new FastTests.Client.Subscriptions.Subscriptions.Thing
                    {
                        Name = $"Thing"
                    });
                    session.SaveChanges();
                }

                var lastEtag = store.GetLastWrittenEtag() ?? 0;
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 5; i++)
                    {
                        session.Store(new FastTests.Client.Subscriptions.Subscriptions.Thing
                        {
                            Name = $"ThingNo{i}"
                        });
                    }
                    session.SaveChanges();
                }

                var subscriptionCriteria = new Raven.Abstractions.Data.SubscriptionCriteria
                {
                    Collection = "Things",
                    FilterJavaScript = " var a = 'c';",
                    KeyStartsWith = "/"
                };
                var subsId = subscriptionManager.Create(subscriptionCriteria, lastEtag);
                var subscription = subscriptionManager.Open<FastTests.Client.Subscriptions.Subscriptions.Thing>(subsId, new SubscriptionConnectionOptions());
                var list = new List<FastTests.Client.Subscriptions.Subscriptions.Thing>();
                subscription.Subscribe<FastTests.Client.Subscriptions.Subscriptions.Thing>(x =>
                {
                    AsyncHelpers.RunSync(() => Task.Delay(1000));
                    list.Add(x);
                });
                

                await AsyncSpin(() => list.Count == 5, 60000).ConfigureAwait(false);

                Assert.Equal(list.Count, 5);

                for (var j = 0; j < 2; j++)
                {
                    list.Clear();

                    using (var session = store.OpenAsyncSession())
                    {
                        for (var i = 0; i < 5; i++)
                        {
                            await session.StoreAsync(new FastTests.Client.Subscriptions.Subscriptions.Thing
                            {
                                Name = $"ThingNo{i} Iteration {j}"
                            }).ConfigureAwait(false);
                        }
                        await session.SaveChangesAsync().ConfigureAwait(false);
                    }
                    await AsyncSpin(() => list.Count == 5, 60000).ConfigureAwait(false);
                    Assert.Equal(list.Count, 5);
                }
            }
        }
    }
}
