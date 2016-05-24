using Raven.Client.Connection.Implementation;
using Raven.Client.Document;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client;
using Raven.Client.Extensions;
using Xunit;
using Raven.Tests.Notifications;

namespace FastTests.Client.Subscriptions
{
    public class Subscriptions : RavenTestBase
    {
        [Fact]
        public async Task CreateSubscription()
        {
            using (var store = await GetDocumentStore())
            {
                var subscriptionManager = new DocumentSubscriptions(store);

                var subscriptionCriteria = new Raven.Abstractions.Data.SubscriptionCriteria
                {
                    Collection = "People",
                    FilterJavaScript = " var a = 'c';",
                    KeyStartsWith = "/"
                };
                var subsId = subscriptionManager.Create(subscriptionCriteria);

                var subscriptionsConfig = subscriptionManager.GetSubscriptions(0, 10);

                Assert.Equal(1, subscriptionsConfig.Count);
                Assert.Equal(subscriptionCriteria.Collection, subscriptionsConfig[0].Criteria.Collection);
                Assert.Equal(subscriptionCriteria.FilterJavaScript, subscriptionsConfig[0].Criteria.FilterJavaScript);
                Assert.Equal(subscriptionCriteria.KeyStartsWith, subscriptionsConfig[0].Criteria.KeyStartsWith);
                Assert.Equal(0, subscriptionsConfig[0].AckEtag);
                Assert.Equal(subsId, subscriptionsConfig[0].SubscriptionId);
            }
        }

        public class Thing
        {
            public string Name { get; set; }
        }

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
                    session.Store(new Thing
                    {
                        Name = $"Thing"
                    });
                    session.SaveChanges();
                }

                var lastEtag = store.GetLastWrittenEtag()??0;
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 5; i++)
                    {
                        session.Store(new Thing
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
                var subscription = subscriptionManager.Open<Thing>(subsId, new SubscriptionConnectionOptions()
                {

                });
                var list = new List<Thing>();
                subscription.Subscribe<Thing>(x =>
                {
                    list.Add(x);
                });

                await AsyncSpin(() => list.Count == 5, 60000).ConfigureAwait(false);
                
                Assert.Equal(list.Count, 5);

               /* list.Clear();

                using (var session = store.OpenAsyncSession())
                {
                    for (var i = 0; i < 5; i++)
                    {
                        await session.StoreAsync(new Thing
                        {
                            Name = $"ThingNo{i}"
                        }).ConfigureAwait(false);
                    }
                    await session.SaveChangesAsync().ConfigureAwait(false);
                }
                await AsyncSpin(() => list.Count == 5, 60000).ConfigureAwait(false);
                Assert.Equal(list.Count, 5);*/
            }
        }
    }
}