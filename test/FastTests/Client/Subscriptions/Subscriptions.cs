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
                    KeyStartsWith = "/",
                    StartEtag = 0
                };
                var subsId = subscriptionManager.Create(subscriptionCriteria);

                var subscriptionsConfig = subscriptionManager.GetSubscriptions(0, 10);

                Assert.Equal(1, subscriptionsConfig.Count);
                Assert.Equal(subscriptionCriteria.Collection, subscriptionsConfig[0].Criteria.Collection);
                Assert.Equal(subscriptionCriteria.FilterJavaScript, subscriptionsConfig[0].Criteria.FilterJavaScript);
                Assert.Equal(subscriptionCriteria.KeyStartsWith, subscriptionsConfig[0].Criteria.KeyStartsWith);
                Assert.Equal(subscriptionCriteria.StartEtag, subscriptionsConfig[0].Criteria.StartEtag);
                Assert.Equal(subsId, subscriptionsConfig[0].SubscriptionId);
            }
        }

        public class Thing
        {
            public string Name { get; set; }
        }

        [Fact]
        public async Task BasicSusbscriptionTest()
        {
            using (var store = await GetDocumentStore())
            using (var subscriptionManager = new DocumentSubscriptions(store))
            {
                var sp = Stopwatch.StartNew();
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
                Console.WriteLine($"Stored First 5 Docs time {sp.ElapsedMilliseconds}");
                sp.Restart();

                var subscriptionCriteria = new Raven.Abstractions.Data.SubscriptionCriteria
                {
                    Collection = "Things",
                    FilterJavaScript = " var a = 'c';",
                    KeyStartsWith = "/",
                    StartEtag = 0
                };
                var subsId = subscriptionManager.Create(subscriptionCriteria);
                var subscription = subscriptionManager.Open<Thing>(subsId, new SubscriptionConnectionOptions()
                {

                });
                var list = new List<Thing>();
                subscription.Subscribe<Thing>(x =>
                {
                    Console.WriteLine($"Proccessing {x.Name}");
                    Task.Delay(1000).Wait();
                    list.Add(x);
                    Console.WriteLine($"Finished Proccessing {x.Name}");
                });

                Console.WriteLine($"Opened Subscription time {sp.ElapsedMilliseconds}");
                sp.Restart();

                SpinWait.SpinUntil(() => list.Count == 5, 60000);
                Console.WriteLine($"Waited For Subscription to end {sp.ElapsedMilliseconds}");
                sp.Restart();
                Assert.Equal(list.Count, 5);
                for (var j = 0; j < 2; j++)
                {
                    list.Clear();
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
                    Console.ForegroundColor = ConsoleColor.Blue;
                    Console.WriteLine($"Stored {j}th 5 Docs time {sp.ElapsedMilliseconds}");
                    Console.ForegroundColor = ConsoleColor.White;
                    sp.Restart();
                    SpinWait.SpinUntil(() => list.Count == 5, 60000);
                    Console.ForegroundColor = ConsoleColor.Blue;
                    Console.WriteLine($"Waited For {j}th Subscription to end {sp.ElapsedMilliseconds}");
                    Console.ForegroundColor = ConsoleColor.White;
                    sp.Restart();
                    Assert.Equal(list.Count, 5);
                }
            }
        }
    }
}