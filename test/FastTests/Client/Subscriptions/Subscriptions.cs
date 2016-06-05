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
    public class Subscriptions : SubscriptionTestBase
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

        [Fact]
        public async Task BasicSusbscriptionTest()
        {
            using (var store = await GetDocumentStore().ConfigureAwait(false))
            using (var subscriptionManager = new DocumentSubscriptions(store))
            {
                CreateDocuments(store, 1);

                var lastEtag = store.GetLastWrittenEtag()??0;
                CreateDocuments(store, 5);

                var subscriptionCriteria = new Raven.Abstractions.Data.SubscriptionCriteria
                {
                    Collection = "Things",
                    FilterJavaScript = " var a = 'c';",
                    KeyStartsWith = "/"
                };
                var subsId = subscriptionManager.Create(subscriptionCriteria, lastEtag);
                var subscription = subscriptionManager.Open<Thing>(subsId, new SubscriptionConnectionOptions()
                {
                    SubscriptionId = subsId
                });
                var list = new List<Thing>();
                subscription.Subscribe<Thing>(x =>
                {
                    list.Add(x);
                });

                await AsyncSpin(() => list.Count == 5, 60000).ConfigureAwait(false);
                
                Assert.Equal( 5, list.Count);
            }
        }

        [Fact]
        public async Task SubscriptionStrategyConnectIfFree()
        {
            using (var store = await GetDocumentStore().ConfigureAwait(false))
            using (var subscriptionManager = new DocumentSubscriptions(store))
            {
                CreateDocuments(store, 1);

                var lastEtag = store.GetLastWrittenEtag() ?? 0;
                CreateDocuments(store, 5);

                var subscriptionCriteria = new Raven.Abstractions.Data.SubscriptionCriteria
                {
                    Collection = "Things",
                    FilterJavaScript = " var a = 'c';",
                    KeyStartsWith = "/"
                };
                var subsId = subscriptionManager.Create(subscriptionCriteria, lastEtag);
                var acceptedSubscription = subscriptionManager.Open<Thing>(subsId, new SubscriptionConnectionOptions()
                {
                    SubscriptionId = subsId
                });

                var acceptedSusbscriptionList = new List<Thing>();
                var rejectedSusbscriptionList = new List<Thing>();
                acceptedSubscription.Subscribe(x =>
                {
                    acceptedSusbscriptionList.Add(x);
                });

                // wait until we know that connection was established
                await AsyncSpin(() => acceptedSusbscriptionList.Count >1, 60000).ConfigureAwait(false);

                // open second subscription
                var rejectedSusbscription = subscriptionManager.Open<Thing>(subsId, new SubscriptionConnectionOptions()
                {
                    SubscriptionId = subsId
                });

                rejectedSusbscription.Subscribe(x =>
                {
                    rejectedSusbscriptionList.Add(x);
                });

                await AsyncSpin(() => acceptedSusbscriptionList.Count == 5, 60000).ConfigureAwait(false);

                Assert.Equal(5, acceptedSusbscriptionList.Count);
                Assert.Equal(0, rejectedSusbscriptionList.Count);
            }
        }

        [Fact]
        public async Task SubscriptionWaitStrategy()
        {
            using (var store = await GetDocumentStore().ConfigureAwait(false))
            using (var subscriptionManager = new DocumentSubscriptions(store))
            {
                CreateDocuments(store, 1);

                var lastEtag = store.GetLastWrittenEtag() ?? 0;
                CreateDocuments(store, 5);

                var subscriptionCriteria = new Raven.Abstractions.Data.SubscriptionCriteria
                {
                    Collection = "Things",
                    FilterJavaScript = " var a = 'c';",
                    KeyStartsWith = "/"
                };
                var subsId = subscriptionManager.Create(subscriptionCriteria, lastEtag);
                var acceptedSubscription = subscriptionManager.Open<Thing>(subsId, new SubscriptionConnectionOptions()
                {
                    SubscriptionId = subsId
                });

                var acceptedSusbscriptionList = new List<Thing>();
                var waitingSubscriptionList = new List<Thing>();

                var ackSent = false;
                acceptedSubscription.AfterAcknowledgment += () => ackSent = true;

                acceptedSubscription.Subscribe(x =>
                {
                    acceptedSusbscriptionList.Add(x);
                    AsyncHelpers.RunSync(() => Task.Delay(20));
                });

                var sp = Stopwatch.StartNew();
                // wait until we know that connection was established
                await AsyncSpin(() => acceptedSusbscriptionList.Count > 1, 60000).ConfigureAwait(false);
                
                // open second subscription
                var waitingSubscription = subscriptionManager.Open<Thing>(subsId, new SubscriptionConnectionOptions()
                {
                    SubscriptionId = subsId,
                    Strategy = SubscriptionOpeningStrategy.WaitForFree
                });

                waitingSubscription.Subscribe(x =>
                {
                    waitingSubscriptionList.Add(x);
                });

                sp.Restart();
                await AsyncSpin(() => acceptedSusbscriptionList.Count == 5, 60000).ConfigureAwait(false);
                Assert.Equal(5, acceptedSusbscriptionList.Count);
                Assert.Equal(0, waitingSubscriptionList.Count);

                

                sp.Restart();
                await AsyncSpin(() => ackSent, 60000).ConfigureAwait(false);
                sp.Restart();
                acceptedSubscription.Dispose();
                sp.Restart();
                CreateDocuments(store,5);
                sp.Restart();
                await AsyncSpin(() => waitingSubscriptionList.Count == 5, 60000).ConfigureAwait(false);
                Assert.Equal(5, acceptedSusbscriptionList.Count);
                Assert.Equal(5, waitingSubscriptionList.Count);
            }
        }

        [Fact]
        public async Task SubscriptionSimpleTakeOverStrategy()
        {
            using (var store = await GetDocumentStore().ConfigureAwait(false))
            using (var subscriptionManager = new DocumentSubscriptions(store))
            {
                CreateDocuments(store, 1);

                var lastEtag = store.GetLastWrittenEtag() ?? 0;
                CreateDocuments(store, 5);

                var subscriptionCriteria = new Raven.Abstractions.Data.SubscriptionCriteria
                {
                    Collection = "Things",
                    FilterJavaScript = " var a = 'c';",
                    KeyStartsWith = "/"
                };
                var subsId = subscriptionManager.Create(subscriptionCriteria, lastEtag);
                var acceptedSubscription = subscriptionManager.Open<Thing>(subsId, new SubscriptionConnectionOptions()
                {
                    SubscriptionId = subsId
                });

                var acceptedSusbscriptionList = new List<Thing>();
                var takingOverSubscriptionList = new List<Thing>();

                acceptedSubscription.Subscribe(x =>
                {
                    acceptedSusbscriptionList.Add(x);
                });

                bool batchProccessedByFirstSubscription = false;
                acceptedSubscription.AfterAcknowledgment += () => batchProccessedByFirstSubscription = true;
                // wait until we know that connection was established

                
                await AsyncSpin(() => acceptedSusbscriptionList.Count ==5&& batchProccessedByFirstSubscription, 60000).ConfigureAwait(false);
                
                // open second subscription
                var takingOverSubscription = subscriptionManager.Open<Thing>(subsId, new SubscriptionConnectionOptions()
                {
                    SubscriptionId = subsId,
                    Strategy = SubscriptionOpeningStrategy.TakeOver
                });
                bool secondSubscriptionStartedProccessing = false;
                takingOverSubscription.BeforeBatch += () =>
                {
                    secondSubscriptionStartedProccessing = true;
                };

                takingOverSubscription.Subscribe(x => takingOverSubscriptionList.Add(x));


                
                await AsyncSpin(() => secondSubscriptionStartedProccessing, 60000).ConfigureAwait(false);
                Assert.True(secondSubscriptionStartedProccessing);
                
                CreateDocuments(store, 5);
                
                await AsyncSpin(() => (acceptedSusbscriptionList.Count + takingOverSubscriptionList.Count) == 10, 60000).ConfigureAwait(false);
                
                Assert.Equal(5, acceptedSusbscriptionList.Count );
                Assert.Equal(5, takingOverSubscriptionList.Count);
            }
        }
    }
}