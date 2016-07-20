// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3612.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3612 : RavenTest
    {
        private readonly TimeSpan waitForEvent = TimeSpan.FromSeconds(20);

        [Fact]
        public void BeforeBatch_Is_Called()
        {
            using (var store = NewDocumentStore())
            {
                SaveThreeUsers(store);

                var subscription = CreateUserSubscription(store);

                var beforeBatch = new ManualResetEvent(false);
                subscription.BeforeBatch += () => beforeBatch.Set();

                subscription.Subscribe(x => { });
                subscription.Subscribe(x => { });

                Assert.True(beforeBatch.WaitOne(waitForEvent));
            }
        }

        [Fact]
        public void AfterBatch_Gets_Count_Of_Processed_Docs()
        {
            using (var store = NewDocumentStore())
            {
                SaveThreeUsers(store);

                var subscription = CreateUserSubscription(store);

                int processed = -1;

                var afterBatchCalled = new ManualResetEvent(false);
                subscription.AfterBatch += count =>
                {
                    processed = count;
                    afterBatchCalled.Set();
                };

                subscription.Subscribe(x => { });
                subscription.Subscribe(x => { });

                Assert.True(afterBatchCalled.WaitOne(waitForEvent));
                Assert.Equal(3, processed);
            }
        }

        [Fact]
        public void BeforeAcknowledgment_Can_Prevent_Batch_Acknowledgment()
        {
            using (var store = NewDocumentStore())
            {
                SaveThreeUsers(store);

                var subscription = CreateUserSubscription(store);

                var afterBatchCalled = new ManualResetEvent(false);
                subscription.AfterBatch += count => afterBatchCalled.Set();

                var beforeAcknowledgmentCalled = new ManualResetEvent(false);
                subscription.BeforeAcknowledgment += () =>
                {
                    beforeAcknowledgmentCalled.Set();

                    return false;
                };

                bool afterAcknowledgment = false;
                subscription.AfterAcknowledgment += etag =>
                {
                    afterAcknowledgment = true;
                };

                subscription.Subscribe(x => { });

                Assert.True(afterBatchCalled.WaitOne(waitForEvent));

                Assert.True(beforeAcknowledgmentCalled.WaitOne(1));
                Assert.False(afterAcknowledgment);

                var subscriptionConfigs = store.Subscriptions.GetSubscriptions(0, 10);
                Assert.Equal(1, subscriptionConfigs.Count);

                Assert.Equal(Etag.Empty, subscriptionConfigs[0].AckEtag);
            }
        }

        [Fact]
        public void AfterAcknowledgment_Gets_Etag_Of_Last_Processed_Doc()
        {
            using (var store = NewDocumentStore())
            {
                SaveThreeUsers(store);

                var subscription = CreateUserSubscription(store);

                Etag ackEtag = null;

                var afterAcknowledgmentCalled = new ManualResetEvent(false);
                subscription.AfterAcknowledgment += etag =>
                {
                    ackEtag = etag;
                    afterAcknowledgmentCalled.Set();
                };

                subscription.Subscribe(x => { });

                Assert.True(afterAcknowledgmentCalled.WaitOne(TimeSpan.FromMinutes(5)));

                Assert.NotEqual(Etag.Empty, ackEtag);
                var subscriptionConfigs = store.Subscriptions.GetSubscriptions(0, 10);
                Assert.Equal(1, subscriptionConfigs.Count);

                Assert.Equal(subscriptionConfigs[0].AckEtag, ackEtag);
            }
        }

        private static Subscription<User> CreateUserSubscription(IDocumentStore store)
        {
            var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());
            var subscription = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions());
            return subscription;
        }

        private static void SaveThreeUsers(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User());
                session.Store(new User());
                session.Store(new User());

                session.SaveChanges();
            }
        }
    }
}
