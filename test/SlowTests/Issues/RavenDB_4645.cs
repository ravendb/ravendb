// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3193.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using FastTests;
using FastTests.Server.Documents.Notifications;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_4645 : RavenTestBase
    {
        [Fact]
        public void ShouldStopPullingTaskWhenSubscriptionIsDeleted()
        {
            using (var store = GetDocumentStore())
            {
                // insert few documents and fetch them using subscription
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new Company());
                    }

                    session.SaveChanges();
                }

                var subscriptionCreationParams = new SubscriptionCreationParams
                {
                    Criteria = new SubscriptionCriteria("Companies")
                };
                var id = store.Subscriptions.Create(subscriptionCreationParams);

                var subscription = store.Subscriptions.Open(new SubscriptionConnectionOptions(id)
                {
                    MaxDocsPerBatch = 5
                });

                var docs = new List<dynamic>();

                subscription.Subscribe(docs.Add);
                subscription.Start();

                Assert.True(SpinWait.SpinUntil(() => docs.Count == 10, TimeSpan.FromSeconds(60)));

                // all documents were fetched - time to delete subscription

                store.Subscriptions.Delete(id);

                // verify if we don't get new items

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 2; i++)
                    {
                        session.Store(new Company());
                    }

                    session.SaveChanges();
                }

                // wait 3 seconds for new documents - we shouldn't get any
                Assert.False(SpinWait.SpinUntil(() => docs.Count != 10, TimeSpan.FromSeconds(3)));
            }
        }
    }
}
