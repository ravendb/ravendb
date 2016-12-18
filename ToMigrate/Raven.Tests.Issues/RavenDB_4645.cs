// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3193.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4645 : RavenTest
    {
        [Fact]
        public void ShouldStopPullingTaskWhenSubscriptionIsDeleted()
        {
            using (var store = NewDocumentStore())
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

                var id = store.Subscriptions.Create(new SubscriptionCriteria());

                var subscription = store.Subscriptions.Open(id, new SubscriptionConnectionOptions
                {
                    BatchOptions = new SubscriptionBatchOptions { MaxDocCount = 5 }
                });

                var docs = new List<RavenJObject>();

                subscription.Subscribe(docs.Add);

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
