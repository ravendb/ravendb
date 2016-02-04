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
    public class RavenDB_3193 : RavenTest
    {
        [Fact]
        public void ShouldRespectCollectionCriteria()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        session.Store(new Company());
                        session.Store(new User());
                        session.Store(new Address());
                    }

                    session.SaveChanges();
                }

                var id = store.Subscriptions.Create(new SubscriptionCriteria
                {
                    BelongsToAnyCollection = new[] { "Users", "Addresses" }
                });

                var subscription = store.Subscriptions.Open(id, new SubscriptionConnectionOptions
                {
                    BatchOptions = new SubscriptionBatchOptions { MaxDocCount = 31 }
                });

                var docs = new List<RavenJObject>();

                subscription.Subscribe(docs.Add);

                Assert.True(SpinWait.SpinUntil(() => docs.Count >= 200, TimeSpan.FromSeconds(60)));


                foreach (var jsonDocument in docs)
                {
                    var collection = jsonDocument[Constants.Metadata].Value<string>(Constants.RavenEntityName);
                    Assert.True(collection == "Users" || collection == "Addresses");
                }
            }
        } 
    }
}
