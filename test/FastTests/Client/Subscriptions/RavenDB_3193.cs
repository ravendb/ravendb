using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.AspNetCore.Http.Internal;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Client.Subscriptions
{
    public class RavenDB_3193 : RavenTestBase
    {
        [Fact]
        public void ShouldRespectCollectionCriteria()
        {
            using (var store = GetDocumentStore())
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

                var id = store.Subscriptions.Create<User>();

                var subscription = store.Subscriptions.Open(new SubscriptionConnectionOptions(id)
                {
                    MaxDocsPerBatch = 31
                });

                var docs = new List<dynamic>();

                subscription.Run(x=>
                {
                    foreach (var item in x.Items)
                    {
                        var collection = item.Metadata[Raven.Client.Constants.Documents.Metadata.Collection];
                        if (collection.Equals("Users"))
                            docs.Add(item.Result);
                    }
                });

                Assert.True(SpinWait.SpinUntil(() => docs.Count >= 100, TimeSpan.FromSeconds(60)));                 
            }
        }
    }
}
