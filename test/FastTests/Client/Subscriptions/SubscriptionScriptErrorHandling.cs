using Lucene.Net;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using Raven.Client.ServerWide.Revisions;
using Raven.Client.Util;
using Xunit;

namespace FastTests.Client.Subscriptions
{
    public class SubscriptionScriptErrorHandling: RavenTestBase
    {
        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(15);

        [Fact]
        public void ValidateFailedSubscriptionScriptExceptionHandling()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionId = store.Subscriptions.Create(new SubscriptionCreationOptions<User>()
                {
                    Criteria = new SubscriptionCriteria<User>()
                    {
                        Script = "throw 'a party'"
                    }
                });

                var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId));

                var exceptions = new List<Exception>();

                var mre = new ManualResetEvent(false);
                var receivedItem = new SubscriptionBatch<User>.Item();
                subscription.Run(x =>
                {
                    foreach (var item in x.Items)
                    {
                        receivedItem = item;
                        try
                        {
                            var res = item;
                        }
                        catch (Exception e)
                        {
                            exceptions.Add(e);
                        }
                    }
                    mre.Set();
                });

                var userId = string.Empty;

                using (var session = store.OpenSession())
                {
                    var newUser = new User();
                    session.Store(newUser);
                    session.SaveChanges();
                    userId = session.Advanced.GetDocumentId(newUser);
                }

                Assert.True(mre.WaitOne(_reasonableWaitTime));
                Assert.NotNull(receivedItem);
                Assert.Throws<InvalidOperationException>(() => receivedItem.Result);
                Assert.NotNull(receivedItem.Metadata);
                Assert.Equal(receivedItem.Id, userId);
            }
        }

        [Fact]
        public void ValidateFailedRevisionsSubscriptionScriptExceptionHandling()
        {
            using (var store = GetDocumentStore())
            {

                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var configuration = new RevisionsConfiguration
                    {
                        Default = new RevisionsCollectionConfiguration
                        {
                            Active = true,
                            MinimumRevisionsToKeep = 5,
                        },
                        Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                        {
                            ["Users"] = new RevisionsCollectionConfiguration
                            {
                                Active = true
                            },
                            ["Dons"] = new RevisionsCollectionConfiguration
                            {
                                Active = true,
                            }
                        }
                    };

                    AsyncHelpers.RunSync(() => Server.ServerStore.ModifyDatabaseRevisions(context,
                        store.Database,
                        EntityToBlittable.ConvertEntityToBlittable(configuration,
                            new DocumentConventions(),
                            context)));
                }

                var subscriptionId = store.Subscriptions.Create(new SubscriptionCreationOptions<User>()
                {
                    Criteria = new SubscriptionCriteria<User>()
                    {
                        Script = "throw 'nice'",
                        IncludeRevisions = true
                    }
                });

                var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId));

                var exceptions = new List<Exception>();

                var mre = new ManualResetEvent(false);
                var receivedItem = new SubscriptionBatch<User>.Item();
                subscription.Run(x =>
                {
                    foreach (var item in x.Items)
                    {
                        receivedItem = item;
                        try
                        {
                            var res = item;
                        }
                        catch (Exception e)
                        {
                            exceptions.Add(e);
                        }
                    }
                    mre.Set();
                });

                var userId = string.Empty;

                using (var session = store.OpenSession())
                {
                    var newUser = new User();
                    session.Store(newUser);
                    session.SaveChanges();
                    userId = session.Advanced.GetDocumentId(newUser);
                }

                Assert.True(mre.WaitOne(_reasonableWaitTime));
                Assert.NotNull(receivedItem);
                Assert.Throws<InvalidOperationException>(() => receivedItem.Result);
                Assert.NotNull(receivedItem.Metadata);
                Assert.Equal(receivedItem.Id, userId);
            }
        }
    }
}
