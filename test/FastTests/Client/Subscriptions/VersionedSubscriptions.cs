using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Server.Versioning;
using Raven.Client.Util;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;
using FastTests.Server.Documents.Notifications;
using Sparrow;

namespace FastTests.Client.Subscriptions
{
    public class VersionedSubscriptions:RavenTestBase
    {
        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromSeconds(60);
        [Fact]
        public async Task PlainVersionedSubscriptions()
        {
            using (var store = GetDocumentStore())
            {

                var subscriptionId = await store.AsyncSubscriptions.CreateAsync(new SubscriptionCreationOptions<Versioned<User>>
                {
                    Criteria = new SubscriptionCriteria<Versioned<User>>
                    {
                        //                            FilterJavaScript = @"
                        //if(this.Current.Status == 'Open' && this.Previous.Status == 'Closed')
                        //{
                        //    return { Id: this.Current.Id, Status: 'Ropened' }
                        //}
                        //else retunr null;
                        //"
                    }
                });

                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var versioningDoc = new VersioningConfiguration
                    {
                        Default = new VersioningConfigurationCollection
                        {
                            Active = true,
                            MaxRevisions = 5,
                        },
                        Collections = new Dictionary<string, VersioningConfigurationCollection>
                        {
                            ["Users"] = new VersioningConfigurationCollection
                            {
                                Active = true
                            },
                            ["Companies"] = new VersioningConfigurationCollection
                            {
                                Active = true,
                            }
                        }
                    };

                    await Server.ServerStore.ModifyDatabaseVersioning(context,
                        store.Database,
                        EntityToBlittable.ConvertEntityToBlittable(versioningDoc,
                            new DocumentConventions(),
                            context));
                }

                for (int i = 0; i < 10; i++)
                {
                    for (var j = 0; j < 10; j++)
                    {
                        using (var session = store.OpenSession())
                        {
                            session.Store(new User
                            {
                                Name = $"users{i} ver {j}"
                            }, "users/" + i);

                            session.Store(new Company()
                            {
                                Name = $"dons{i} ver {j}"
                            }, "companies/" + i);

                            session.SaveChanges();
                        }
                    }
                }

                using (var sub = store.Subscriptions.Open<Versioned<User>>(new SubscriptionConnectionOptions(subscriptionId)))
                {
                    var mre = new AsyncManualResetEvent();
                    var names = new HashSet<string>();
                    sub.Subscribe(x =>
                    {
                        names.Add(x.Current?.Name + x.Previous?.Name);
                        if (names.Count == 100)
                            mre.Set();
                    });
                    sub.Start();

                    Assert.True(await mre.WaitAsync(_reasonableWaitTime));

                }
            }
        }
    }
}
