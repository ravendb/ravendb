using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Server;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Subscriptions
{
    public class RevisionsSubscriptions:RavenTestBase
    {
        public RevisionsSubscriptions(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(15);

        [Fact]
        public async Task PlainRevisionsSubscriptions()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionId = await store.Subscriptions.CreateAsync<Revision<User>>();

                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var configuration = new RevisionsConfiguration
                    {
                        Default = new RevisionsCollectionConfiguration
                        {
                            Disabled = false,
                            MinimumRevisionsToKeep = 5,
                        },
                        Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                        {
                            ["Users"] = new RevisionsCollectionConfiguration
                            {
                                Disabled = false
                            },
                            ["Dons"] = new RevisionsCollectionConfiguration
                            {
                                Disabled = false
                            }
                        }
                    };

                    await Server.ServerStore.ModifyDatabaseRevisions(context,
                        store.Database,
                        DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(configuration,
                            context), Guid.NewGuid().ToString());
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
                            }, "dons/" + i);

                            session.SaveChanges();
                        }
                    }
                }

                using (var sub = store.Subscriptions.GetSubscriptionWorker<Revision<User>>(new SubscriptionWorkerOptions(subscriptionId) {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                }))
                {
                    var mre = new AsyncManualResetEvent();
                    var names = new HashSet<string>();
                    GC.KeepAlive(sub.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            names.Add(item.Result.Current?.Name + item.Result.Previous?.Name);

                            if (names.Count == 100)
                                mre.Set();
                        }
                    }));

                    Assert.True(await mre.WaitAsync(_reasonableWaitTime));

                }
            }
        }

        [Fact]
        public async Task PlainRevisionsSubscriptionsCompareDocs()
        {
            using (var store = GetDocumentStore())
            {

                var subscriptionId = await store.Subscriptions.CreateAsync<Revision<User>>();

                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var configuration = new RevisionsConfiguration
                    {
                        Default = new RevisionsCollectionConfiguration
                        {
                            Disabled = false,
                            MinimumRevisionsToKeep = 5,
                        },
                        Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                        {
                            ["Users"] = new RevisionsCollectionConfiguration
                            {
                                Disabled = false
                            },
                            ["Dons"] = new RevisionsCollectionConfiguration
                            {
                                Disabled = false
                            }
                        }
                    };

                    await Server.ServerStore.ModifyDatabaseRevisions(context,
                        store.Database,
                        DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(configuration,
                            context), Guid.NewGuid().ToString());
                }

                for (var j = 0; j < 10; j++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User
                        {
                            Name = $"users1 ver {j}",
                            Age = j
                        }, "users/1");

                        session.Store(new Company()
                        {
                            Name = $"dons1 ver {j}"
                        }, "dons/1");

                        session.SaveChanges();
                    }
                }

                using (var sub = store.Subscriptions.GetSubscriptionWorker<Revision<User>>(new SubscriptionWorkerOptions(subscriptionId) {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                }))
                {
                    var mre = new AsyncManualResetEvent();
                    var names = new HashSet<string>();
                    var maxAge = -1;
                    GC.KeepAlive(sub.Run(a =>
                    {
                        foreach (var item in a.Items)
                        {
                            var x = item.Result;
                            if (x.Current.Age > maxAge && x.Current.Age > (x.Previous?.Age ?? -1))
                            {
                                names.Add(x.Current?.Name + x.Previous?.Name);
                                maxAge = x.Current.Age;
                            }
                            names.Add(x.Current?.Name + x.Previous?.Name);
                            if (names.Count == 10)
                                mre.Set();
                        }
                    }));

                    Assert.True(await mre.WaitAsync(_reasonableWaitTime));

                }
            }
        }

        public class Result
        {
            public string Id;
            public int Age;
        }

        [Fact]
        public async Task RevisionsSubscriptionsWithCustomScript()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionId = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                {
                    Query = @"
declare function match(d){
    return d.Current.Age > d.Previous.Age;
}
from Users (Revisions = true) as d
where match(d)
select { Id: id(d.Current), Age: d.Current.Age }
"
                });

                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var configuration = new RevisionsConfiguration
                    {
                        Default = new RevisionsCollectionConfiguration
                        {
                            Disabled = false,
                            MinimumRevisionsToKeep = 5,
                        },
                        Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                        {
                            ["Users"] = new RevisionsCollectionConfiguration
                            {
                                Disabled = false
                            },
                            ["Dons"] = new RevisionsCollectionConfiguration
                            {
                                Disabled = false
                            }
                        }
                    };

                    await Server.ServerStore.ModifyDatabaseRevisions(context,
                        store.Database,
                        DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(configuration,
                            context), Guid.NewGuid().ToString());
                }

                for (int i = 0; i < 10; i++)
                {
                    for (var j = 0; j < 10; j++)
                    {
                        using (var session = store.OpenSession())
                        {
                            session.Store(new User
                            {
                                Name = $"users{i} ver {j}",
                                Age=j
                            }, "users/" + i);

                            session.Store(new Company()
                            {
                                Name = $"dons{i} ver {j}"
                            }, "companies/" + i);

                            session.SaveChanges();
                        }
                    }
                }

                using (var sub = store.Subscriptions.GetSubscriptionWorker<Result>(new SubscriptionWorkerOptions(subscriptionId) {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                }))
                {
                    var mre = new AsyncManualResetEvent();
                    var names = new HashSet<string>();
                    GC.KeepAlive(sub.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            names.Add(item.Result.Id + item.Result.Age);
                            if (names.Count == 90)
                                mre.Set();
                        }
                    }));

                    Assert.True(await mre.WaitAsync(_reasonableWaitTime));

                }
            }
        }

        [Fact]
        public async Task RevisionsSubscriptionsWithCustomScriptCompareDocs()
        {
            using (var store = GetDocumentStore())
            {

                var subscriptionId = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                {
                    Query = @"
declare function match(d){
    return d.Current.Age > d.Previous.Age;
}
from Users (Revisions = true) as d
where match(d)
select { Id: id(d.Current), Age: d.Current.Age }
"
                });

                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var configuration = new RevisionsConfiguration
                    {
                        Default = new RevisionsCollectionConfiguration
                        {
                            Disabled = false,
                            MinimumRevisionsToKeep = 5
                        },
                        Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                        {
                            ["Users"] = new RevisionsCollectionConfiguration
                            {
                                Disabled = false
                            },
                            ["Dons"] = new RevisionsCollectionConfiguration
                            {
                                Disabled = false
                            }
                        }
                    };

                    await Server.ServerStore.ModifyDatabaseRevisions(context,
                        store.Database,
                        DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(configuration,
                            context), Guid.NewGuid().ToString());
                }

                for (int i = 0; i < 10; i++)
                {
                    for (var j = 0; j < 10; j++)
                    {
                        using (var session = store.OpenSession())
                        {
                            session.Store(new User
                            {
                                Name = $"users{i} ver {j}",
                                Age = j
                            }, "users/" + i);

                            session.Store(new Company()
                            {
                                Name = $"dons{i} ver {j}"
                            }, "companies/" + i);

                            session.SaveChanges();
                        }
                    }
                }

                using (var sub = store.Subscriptions.GetSubscriptionWorker<Result>(new SubscriptionWorkerOptions(subscriptionId) {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                }))
                {
                    var mre = new AsyncManualResetEvent();
                    var names = new HashSet<string>();
                    var maxAge = -1;
                    GC.KeepAlive(sub.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            if (item.Result.Age > maxAge)
                            {
                                names.Add(item.Result.Id + item.Result.Age);
                                maxAge = item.Result.Age;
                            }

                            if (names.Count == 9)
                                mre.Set();
                        }
                    }));

                    Assert.True(await mre.WaitAsync(_reasonableWaitTime));

                }
            }
        }

        [Fact]
        public async Task RDBCL_801()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                        {
                            Name = "Karmel"
                        }, "users/1");
                    session.SaveChanges();
                }

                var subscriptionId = await store.Subscriptions.CreateAsync<Revision<User>>();
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var configuration = new RevisionsConfiguration
                    {
                        Default = new RevisionsCollectionConfiguration
                        {
                            Disabled = false,
                            MinimumRevisionsToKeep = 5,
                        },
                        Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                        {
                            ["Users"] = new RevisionsCollectionConfiguration
                            {
                                Disabled = false
                            },
                            ["Dons"] = new RevisionsCollectionConfiguration
                            {
                                Disabled = false
                            }
                        }
                    };

                    await Server.ServerStore.ModifyDatabaseRevisions(context,
                        store.Database,
                        DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(configuration,
                            context), Guid.NewGuid().ToString());
                }

                using (var sub = store.Subscriptions.GetSubscriptionWorker<Revision<User>>(new SubscriptionWorkerOptions(subscriptionId)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                }))
                {
                    GC.KeepAlive(sub.Run(x =>
                    {
                    
                    }));

                    var acks = 0;
                    await Task.Delay(5000);

                    using (Server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        foreach (var entry in Server.ServerStore.Engine.LogHistory.GetHistoryLogs(context))
                        {
                            var type = entry[nameof(RachisLogHistory.LogHistoryColumn.Type)].ToString();
                            if (type == nameof(AcknowledgeSubscriptionBatchCommand))
                            {
                                acks++;
                            }
                        }
                    }
                   
                    Assert.True(acks < 50, $"{acks}");
                }
            }
        }
    }
}
