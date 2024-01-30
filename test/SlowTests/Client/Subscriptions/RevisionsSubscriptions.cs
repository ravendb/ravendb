using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Server;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
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

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [InlineData(@"from Orders (Revisions = true) as docs
select {
    Current: docs.Current,
    Previous: docs.Previous,
    CurrentMetadata: docs.Current[""@metadata""],
    PreviousMetadata: docs.Previous[""@metadata""],
    CurrentId: id(docs.Current),
    PreviousId: id(docs.Previous),
    CurrentChangeVector: docs.Current[""@metadata""][""@change-vector""],
    PreviousChangeVector: docs.Previous[""@metadata""][""@change-vector""]
}")]
        [InlineData(@"from Orders (Revisions = true) as docs
select {
    Current: docs.Current,
    Previous: docs.Previous,
    CurrentMetadata: docs.Current[""@metadata""],
    PreviousMetadata: docs.Previous[""@metadata""],
    CurrentId: id(docs.Current),
    PreviousId: id(docs.Previous),
    CurrentChangeVector: docs.Current == null ? null : metadataFor(docs.Current)[""@change-vector""],
    PreviousChangeVector: docs.Previous == null ? null : metadataFor(docs.Previous)[""@change-vector""]
}")]
        public async Task CanReturnMetadataInRevisionsSubscription(string query)
        {
            var jsonString =
                "{\r\n    \"BuildVersion\": 54,\r\n    \"DatabaseRecord\": {\r\n        \"DatabaseName\": \"test111\",\r\n        \"Encrypted\": false,\r\n        \"UnusedDatabaseIds\": [],\r\n        \"LockMode\": \"Unlock\",\r\n        \"ConflictSolverConfig\": null,\r\n        \"Settings\": [],\r\n        \"Revisions\": {\r\n            \"Default\": null,\r\n            \"Collections\": {\r\n                \"Orders\": {\r\n                    \"Disabled\": false,\r\n                    \"MinimumRevisionsToKeep\": null,\r\n                    \"MinimumRevisionAgeToKeep\": null,\r\n                    \"PurgeOnDelete\": false,\r\n                    \"MaximumRevisionsToDeleteUponDocumentUpdate\": null\r\n                }\r\n            }\r\n        },\r\n        \"TimeSeries\": {},\r\n        \"DocumentsCompression\": {\r\n            \"Collections\": [],\r\n            \"CompressAllCollections\": false,\r\n            \"CompressRevisions\": true\r\n        },\r\n        \"Expiration\": null,\r\n        \"Refresh\": null,\r\n        \"Client\": null,\r\n        \"Sorters\": {},\r\n        \"Analyzers\": {},\r\n        \"IndexesHistory\": {\r\n            \"Product/Search\": [{\r\n                    \"Source\": \"Smuggler\",\r\n                    \"CreatedAt\": \"2024-01-14T16:23:10.7901207Z\",\r\n                    \"RollingDeployment\": {},\r\n                    \"Definition\": {\r\n                        \"SourceType\": \"Documents\",\r\n                        \"LockMode\": \"Unlock\",\r\n                        \"Priority\": \"Normal\",\r\n                        \"State\": null,\r\n                        \"OutputReduceToCollection\": null,\r\n                        \"PatternForOutputReduceToCollectionReferences\": null,\r\n                        \"PatternReferencesCollectionName\": null,\r\n                        \"ReduceOutputIndex\": null,\r\n                        \"DeploymentMode\": null,\r\n                        \"Name\": \"Product/Search\",\r\n                        \"Reduce\": null,\r\n                        \"Type\": \"Map\",\r\n                        \"Maps\": [\"from p in docs.Products\\r\\nselect new\\r\\n{\\r\\n    p.Name,\\r\\n    p.Category,\\r\\n    p.Supplier,\\r\\n    p.PricePerUnit\\r\\n}\"],\r\n                        \"ClusterState\": {\r\n                            \"LastStateIndex\": 0\r\n                        },\r\n                        \"Fields\": {\r\n                            \"Name\": {\r\n                                \"Analyzer\": null,\r\n                                \"Indexing\": \"Search\",\r\n                                \"Spatial\": null,\r\n                                \"Storage\": null,\r\n                                \"Suggestions\": true,\r\n                                \"TermVector\": \"Yes\"\r\n                            }\r\n                        },\r\n                        \"Configuration\": {},\r\n                        \"AdditionalSources\": {},\r\n                        \"AdditionalAssemblies\": []\r\n                    }\r\n                }\r\n            ],\r\n            \"Orders/ByShipment/Location\": [{\r\n                    \"Source\": \"Smuggler\",\r\n                    \"CreatedAt\": \"2024-01-14T16:23:10.7901207Z\",\r\n                    \"RollingDeployment\": {},\r\n                    \"Definition\": {\r\n                        \"SourceType\": \"Documents\",\r\n                        \"LockMode\": \"Unlock\",\r\n                        \"Priority\": \"Normal\",\r\n                        \"State\": null,\r\n                        \"OutputReduceToCollection\": null,\r\n                        \"PatternForOutputReduceToCollectionReferences\": null,\r\n                        \"PatternReferencesCollectionName\": null,\r\n                        \"ReduceOutputIndex\": null,\r\n                        \"DeploymentMode\": null,\r\n                        \"Name\": \"Orders/ByShipment/Location\",\r\n                        \"Reduce\": null,\r\n                        \"Type\": \"Map\",\r\n                        \"Maps\": [\"from order in docs.Orders\\r\\nwhere order.ShipTo.Location != null\\r\\nselect new\\r\\n{\\r\\n    order.Employee,\\r\\n    order.Company,\\r\\n    ShipmentLocation = CreateSpatialField(order.ShipTo.Location.Latitude, order.ShipTo.Location.Longitude)\\r\\n}\"],\r\n                        \"ClusterState\": {\r\n                            \"LastStateIndex\": 0\r\n                        },\r\n                        \"Fields\": {\r\n                            \"ShipmentLocation\": {\r\n                                \"Analyzer\": null,\r\n                                \"Indexing\": null,\r\n                                \"Spatial\": {\r\n                                    \"MaxTreeLevel\": 9,\r\n                                    \"MaxX\": 180.0,\r\n                                    \"MaxY\": 90.0,\r\n                                    \"MinX\": -180.0,\r\n                                    \"MinY\": -90.0,\r\n                                    \"Strategy\": \"GeohashPrefixTree\",\r\n                                    \"Type\": \"Geography\",\r\n                                    \"Units\": \"Kilometers\"\r\n                                },\r\n                                \"Storage\": null,\r\n                                \"Suggestions\": null,\r\n                                \"TermVector\": null\r\n                            }\r\n                        },\r\n                        \"Configuration\": {},\r\n                        \"AdditionalSources\": {},\r\n                        \"AdditionalAssemblies\": []\r\n                    }\r\n                }\r\n            ],\r\n            \"Orders/ByCompany\": [{\r\n                    \"Source\": \"Smuggler\",\r\n                    \"CreatedAt\": \"2024-01-14T16:23:10.7901207Z\",\r\n                    \"RollingDeployment\": {},\r\n                    \"Definition\": {\r\n                        \"SourceType\": \"Documents\",\r\n                        \"LockMode\": \"Unlock\",\r\n                        \"Priority\": \"Normal\",\r\n                        \"State\": null,\r\n                        \"OutputReduceToCollection\": null,\r\n                        \"PatternForOutputReduceToCollectionReferences\": null,\r\n                        \"PatternReferencesCollectionName\": null,\r\n                        \"ReduceOutputIndex\": null,\r\n                        \"DeploymentMode\": null,\r\n                        \"Name\": \"Orders/ByCompany\",\r\n                        \"Reduce\": \"from result in results\\r\\ngroup result by result.Company \\r\\ninto g\\r\\nselect new\\r\\n{\\r\\n\\tCompany = g.Key,\\r\\n\\tCount = g.Sum(x => x.Count),\\r\\n\\tTotal = g.Sum(x => x.Total)\\r\\n}\",\r\n                        \"Type\": \"MapReduce\",\r\n                        \"Maps\": [\"from order in docs.Orders\\r\\nselect new\\r\\n{\\r\\n    order.Company,\\r\\n    Count = 1,\\r\\n    Total = order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))\\r\\n}\"],\r\n                        \"ClusterState\": {\r\n                            \"LastStateIndex\": 0\r\n                        },\r\n                        \"Fields\": {},\r\n                        \"Configuration\": {},\r\n                        \"AdditionalSources\": {},\r\n                        \"AdditionalAssemblies\": []\r\n                    }\r\n                }\r\n            ],\r\n            \"Products/ByUnitOnStock\": [{\r\n                    \"Source\": \"Smuggler\",\r\n                    \"CreatedAt\": \"2024-01-14T16:23:10.7901207Z\",\r\n                    \"RollingDeployment\": {},\r\n                    \"Definition\": {\r\n                        \"SourceType\": \"Documents\",\r\n                        \"LockMode\": \"Unlock\",\r\n                        \"Priority\": \"Normal\",\r\n                        \"State\": null,\r\n                        \"OutputReduceToCollection\": null,\r\n                        \"PatternForOutputReduceToCollectionReferences\": null,\r\n                        \"PatternReferencesCollectionName\": null,\r\n                        \"ReduceOutputIndex\": null,\r\n                        \"DeploymentMode\": null,\r\n                        \"Name\": \"Products/ByUnitOnStock\",\r\n                        \"Reduce\": null,\r\n                        \"Type\": \"Map\",\r\n                        \"Maps\": [\"from product in docs.Products\\r\\nselect new {\\r\\n    UnitOnStock = LoadCompareExchangeValue(Id(product))\\r\\n}\"],\r\n                        \"ClusterState\": {\r\n                            \"LastStateIndex\": 0\r\n                        },\r\n                        \"Fields\": {},\r\n                        \"Configuration\": {},\r\n                        \"AdditionalSources\": {},\r\n                        \"AdditionalAssemblies\": []\r\n                    }\r\n                }\r\n            ],\r\n            \"Product/Rating\": [{\r\n                    \"Source\": \"Smuggler\",\r\n                    \"CreatedAt\": \"2024-01-14T16:23:10.7901207Z\",\r\n                    \"RollingDeployment\": {},\r\n                    \"Definition\": {\r\n                        \"SourceType\": \"Counters\",\r\n                        \"LockMode\": \"Unlock\",\r\n                        \"Priority\": \"Normal\",\r\n                        \"State\": null,\r\n                        \"OutputReduceToCollection\": null,\r\n                        \"PatternForOutputReduceToCollectionReferences\": null,\r\n                        \"PatternReferencesCollectionName\": null,\r\n                        \"ReduceOutputIndex\": null,\r\n                        \"DeploymentMode\": null,\r\n                        \"Name\": \"Product/Rating\",\r\n                        \"Reduce\": \"from result in results\\r\\ngroup result by result.Name into g\\r\\nlet totalVotes = g.Sum(x => x.TotalVotes)\\r\\nlet rating = g.Sum(x => x.TotalVotes / (double)totalVotes * x.Rating)\\r\\nselect new {\\r\\n   Name = g.Key,\\r\\n   Rating = rating,\\r\\n   TotalVotes = totalVotes,\\r\\n   AllRatings = g.SelectMany(x => x.AllRatings).ToArray()\\r\\n}\",\r\n                        \"Type\": \"MapReduce\",\r\n                        \"Maps\": [\"from counter in counters.Products\\r\\nlet product = LoadDocument(counter.DocumentId, \\\"Products\\\")\\r\\nwhere counter.Name.Contains(\\\"\u2b50\\\")\\r\\nselect new {\\r\\n    Name = product.Name,\\r\\n    Rating = counter.Name.Length,\\r\\n    TotalVotes = counter.Value,\\r\\n    AllRatings = new []\\r\\n    {\\r\\n        new\\r\\n        {\\r\\n            Rating = counter.Name,\\r\\n            Votes = counter.Value\\r\\n        }\\r\\n    }\\r\\n}\"],\r\n                        \"ClusterState\": {\r\n                            \"LastStateIndex\": 0\r\n                        },\r\n                        \"Fields\": {},\r\n                        \"Configuration\": {},\r\n                        \"AdditionalSources\": {},\r\n                        \"AdditionalAssemblies\": []\r\n                    }\r\n                }\r\n            ],\r\n            \"Orders/Totals\": [{\r\n                    \"Source\": \"Smuggler\",\r\n                    \"CreatedAt\": \"2024-01-14T16:23:10.7901207Z\",\r\n                    \"RollingDeployment\": {},\r\n                    \"Definition\": {\r\n                        \"SourceType\": \"Documents\",\r\n                        \"LockMode\": \"Unlock\",\r\n                        \"Priority\": \"Normal\",\r\n                        \"State\": null,\r\n                        \"OutputReduceToCollection\": null,\r\n                        \"PatternForOutputReduceToCollectionReferences\": null,\r\n                        \"PatternReferencesCollectionName\": null,\r\n                        \"ReduceOutputIndex\": null,\r\n                        \"DeploymentMode\": null,\r\n                        \"Name\": \"Orders/Totals\",\r\n                        \"Reduce\": null,\r\n                        \"Type\": \"Map\",\r\n                        \"Maps\": [\"from order in docs.Orders\\r\\nselect new\\r\\n{\\r\\n    order.Employee,\\r\\n    order.Company,\\r\\n    Total = order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))\\r\\n}\"],\r\n                        \"ClusterState\": {\r\n                            \"LastStateIndex\": 0\r\n                        },\r\n                        \"Fields\": {},\r\n                        \"Configuration\": {},\r\n                        \"AdditionalSources\": {},\r\n                        \"AdditionalAssemblies\": []\r\n                    }\r\n                }\r\n            ],\r\n            \"Companies/StockPrices/TradeVolumeByMonth\": [{\r\n                    \"Source\": \"Smuggler\",\r\n                    \"CreatedAt\": \"2024-01-14T16:23:10.7901207Z\",\r\n                    \"RollingDeployment\": {},\r\n                    \"Definition\": {\r\n                        \"SourceType\": \"TimeSeries\",\r\n                        \"LockMode\": \"Unlock\",\r\n                        \"Priority\": \"Normal\",\r\n                        \"State\": null,\r\n                        \"OutputReduceToCollection\": null,\r\n                        \"PatternForOutputReduceToCollectionReferences\": null,\r\n                        \"PatternReferencesCollectionName\": null,\r\n                        \"ReduceOutputIndex\": null,\r\n                        \"DeploymentMode\": null,\r\n                        \"Name\": \"Companies/StockPrices/TradeVolumeByMonth\",\r\n                        \"Reduce\": \"from result in results\\r\\ngroup result by new { result.Date, result.Country } into g\\r\\nselect new {\\r\\n    Date = g.Key.Date,\\r\\n    Country = g.Key.Country,\\r\\n    Volume = g.Sum(x => x.Volume)\\r\\n}\",\r\n                        \"Type\": \"MapReduce\",\r\n                        \"Maps\": [\"from segment in timeseries.Companies.StockPrices\\r\\nlet company = LoadDocument(segment.DocumentId, \\\"Companies\\\")\\r\\nfrom entry in segment.Entries\\r\\nselect new \\r\\n{\\r\\n    Date = new DateTime(entry.Timestamp.Year, entry.Timestamp.Month, 1),\\r\\n    Country = company.Address.Country,\\r\\n    Volume = entry.Values[4]\\r\\n}\"],\r\n                        \"ClusterState\": {\r\n                            \"LastStateIndex\": 0\r\n                        },\r\n                        \"Fields\": {},\r\n                        \"Configuration\": {},\r\n                        \"AdditionalSources\": {},\r\n                        \"AdditionalAssemblies\": []\r\n                    }\r\n                }\r\n            ]\r\n        },\r\n        \"RavenConnectionStrings\": {},\r\n        \"SqlConnectionStrings\": {},\r\n        \"PeriodicBackups\": [],\r\n        \"ExternalReplications\": [],\r\n        \"RavenEtls\": [],\r\n        \"SqlEtls\": [],\r\n        \"HubPullReplications\": [],\r\n        \"SinkPullReplications\": [],\r\n        \"OlapConnectionStrings\": {},\r\n        \"OlapEtls\": [],\r\n        \"ElasticSearchConnectionStrings\": {},\r\n        \"ElasticSearchEtls\": [],\r\n        \"QueueConnectionStrings\": {},\r\n        \"QueueEtls\": []\r\n    },\r\n    \"Docs\": [],\r\n    \"RevisionDocuments\": [{\r\n            \"Company\": \"companies/76-A\",\r\n            \"Employee\": \"employees/4-A\",\r\n            \"Freight\": 51.3,\r\n            \"Lines\": [{\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 64.8,\r\n                    \"Product\": \"products/20-A\",\r\n                    \"ProductName\": \"Sir Rodney's Marmalade\",\r\n                    \"Quantity\": 40\r\n                }, {\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 2.0,\r\n                    \"Product\": \"products/33-A\",\r\n                    \"ProductName\": \"Geitost\",\r\n                    \"Quantity\": 25\r\n                }, {\r\n                    \"Discount\": 0.0,\r\n                    \"PricePerUnit\": 27.2000,\r\n                    \"Product\": \"products/60-A\",\r\n                    \"ProductName\": \"Camembert Pierrot\",\r\n                    \"Quantity\": 40\r\n                }\r\n            ],\r\n            \"OrderedAt\": \"1996-07-09T00:00:00.0000000\",\r\n            \"RequireAt\": \"1996-08-06T00:00:00.0000000\",\r\n            \"ShipTo\": {\r\n                \"City\": \"Charleroi\",\r\n                \"Country\": \"Belgium\",\r\n                \"Line1\": \"Boulevard Tirou, 255\",\r\n                \"Line2\": null,\r\n                \"Location\": {\r\n                    \"Latitude\": 50.4062634,\r\n                    \"Longitude\": 4.4470125\r\n                },\r\n                \"PostalCode\": \"B-6000\",\r\n                \"Region\": null\r\n            },\r\n            \"ShipVia\": \"shippers/2-A\",\r\n            \"ShippedAt\": \"1996-07-11T00:00:00.0000000\",\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"A:93-OSKWIRBEDEGoAxbEIiFJeQ\",\r\n                \"@flags\": \"HasRevisions, Revision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2018-07-27T12:11:53.0456146Z\"\r\n            }\r\n        }, {\r\n            \"Company\": \"companies/76-A\",\r\n            \"Employee\": \"employees/4-A\",\r\n            \"Freight\": 51.3,\r\n            \"Lines\": [{\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 64.8,\r\n                    \"Product\": \"products/20-A\",\r\n                    \"ProductName\": \"Sir Rodney's Marmalade\",\r\n                    \"Quantity\": 40\r\n                }, {\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 2.0,\r\n                    \"Product\": \"products/33-A\",\r\n                    \"ProductName\": \"Geitost\",\r\n                    \"Quantity\": 25\r\n                }, {\r\n                    \"Discount\": 0.0,\r\n                    \"PricePerUnit\": 27.2000,\r\n                    \"Product\": \"products/60-A\",\r\n                    \"ProductName\": \"Camembert Pierrot\",\r\n                    \"Quantity\": 40\r\n                }\r\n            ],\r\n            \"OrderedAt\": \"1996-07-09T00:00:00.0000000\",\r\n            \"RequireAt\": \"1996-08-06T00:00:00.0000000\",\r\n            \"ShipTo\": {\r\n                \"City\": \"Charleroi\",\r\n                \"Country\": \"Belgium\",\r\n                \"Line1\": \"Boulevard Tirou, 255\",\r\n                \"Line2\": null,\r\n                \"Location\": {\r\n                    \"Latitude\": 50.4062634,\r\n                    \"Longitude\": 4.4470125\r\n                },\r\n                \"PostalCode\": \"B-6000\",\r\n                \"Region\": null\r\n            },\r\n            \"ShipVia\": \"shippers/2-A\",\r\n            \"ShippedAt\": \"1996-07-11T00:00:00.0000000\",\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"A:93-F9I6Egqwm0Kz+K0oFVIR9Q\",\r\n                \"@flags\": \"HasRevisions, Revision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2018-07-27T12:11:53.0456146Z\"\r\n            }\r\n        }, {\r\n            \"Company\": \"companies/76-A\",\r\n            \"Employee\": \"employees/4-A\",\r\n            \"Freight\": 51.3,\r\n            \"Lines\": [{\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 64.8,\r\n                    \"Product\": \"products/20-A\",\r\n                    \"ProductName\": \"Sir Rodney's Marmalade\",\r\n                    \"Quantity\": 40\r\n                }, {\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 2.0,\r\n                    \"Product\": \"products/33-A\",\r\n                    \"ProductName\": \"Geitost\",\r\n                    \"Quantity\": 25\r\n                }, {\r\n                    \"Discount\": 0.0,\r\n                    \"PricePerUnit\": 27.2000,\r\n                    \"Product\": \"products/60-A\",\r\n                    \"ProductName\": \"Camembert Pierrot\",\r\n                    \"Quantity\": 40\r\n                }\r\n            ],\r\n            \"OrderedAt\": \"1996-07-09T00:00:00.0000000\",\r\n            \"RequireAt\": \"1996-08-06T00:00:00.0000000\",\r\n            \"ShipTo\": {\r\n                \"City\": \"Charleroi\",\r\n                \"Country\": \"Belgium\",\r\n                \"Line1\": \"Boulevard Tirou, 255\",\r\n                \"Line2\": null,\r\n                \"Location\": {\r\n                    \"Latitude\": 50.4062634,\r\n                    \"Longitude\": 4.4470125\r\n                },\r\n                \"PostalCode\": \"B-6000\",\r\n                \"Region\": null\r\n            },\r\n            \"ShipVia\": \"shippers/2-A\",\r\n            \"ShippedAt\": \"1996-07-11T00:00:00.0000000\",\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"A:2144-IG4VwBTOnkqoT/uwgm2OQg\",\r\n                \"@flags\": \"HasRevisions, Revision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2018-07-27T12:11:53.8295488Z\"\r\n            }\r\n        }, {\r\n            \"Company\": \"companies/76-A\",\r\n            \"Employee\": \"employees/4-A\",\r\n            \"OrderedAt\": \"1996-07-09T00:00:00.0000000\",\r\n            \"RequireAt\": \"1996-08-06T00:00:00.0000000\",\r\n            \"ShippedAt\": \"1996-07-11T00:00:00.0000000\",\r\n            \"ShipTo\": {\r\n                \"Line1\": \"Boulevard Tirou, 255\",\r\n                \"Line2\": null,\r\n                \"City\": \"Charleroi\",\r\n                \"Region\": null,\r\n                \"PostalCode\": \"B-6000\",\r\n                \"Country\": \"Belgium\",\r\n                \"Location\": {\r\n                    \"Latitude\": 50.4062634,\r\n                    \"Longitude\": 4.4470125\r\n                }\r\n            },\r\n            \"ShipVia\": \"shippers/2-A\",\r\n            \"Freight\": 51.3000,\r\n            \"Lines\": [],\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"A:3804-IG4VwBTOnkqoT/uwgm2OQg\",\r\n                \"@flags\": \"HasRevisions, Revision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2018-07-27T12:11:53.9801503Z\"\r\n            }\r\n        }, {\r\n            \"Company\": \"companies/76-A\",\r\n            \"Employee\": \"employees/4-A\",\r\n            \"Freight\": 51.3,\r\n            \"Lines\": [{\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 64.8000,\r\n                    \"Product\": \"products/20-A\",\r\n                    \"ProductName\": \"Sir Rodney's Marmalade\",\r\n                    \"Quantity\": 40\r\n                }\r\n            ],\r\n            \"OrderedAt\": \"1996-07-09T00:00:00.0000000\",\r\n            \"RequireAt\": \"1996-08-06T00:00:00.0000000\",\r\n            \"ShipTo\": {\r\n                \"City\": \"Charleroi\",\r\n                \"Country\": \"Belgium\",\r\n                \"Line1\": \"Boulevard Tirou, 255\",\r\n                \"Line2\": null,\r\n                \"Location\": {\r\n                    \"Latitude\": 50.4062634,\r\n                    \"Longitude\": 4.4470125\r\n                },\r\n                \"PostalCode\": \"B-6000\",\r\n                \"Region\": null\r\n            },\r\n            \"ShipVia\": \"shippers/2-A\",\r\n            \"ShippedAt\": \"1996-07-11T00:00:00.0000000\",\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"A:5478-IG4VwBTOnkqoT/uwgm2OQg\",\r\n                \"@flags\": \"HasRevisions, Revision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2018-07-27T12:11:54.1021446Z\"\r\n            }\r\n        }, {\r\n            \"Company\": \"companies/76-A\",\r\n            \"Employee\": \"employees/4-A\",\r\n            \"Freight\": 51.3,\r\n            \"Lines\": [{\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 64.8,\r\n                    \"Product\": \"products/20-A\",\r\n                    \"ProductName\": \"Sir Rodney's Marmalade\",\r\n                    \"Quantity\": 40\r\n                }, {\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 2.0000,\r\n                    \"Product\": \"products/33-A\",\r\n                    \"ProductName\": \"Geitost\",\r\n                    \"Quantity\": 25\r\n                }\r\n            ],\r\n            \"OrderedAt\": \"1996-07-09T00:00:00.0000000\",\r\n            \"RequireAt\": \"1996-08-06T00:00:00.0000000\",\r\n            \"ShipTo\": {\r\n                \"City\": \"Charleroi\",\r\n                \"Country\": \"Belgium\",\r\n                \"Line1\": \"Boulevard Tirou, 255\",\r\n                \"Line2\": null,\r\n                \"Location\": {\r\n                    \"Latitude\": 50.4062634,\r\n                    \"Longitude\": 4.4470125\r\n                },\r\n                \"PostalCode\": \"B-6000\",\r\n                \"Region\": null\r\n            },\r\n            \"ShipVia\": \"shippers/2-A\",\r\n            \"ShippedAt\": \"1996-07-11T00:00:00.0000000\",\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"A:5480-IG4VwBTOnkqoT/uwgm2OQg\",\r\n                \"@flags\": \"HasRevisions, Revision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2018-07-27T12:11:54.1022519Z\"\r\n            }\r\n        }, {\r\n            \"Company\": \"companies/76-A\",\r\n            \"Employee\": \"employees/4-A\",\r\n            \"Freight\": 51.3,\r\n            \"Lines\": [{\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 64.8,\r\n                    \"Product\": \"products/20-A\",\r\n                    \"ProductName\": \"Sir Rodney's Marmalade\",\r\n                    \"Quantity\": 40\r\n                }, {\r\n                    \"Discount\": 0.05,\r\n                    \"PricePerUnit\": 2.0,\r\n                    \"Product\": \"products/33-A\",\r\n                    \"ProductName\": \"Geitost\",\r\n                    \"Quantity\": 25\r\n                }, {\r\n                    \"Discount\": 0.0,\r\n                    \"PricePerUnit\": 27.2000,\r\n                    \"Product\": \"products/60-A\",\r\n                    \"ProductName\": \"Camembert Pierrot\",\r\n                    \"Quantity\": 40\r\n                }\r\n            ],\r\n            \"OrderedAt\": \"1996-07-09T00:00:00.0000000\",\r\n            \"RequireAt\": \"1996-08-06T00:00:00.0000000\",\r\n            \"ShipTo\": {\r\n                \"City\": \"Charleroi\",\r\n                \"Country\": \"Belgium\",\r\n                \"Line1\": \"Boulevard Tirou, 255\",\r\n                \"Line2\": null,\r\n                \"Location\": {\r\n                    \"Latitude\": 50.4062634,\r\n                    \"Longitude\": 4.4470125\r\n                },\r\n                \"PostalCode\": \"B-6000\",\r\n                \"Region\": null\r\n            },\r\n            \"ShipVia\": \"shippers/2-A\",\r\n            \"ShippedAt\": \"1996-07-11T00:00:00.0000000\",\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"A:5482-IG4VwBTOnkqoT/uwgm2OQg\",\r\n                \"@flags\": \"HasRevisions, Revision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2018-07-27T12:11:54.1024494Z\"\r\n            }\r\n        }, {\r\n            \"@metadata\": {\r\n                \"@collection\": \"Orders\",\r\n                \"@change-vector\": \"A:2568-F9I6Egqwm0Kz+K0oFVIR9Q, A:13366-IG4VwBTOnkqoT/uwgm2OQg, A:2568-OSKWIRBEDEGoAxbEIiFJeQ, A:17614-jxcHZAmE70Kb2y3I+eaWdw\",\r\n                \"@flags\": \"HasRevisions, DeleteRevision\",\r\n                \"@id\": \"orders/5-A\",\r\n                \"@last-modified\": \"2024-01-18T12:12:36.5474797Z\"\r\n            }\r\n        }\r\n    ]\r\n}\r\n";

            using (var store = GetDocumentStore())
            {
                using (var ms = new MemoryStream(Encoding.UTF8.GetBytes(jsonString)))
                using (var zipStream = new GZipStream(ms, CompressionMode.Compress))
                {
                    zipStream.Flush();
                    ms.Position = 0;
                    using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1)))
                    {
                        var operation = await store.Smuggler.ForDatabase(store.Database)
                            .ImportAsync(
                                new DatabaseSmugglerImportOptions
                                {
                                    OperateOnTypes = DatabaseItemType.RevisionDocuments
                                }, ms, cts.Token);


                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                    }
                }

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
                            ["Orders"] = new RevisionsCollectionConfiguration
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

                var subscriptionId = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                {
                    Query = query
                });
                var revisions = new HashSet<MyRevision>();
                using (var sub = store.Subscriptions.GetSubscriptionWorker<MyRevision>(new SubscriptionWorkerOptions(subscriptionId)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                }))
                {
                    _ = sub.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            revisions.Add(item.Result);
                        }
                    });

                    dynamic resultsObj1 = JsonConvert.DeserializeObject<ExpandoObject>(jsonString);

                    List<dynamic> results = resultsObj1.RevisionDocuments;
                    Assert.Equal(8, await WaitForValueAsync(() => revisions.Count, 8));

                    var f = revisions.First();
                    IDictionary<string, object> metadata = (IDictionary<string, object>)((IDictionary<string, object>)results[0])["@metadata"];

                    Assert.Equal((string)metadata["@change-vector"], f.CurrentChangeVector);
                    Assert.Equal((string)metadata["@change-vector"], f.CurrentMetadata["@change-vector"]);

                    Assert.Equal((string)metadata["@id"], f.CurrentId);
                    Assert.Equal((string)metadata["@id"], f.CurrentMetadata["@id"]);

                    Assert.Null(f.Previous);
                    Assert.Null(f.PreviousMetadata);
                    Assert.Null(f.PreviousId);
                    Assert.Null(f.PreviousChangeVector);

                    for (int i = 1; i < revisions.Count - 1; i++)
                    {
                        var r = revisions.ElementAt(i);

                        metadata = (IDictionary<string, object>)((IDictionary<string, object>)results[i])["@metadata"];
                        Assert.Equal((string)metadata["@change-vector"], r.CurrentChangeVector);
                        Assert.Equal((string)metadata["@change-vector"], r.CurrentMetadata["@change-vector"]);

                        Assert.Equal((string)metadata["@id"], r.CurrentId);
                        Assert.Equal((string)metadata["@id"], r.CurrentMetadata["@id"]);
                        IDictionary<string, object> prevMetadata = (IDictionary<string, object>)((IDictionary<string, object>)results[i - 1])["@metadata"];
                        Assert.Equal((string)prevMetadata["@change-vector"], r.PreviousChangeVector);
                        Assert.Equal((string)prevMetadata["@change-vector"], r.PreviousMetadata["@change-vector"]);

                        Assert.Equal((string)prevMetadata["@id"], r.PreviousId);
                        Assert.Equal((string)prevMetadata["@id"], r.PreviousMetadata["@id"]);
                        Assert.NotNull(r.Previous);
                        Assert.NotNull(r.PreviousMetadata);
                        Assert.NotNull(r.PreviousId);
                        Assert.NotNull(r.PreviousChangeVector);

                    }

                    metadata = (IDictionary<string, object>)((IDictionary<string, object>)results[revisions.Count - 1])["@metadata"];
                    var l = revisions.Last();
                    Assert.Equal((string)metadata["@change-vector"], l.CurrentChangeVector);
                    Assert.Equal((string)metadata["@change-vector"], l.CurrentMetadata["@change-vector"]);

                    Assert.Equal((string)metadata["@id"], l.CurrentId);
                    Assert.Equal((string)metadata["@id"], l.CurrentMetadata["@id"]);

                    Assert.NotNull(l.Previous);
                    Assert.NotNull(l.PreviousMetadata);
                    Assert.NotNull(l.PreviousId);
                    Assert.NotNull(l.PreviousChangeVector);
                }
            }
        }

        private class MyRevision
        {
            public Order Previous;
            public Order Current;
            public IDictionary<string, string> CurrentMetadata;
            public IDictionary<string, string> PreviousMetadata;
            public string CurrentId;
            public string PreviousId;
            public string CurrentChangeVector;
            public string PreviousChangeVector;
        }
    }
}
