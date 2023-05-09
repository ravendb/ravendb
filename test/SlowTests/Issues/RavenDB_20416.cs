using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_20416 : ReplicationTestBase
    {
        public RavenDB_20416(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [RavenData("Users", true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData("Users", false, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData("@all_docs", true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData("@all_docs", false, DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanRunSubscriptionWithLastDocumentAfterReplicationSharded(Options options, string collection, bool afterReplication)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                using (var session = store2.OpenSession())
                {
                    session.Store(new User() { Age = 32, Name = "EGR" }, $"Users/322");
                    session.SaveChanges();
                }

                var count = 10;
                using (var session = store1.OpenSession())
                {
                    for (int i = 0; i < count; i++)
                    {
                        session.Store(new User() { Age = i }, $"Users/{i}");
                    }

                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);

                var res = await WaitForValueAsync(() =>
                {
                    using (var session = store2.OpenSession())
                    {
                        return Task.FromResult(session.Query<User>().Count());
                    }
                }, count + 1, interval: 333);

                Assert.Equal(count + 1, res);

                var id = await store2.Subscriptions.CreateAsync(new SubscriptionUpdateOptions() { Query = $"from '{collection}'" });
                var cv = Constants.Documents.SubscriptionChangeVectorSpecialStates.LastDocument.ToString();
                await store2.Subscriptions.UpdateAsync(new SubscriptionUpdateOptions() { Name = id, ChangeVector = cv });
                var state = await store2.Subscriptions.GetSubscriptionStateAsync(id);

                using (var subscription = store2.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)))
                {
                    await CheckSubscriptionFetchAfterUpdateAsync(options, store1, store2, state, count, collection);
                    Dictionary<string, string> changeVectorsCollection = null;
                    if (options.DatabaseMode == RavenDatabaseMode.Sharded)
                    {
                        DatabasesLandlord.DatabaseSearchResult result = Server.ServerStore.DatabasesLandlord.TryGetOrCreateDatabase(store2.Database);
                        Assert.Equal(DatabasesLandlord.DatabaseSearchResult.Status.Sharded, result.DatabaseStatus);
                        Assert.NotNull(result.DatabaseContext);
                        var shardExecutor = result.DatabaseContext.ShardExecutor;
                        var ctx = new DefaultHttpContext();

                        changeVectorsCollection =
                            (await shardExecutor.ExecuteParallelForAllAsync(
                                new ShardedLastChangeVectorForCollectionOperation(ctx.Request, "Users", result.DatabaseContext.DatabaseName))).LastChangeVectors;
                    }


                    var items = new List<string>();
                    var _ = subscription.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            items.Add(item.Id);
                        }
                    });

                    await CheckSubscriptionLastProcessedCVsAsync(options, store1, store2, state, count, changeVectorsCollection);
                    Assert.Empty(items);
                    if (afterReplication)
                    {
                        using (var session = store1.OpenSession())
                        {
                            if (collection == "Users")
                            {
                                session.Store(new User() { Age = 32, Name = "EGoR" }, $"Users/100");
                            }
                            else
                            {
                                session.Store(new Camera() { Megapixels = 322 }, $"Cameras/100");
                            }

                            session.SaveChanges();
                        }

                        using (var session = store2.OpenSession())
                        {
                            if (collection == "Users")
                            {
                                session.Store(new User() { Age = 32, Name = "EGoR" }, $"Users/200");
                            }
                            else
                            {
                                session.Store(new Camera() { Megapixels = 322 }, $"Cameras/200");
                            }

                            session.SaveChanges();
                        }
                    }
                    else
                    {
                        using (var session = store2.OpenSession())
                        {
                            if (collection == "Users")
                            {
                                session.Store(new User() { Age = 32, Name = "EGoR" }, $"Users/100");
                            }
                            else
                            {
                                session.Store(new Camera() { Megapixels = 322 }, $"Cameras/100");
                            }

                            session.SaveChanges();
                        }

                        using (var session = store1.OpenSession())
                        {
                            if (collection == "Users")
                            {
                                session.Store(new User() { Age = 32, Name = "EGoR" }, $"Users/200");
                            }
                            else
                            {
                                session.Store(new Camera() { Megapixels = 322 }, $"Cameras/200");
                            }

                            session.SaveChanges();
                        }
                    }


                    Assert.Equal(2, await WaitForValueAsync(() => items.Count, 2));
                }
            }
        }

        private async Task CheckSubscriptionFetchAfterUpdateAsync(Options options, DocumentStore store1, IDocumentStore store2, SubscriptionState state, int count,
            string collection)
        {
            if (options.DatabaseMode == RavenDatabaseMode.Sharded)
            {
                var shards = Sharding.GetShardsDocumentDatabaseInstancesFor(store2);
                await foreach (var db2 in shards)
                {
                    var connectionState = new SubscriptionConnectionsState(store2.Database, state.SubscriptionId, db2.SubscriptionStorage);
                    state.ShardingState.ChangeVectorForNextBatchStartingPointPerShard.TryGetValue(db2.Name, out string cv);
                    connectionState.InitializeLastChangeVectorSent(cv);
                    CheckSubscriptionFetchAfterUpdateInternal(collection, db2, connectionState);
                }
            }
            else
            {
                var db2 = await Databases.GetDocumentDatabaseInstanceFor(store2);
                var connectionState = new SubscriptionConnectionsState(store2.Database, state.SubscriptionId, db2.SubscriptionStorage);
                connectionState.InitializeLastChangeVectorSent(state.ChangeVectorForNextBatchStartingPoint);
                CheckSubscriptionFetchAfterUpdateInternal(collection, db2, connectionState);
            }
        }

        private static void CheckSubscriptionFetchAfterUpdateInternal(string collection, DocumentDatabase db2, SubscriptionConnectionsState connectionState)
        {
            var fetcher = new DocumentSubscriptionFetcher(db2, connectionState, collection);
            using (db2.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
            using (db2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx2))
            using (ctx.OpenReadTransaction())
            using (ctx2.OpenReadTransaction())
            {
                fetcher.Initialize(ctx, ctx2, new HashSet<long>());
                foreach (var x in fetcher.GetEnumerator())
                {
                    Assert.False(true, $"got unexpected {x.Id}");
                }
            }
        }

        private async Task CheckSubscriptionLastProcessedCVsAsync(Options options, DocumentStore store1, IDocumentStore store2, SubscriptionState state, int count,
            Dictionary<string, string> cvs)
        {
            if (options.DatabaseMode == RavenDatabaseMode.Sharded)
            {
                var shards1 = await Sharding.GetShardsDocumentDatabaseInstancesFor(store1).ToListAsync();
                Assert.NotNull(shards1);
                var shards2 = await  Sharding.GetShardsDocumentDatabaseInstancesFor(store2).ToListAsync();
                Assert.NotNull(shards2);

                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal,
                    "enable the commented code after RavenDB-20454 is done");
                /* TODO:  RavenDB-20454
                var dbIds1 = shards1.Select(x => x.DbBase64Id).ToList();
                var dbIds2 = shards2.Select(x => x.DbBase64Id).ToList();
                Dictionary<string, string> cvs2 = new Dictionary<string, string>();
                var count2 = 0L;
                var count1 = 0L;*/

                foreach (var db in shards2)
                {
                    var newCv = cvs[db.Name];
                    var cv = WaitForValue(() =>
                    {
                        using (db.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                        using (ctx.OpenReadTransaction())
                        {
                            var subsState = db.SubscriptionStorage.GetSubscriptionConnectionsState(ctx, state.SubscriptionName)?.GetConnections().FirstOrDefault()?.SubscriptionState;
                            if (subsState?.ShardingState == null)
                                return null;

                            return subsState.ShardingState.ChangeVectorForNextBatchStartingPointPerShard.TryGetValue(db.Name, out string cv) ? cv : null;
                        }
                    }, newCv, interval: 333);
                    Assert.Equal(newCv, cv);

                    DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal,
                        "enable the commented code after RavenDB-20454 is done");

                    /* TODO:  RavenDB-20454
                     var cvList = cv.ToChangeVectorList();
                    Assert.Equal(2, cvList.Count);

                    foreach (var id in dbIds1)
                    {
                        if (cvList.Any(x => x.DbId == id))
                        {
                            count1 += cvList.First(x => x.DbId == id).Etag;
                            break;
                        }
                    }

                    foreach (var id in dbIds2)
                    {
                        if (cvList.Any(x => x.DbId == id))
                        {
                            count2 += cvList.First(x => x.DbId == id).Etag;
                            break;
                        }
                    }*/
                }

                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal,
                    "enable the commented code after RavenDB-20454 is done");

                /* TODO:  RavenDB-20454
                Assert.Equal(count, count1);
                Assert.Equal(count + 1, count2);*/
            }
            else
            {
                var db1 = await Databases.GetDocumentDatabaseInstanceFor(store1);
                var dbId1 = db1.DbBase64Id;
                var db2 = await Databases.GetDocumentDatabaseInstanceFor(store2);
                var dbId2 = db2.DbBase64Id;

                List<ChangeVectorEntry> cvList = null;
                var tt = WaitForValue(() =>
                {
                    string cvs = null;
                    using (db2.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var connectionState = db2.SubscriptionStorage.GetSubscriptionConnectionsState(ctx, state.SubscriptionName);
                        if (connectionState != null)
                        {
                            cvs = ChangeVectorUtils.MergeVectors(cvs, connectionState.LastChangeVectorSent);
                        }
                    }

                    if (cvs == null)
                        return 0;

                    cvList = cvs.ToChangeVectorList();
                    return cvList.Count;
                }, 2, interval: 333);

                Assert.Equal(2, cvList.Count);
                Assert.Equal(count, cvList.FirstOrDefault(x => x.DbId == dbId1).Etag);
                Assert.Equal(count + 1, cvList.FirstOrDefault(x => x.DbId == dbId2).Etag);
            }
        }
    }
}
