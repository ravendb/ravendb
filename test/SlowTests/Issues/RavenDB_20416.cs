using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
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
        [InlineData("Users", true)]
        [InlineData("Users", false)]
        [InlineData("@all_docs", true)]
        [InlineData("@all_docs", false)]
        public async Task CanRunSubscriptionWithLastDocumentAfterReplication(string collection, bool afterReplication)
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
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
                    await CheckSubscriptionFetchAfterUpdate(store1, store2, state, count, collection);

                    var items = new List<string>();
                    var _ = subscription.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            items.Add(item.Id);
                        }
                    });

                    await CheckSubscriptionLastProcessedCVsAsync(store1, store2, state, count);
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
        private async Task CheckSubscriptionFetchAfterUpdate(DocumentStore store1, IDocumentStore store2, SubscriptionState state, int count, string collection)
        {
            var db2 = await Databases.GetDocumentDatabaseInstanceFor(store2);

            var connectionState = new SubscriptionConnectionsState(state.SubscriptionId, db2.SubscriptionStorage);
            connectionState.InitializeLastChangeVectorSent(state.ChangeVectorForNextBatchStartingPoint);
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

        private async Task CheckSubscriptionLastProcessedCVsAsync(DocumentStore store1, IDocumentStore store2, SubscriptionState state, int count)
        {
            var db1 = await Databases.GetDocumentDatabaseInstanceFor(store1);
            var dbId1 = db1.DbBase64Id;
            var db2 = await Databases.GetDocumentDatabaseInstanceFor(store2);
            var dbId2 = db2.DbBase64Id;

            List<ChangeVectorEntry> cvList = null;
            var tt = await WaitForValueAsync(() =>
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
                    return Task.FromResult(0);

                cvList = cvs.ToChangeVectorList();
                return Task.FromResult(cvList.Count);
            }, 2, interval: 333);

            Assert.Equal(2, cvList.Count);
            Assert.Equal(count, cvList.FirstOrDefault(x => x.DbId == dbId1).Etag);
            Assert.Equal(count + 1, cvList.FirstOrDefault(x => x.DbId == dbId2).Etag);
        }
    }
}
