using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;

namespace InterversionTests
{
    public class MixedClusterTests : MixedClusterTestBase
    {
        [Fact]
        public async Task ReplicationInMixedCluster_40Leader_with_two_41_nodes()
        {
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.0.6"
            }, 1);

            var peer = local[0];
            while (true)
            {
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
                {
                    try
                    {
                        if (leader.ServerStore.Engine.CurrentLeader != null)
                        {
                            leader.ServerStore.Engine.CurrentLeader.StepDown();
                        }
                        else
                        {
                            peer.ServerStore.Engine.CurrentLeader?.StepDown();
                        }

                        await leader.ServerStore.Engine.WaitForState(RachisState.Follower, cts.Token);
                        await peer.ServerStore.Engine.WaitForState(RachisState.Follower, cts.Token);
                        break;
                    }
                    catch
                    {
                        //
                    }
                }
            }

            var stores = await GetStores(leader, peers, local);
            using (stores.Disposable)
            {
                var storeA = stores.Stores[0];

                var dbName = await CreateDatabase(storeA, 3);

                using (var session = storeA.OpenSession(dbName))
                {
                    session.Store(new User
                    {
                        Name = "aviv"
                    }, "users/1");
                    session.SaveChanges();
                }

                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    "users/1",
                    u => u.Name.Equals("aviv"),
                    TimeSpan.FromSeconds(10),
                    stores.Stores,
                    dbName));

                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(dbName, true));
            }
        }

        [Fact]
        public async Task ReplicationInMixedCluster_40Leader_with_one_41_node_and_two_40_nodes()
        {
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.0.6",
                "4.0.6"
            });
            leader.ServerStore.Engine.CurrentLeader.StepDown();
            await leader.ServerStore.Engine.WaitForState(RachisState.Follower, CancellationToken.None);

            var stores = await GetStores(leader, peers);
            using (stores.Disposable)
            {
                var storeA = stores.Stores[0];

                var dbName = await CreateDatabase(storeA, 3);

                using (var session = storeA.OpenSession(dbName))
                {
                    session.Store(new User
                    {
                        Name = "aviv"
                    }, "users/1");
                    session.SaveChanges();
                }

                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    "users/1",
                    u => u.Name.Equals("aviv"),
                    TimeSpan.FromSeconds(10),
                    stores.Stores,
                    dbName));

                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(dbName, true));
            }
        }

        [Fact]
        public async Task ReplicationInMixedCluster_41Leader_with_two_406()
        {
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.0.6",
                "4.0.6"
            });

            var stores = await GetStores(leader, peers);
            using (stores.Disposable)
            {
                var storeA = stores.Stores[0];

                var dbName = await CreateDatabase(storeA, 3);

                using (var session = storeA.OpenSession(dbName))
                {
                    session.Store(new User
                    {
                        Name = "aviv"
                    }, "users/1");
                    session.SaveChanges();
                }

                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    "users/1",
                    u => u.Name.Equals("aviv"),
                    TimeSpan.FromSeconds(10),
                    stores.Stores,
                    dbName));

                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(dbName, true));
            }
        }

        [Fact]
        public async Task ClientFailoverInMixedCluster_V41Store()
        {
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.1.0-nightly-20180807-0430",
                "4.0.6"
            });

            var stores = await GetStores(leader, peers,
                modifyDocumentStore: s => s.Conventions.ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin);

            using (stores.Disposable)
            {
                var storeA = stores.Stores[0]; //4.1
                var storeB = stores.Stores[1]; //4.1

                var dbName = await CreateDatabase(storeA, 3);
                await Task.Delay(500);

                using (var session = storeB.OpenSession(dbName))
                {
                    session.Store(new User
                    {
                        Name = "aviv"
                    }, "users/1");
                    session.SaveChanges();
                }

                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    "users/1",
                    u => u.Name.Equals("aviv"),
                    TimeSpan.FromSeconds(10),
                    stores.Stores,
                    dbName));

                // kill node B
                var nodeB = peers.Single(p => p.Url == storeB.Urls[0]).Process;
                KillSlavedServerProcess(nodeB);

                using (var session = storeB.OpenSession(dbName))
                {
                    session.Store(new User
                    {
                        Name = "aviv2"
                    }, "users/2");
                    session.SaveChanges();
                }
                
                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    "users/1",
                    u => u.Name.Equals("aviv"),
                    TimeSpan.FromSeconds(10),
                    stores.Stores,
                    dbName));

                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20)))
                    await storeA.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(dbName, true), cts.Token);
            }
        }

        [Fact]
        public async Task ClientFailoverInMixedCluster_V40Store()
        {
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.0.6"
            }, 1);

            var stores = await GetStores(leader, peers, local,
                modifyDocumentStore: s => s.Conventions.ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin);

            using (stores.Disposable)
            {
                var storeA = stores.Stores[0]; //4.1
                var storeC = stores.Stores[2]; //4.0

                var dbName = await CreateDatabase(storeA, 3);
                await Task.Delay(500);

                using (var session = storeC.OpenSession(dbName))
                {
                    session.Store(new User
                    {
                        Name = "aviv"
                    }, "users/1");
                    session.SaveChanges();
                }

                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    "users/1",
                    u => u.Name.Equals("aviv"),
                    TimeSpan.FromSeconds(10),
                    stores.Stores,
                    dbName));

                // kill node C
                var nodeC = peers[0].Process;
                KillSlavedServerProcess(nodeC);

                using (var session = storeC.OpenSession(dbName))
                {
                    session.Store(new User
                    {
                        Name = "aviv2"
                    }, "users/2");
                    session.SaveChanges();
                }

                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    "users/1",
                    u => u.Name.Equals("aviv"),
                    TimeSpan.FromSeconds(10),
                    stores.Stores,
                    dbName));

                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20)))
                    await storeA.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(dbName, true), cts.Token);
            }
        }

        [Fact]
        public async Task SubscriptionFailoverInMixedCluster_41Mentor()
        {
            var batchSize = 5;
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.1.0-nightly-20180807-0430",
                "4.0.6"
            }, customSettings: new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "1"
            });

            var stores = await GetStores(leader, peers, modifyDocumentStore: s => s.Conventions.DisableTopologyUpdates = false);
            using (stores.Disposable)
            {
                var storeA = stores.Stores[0]; 
                var storeB = stores.Stores[1];

                var dbName = await CreateDatabase(storeA, 3);
                await Task.Delay(500);

                var usersCount = new List<User>();
                var reachedMaxDocCountMre = new AsyncManualResetEvent();

                await CreateDocuments(storeA, dbName ,10);

                var mentor = "B";
                var subscription = await CreateAndInitiateSubscription(leader, storeA, dbName, usersCount, reachedMaxDocCountMre, batchSize, mentor);

                Assert.True(await reachedMaxDocCountMre.WaitAsync(_reasonableWaitTime), $"Reached {usersCount.Count}/10");

                usersCount.Clear();
                reachedMaxDocCountMre.Reset();

                // kill mentor node
                var tag = await GetTagOfServerWhereSubscriptionWorks(leader, dbName, subscription.SubscriptionName);
                Assert.Equal(tag, mentor);
                var nodeB = peers.Single(p => p.Url == storeB.Urls[0]);
                Assert.Equal("4.1.0-nightly-20180807-0430", nodeB.Version);

                KillSlavedServerProcess(nodeB.Process);

                await CreateDocuments(storeA, dbName, 10);

                Assert.True(await reachedMaxDocCountMre.WaitAsync(_reasonableWaitTime), $"Reached {usersCount.Count}/10");

                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20)))
                    await storeA.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(dbName, true), cts.Token);
            }
        }

        [Fact]
        public async Task SubscriptionFailoverInMixedCluster_40Mentor()
        {
            var batchSize = 5;
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.0.6"
            }, localPeers: 1, customSettings: new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "1"
            });

            var stores = await GetStores(leader, peers, local, modifyDocumentStore: s => s.Conventions.DisableTopologyUpdates = false);
            using (stores.Disposable)
            {
                var storeA = stores.Stores[0];
                var storeC = stores.Stores[2];

                var dbName = await CreateDatabase(storeA, 3);
                await Task.Delay(500);

                var usersCount = new List<User>();
                var reachedMaxDocCountMre = new AsyncManualResetEvent();

                await CreateDocuments(storeA, dbName, 10);

                var mentor = "C";
                var subscription = await CreateAndInitiateSubscription(leader, storeA, dbName, usersCount, reachedMaxDocCountMre, batchSize, mentor);

                Assert.True(await reachedMaxDocCountMre.WaitAsync(_reasonableWaitTime), $"Reached {usersCount.Count}/10");

                usersCount.Clear();
                reachedMaxDocCountMre.Reset();

                // kill mentor node
                var tag = await GetTagOfServerWhereSubscriptionWorks(leader, dbName, subscription.SubscriptionName);
                Assert.Equal(tag, mentor);
                var nodeC = peers[0];
                Assert.Equal(storeC.Urls[0], nodeC.Url);
                Assert.Equal("4.0.6", nodeC.Version);

                KillSlavedServerProcess(nodeC.Process);

                await CreateDocuments(storeA, dbName, 10);

                Assert.True(await reachedMaxDocCountMre.WaitAsync(_reasonableWaitTime), $"Reached {usersCount.Count}/10");

                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20)))
                    await storeA.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(dbName, true), cts.Token);
            }
        }

        private async Task CreateDocuments(DocumentStore store, string database, int amount)
        {
            using (var session = store.OpenAsyncSession(database))
            {
                for (var i = 0; i < amount; i++)
                {
                    await session.StoreAsync(new User
                    {
                        Name = $"User{i}"
                    });
                }
                await session.SaveChangesAsync();
            }
        }

        private class SubscriptionProggress
        {
            public int MaxId;
        }

        private readonly TimeSpan _reasonableWaitTime = /*Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) :*/ TimeSpan.FromSeconds(10);

        private async Task<SubscriptionWorker<User>> CreateAndInitiateSubscription(RavenServer server, IDocumentStore store, string database, List<User> usersCount, AsyncManualResetEvent reachedMaxDocCountMre, int batchSize, string mentor)
        {
            var proggress = new SubscriptionProggress()
            {
                MaxId = 0
            };
            var subscriptionName = await store.Subscriptions.CreateAsync<User>(options: new SubscriptionCreationOptions
            {
                MentorNode = mentor
            }, database: database).ConfigureAwait(false);

            var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(subscriptionName)
            {
                TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(500),
                MaxDocsPerBatch = batchSize

            }, database: database);
            var subscripitonState = await store.Subscriptions.GetSubscriptionStateAsync(subscriptionName, database);
            var getDatabaseTopologyCommand = new GetDatabaseRecordOperation(database);
            var record = await store.Maintenance.Server.SendAsync(getDatabaseTopologyCommand).ConfigureAwait(false);

            await server.ServerStore.Cluster.WaitForIndexNotification(subscripitonState.SubscriptionId).ConfigureAwait(false);

            Assert.Equal(mentor, record.Topology.WhoseTaskIsIt(RachisState.Follower, subscripitonState, null));

            var task = subscription.Run(a =>
            {
                foreach (var item in a.Items)
                {
                    var x = item.Result;
                    int curId = 0;
                    var afterSlash = x.Id.Substring(x.Id.LastIndexOf("/", StringComparison.OrdinalIgnoreCase) + 1);
                    curId = int.Parse(afterSlash.Substring(0, afterSlash.Length - 2));
                    Assert.True(curId >= proggress.MaxId);
                    usersCount.Add(x);
                    proggress.MaxId = curId;
                }
            });
            subscription.AfterAcknowledgment += b =>
            {
                try
                {
                    if (usersCount.Count == 10)
                    {
                        reachedMaxDocCountMre.Set();
                    }
                }
                catch (Exception)
                {


                }
                return Task.CompletedTask;
            };

            await Task.WhenAny(task, Task.Delay(_reasonableWaitTime)).ConfigureAwait(false);

            return subscription;
        }

        private static async Task<string> GetTagOfServerWhereSubscriptionWorks(RavenServer server, string database, string subscriptionName)
        {
            using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var databaseRecord = server.ServerStore.Cluster.ReadDatabase(context, database);
                var db = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database).ConfigureAwait(false);
                var subscriptionState = db.SubscriptionStorage.GetSubscriptionFromServerStore(subscriptionName);
                return databaseRecord.Topology.WhoseTaskIsIt(server.ServerStore.Engine.CurrentState, subscriptionState, null);
            }
        }

        private static void DisposeServerAndWaitForFinishOfDisposal(RavenServer serverToDispose)
        {
            var mre = new ManualResetEventSlim();
            serverToDispose.AfterDisposal += () => mre.Set();           
            serverToDispose.Dispose();
            mre.Wait();
        }
    }
}
