using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Identities;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Extensions;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Cluster;
using Sparrow.Json;
using Sparrow.Server;
using Tests.Infrastructure;
using Tests.Infrastructure.InterversionTest;
using Xunit;
using Xunit.Abstractions;

namespace InterversionTests
{
    public class MixedClusterTests : MixedClusterTestBase
    {
        public MixedClusterTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformTheory(RavenTestCategory.Interversion, RavenPlatform.Windows)]
        [InlineData("4.1.7", "4.1.7", "4.1.7")]
        [InlineData("4.1.6", "4.1.6", "4.1.6")]
        [InlineData("4.1.5", "4.1.5", "4.1.5")]
        public async Task UpgradeDirectlyFrom41X(params string[] initialVersions)
        {
            var upgradeTo = new List<string>
            {
                "current"
            };
            var suit = new Version41X(this);
            await ExecuteUpgradeTest(initialVersions, upgradeTo, suit, suit, suit);
        }

        [RavenMultiplatformTheory(RavenTestCategory.Interversion, RavenPlatform.Windows)]
        [InlineData("4.1.5", "4.1.5", "4.1.5")]
        public async Task UpgradeFromEarly41(params string[] initialVersions)
        {
            var upgradeTo = new List<string>
            {
                "4.1.7",
                "current"
            };

            var suit = new Version41X(this);
            await ExecuteUpgradeTest(initialVersions, upgradeTo, suit, suit, suit);
        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task IncrementalUpgrade()
        {
            var initialVersions = new[] { "4.0.11", "4.0.11", "4.0.11" };
            var v40 = new Version40X(this);
            var v41 = new Version41X(this);

            var upgradeTo41X = new List<string>
            {
                "4.1.7",
                "4.2.1",
                "current"
            };
            await ExecuteUpgradeTest(initialVersions, upgradeTo41X, v40, v40, v41);
        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task UpgradeFromLatest40()
        {
            var initialVersions = new[] { "4.0.11", "4.0.11", "4.0.11" };
            var upgradeTo = new List<string>
            {
                "4.1.7",
                "current"
            };
            var before = new Version40X(this);
            var after = new Version41X(this);
            await ExecuteUpgradeTest(initialVersions, upgradeTo, before, before, after);
        }

        [RavenMultiplatformTheory(RavenTestCategory.Interversion, RavenPlatform.Windows)]
        [InlineData("4.2.0", "4.2.0", "4.2.0")]
        [InlineData("4.2.1", "4.2.1", "4.2.1")]
        public async Task UpgradeDirectlyFrom42X(params string[] initialVersions)
        {
            var upgradeTo = new List<string>
            {
                "current"
            };
            var suit = new Version41X(this);
            await ExecuteUpgradeTest(initialVersions, upgradeTo, suit, suit, suit);
        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion, RavenPlatform.Windows)]
        public async Task UpgradeToLatest()
        {
            var latest = "4.2.119";
            var initialVersions = new[] { latest, latest, latest };
            var upgradeTo = new List<string>
            {
                "current"
            };
            var suit = new Version41X(this);
            await ExecuteUpgradeTest(initialVersions, upgradeTo, suit, suit, suit);
        }

        [RavenMultiplatformTheory(RavenTestCategory.Interversion, RavenPlatform.Windows | RavenPlatform.Linux)]
        [InlineData("6.0.104")]
        public async Task UpgradeDirectlyFrom60X(string latest)
        {
            var initialVersions = new[] { latest, latest, latest };
            var upgradeTo = new List<string>
            {
                "current"
            };
            var suit = new Version60X(this);
            await ExecuteUpgradeTest(initialVersions, upgradeTo, suit, suit, suit);
        }

        [RavenMultiplatformTheory(RavenTestCategory.Interversion, RavenPlatform.Windows | RavenPlatform.Linux)]
        [InlineData("5.4.5")]
        [InlineData("5.4.109")]
        public async Task UpgradeDirectlyFrom54X(string latest)
        {
            var initialVersions = new[] { latest, latest, latest };
            var upgradeTo = new List<string>
            {
                "current"
            };
            var suit = new Version54X(this);
            await ExecuteUpgradeTest(initialVersions, upgradeTo, suit, suit, suit);
        }

        [RavenMultiplatformTheory(RavenTestCategory.Interversion, RavenPlatform.Windows | RavenPlatform.Linux)]
        [InlineData("5.2.116")]
        public async Task UpgradeDirectlyFrom52X(string latest)
        {
            var initialVersions = new[] { latest, latest, latest };
            var upgradeTo = new List<string>
            {
                "current"
            };
            var suit = new Version54X(this);
            await ExecuteUpgradeTest(initialVersions, upgradeTo, suit, suit, suit);
        }

        private async Task ExecuteUpgradeTest(
            string[] initialVersions,
            List<string> upgradeTo,
            UpgradeTestSuit before,
            UpgradeTestSuit during,
            UpgradeTestSuit after)
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;

            var nodes = await CreateCluster(initialVersions);
            var database = GetDatabaseName();
            var result = await GetStores(database, nodes);

            using (result.Disposable)
            {
                try
                {
                    var stores = result.Stores;
                    await CreateDatabase(stores, database, nodes.Count);
                    await before.TestClustering(stores, "before");
                    await before.TestReplication(stores);

                    foreach (var nextVersion in upgradeTo)
                    {
                        foreach (var node in nodes)
                        {
                            await UpgradeServerAsync(nextVersion, node);
                            var versions = nodes.Select(x => x.Version).ToArray();

                            if (ShouldTestClustering(versions))
                            {
                                await during.TestClustering(stores, $"during/{nextVersion}/{node.Url}");
                            }

                            await during.TestReplication(stores);
                        }
                    }

                    await after.TestClustering(stores, $"after");
                    await after.TestReplication(stores);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            }
        }


        private static bool ShouldTestClustering(string[] versions)
        {
            var last = IsGreaterOrEqual421(versions[0]);

            foreach (var version in versions)
            {
                var current = IsGreaterOrEqual421(version);
                if (last != current)
                    return false;
            }

            return true;
        }

        public static bool IsGreaterOrEqual421(string version)
        {
            if (version == "current")
                return true;

            const string threshold = "4.2.1";
            Version inputVersion = Version.Parse(version);
            Version thresholdVersion = Version.Parse(threshold);

            return inputVersion >= thresholdVersion;
        }

        private static async Task CreateDatabase(List<DocumentStore> stores, string database, int size)
        {
            try
            {
                var databaseRecord = new DatabaseRecord(database)
                {
                    Settings =
                    {
                        [RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat)] = "1",
                        [RavenConfiguration.GetKey(x => x.Replication.RetryReplicateAfter)] = "1"
                    }
                };

                var result = await stores[0].Maintenance.Server.SendAsync(new CreateDatabaseOperation(databaseRecord, size));
                foreach (var store in stores)
                {
                    using (var context = JsonOperationContext.ShortTermSingleUse())
                    {
                        await store.GetRequestExecutor().ExecuteAsync(new WaitForRaftIndexCommand(result.RaftCommandIndex), context);
                    }
                }
            }
            catch (ConcurrencyException)
            {
                // database already exists
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Replication, RavenPlatform.Windows, Skip = "WIP")]
        public async Task ReplicationInMixedCluster_40Leader_with_two_41_nodes()
        {
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.0.7"
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

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Replication, RavenPlatform.Windows, Skip = "WIP")]
        public async Task ReplicationInMixedCluster_40Leader_with_one_41_node_and_two_40_nodes()
        {
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.0.7",
                "4.0.7"
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

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Replication, RavenPlatform.Windows, Skip = "WIP")]
        public async Task ReplicationInMixedCluster_41Leader_with_two_406()
        {
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.0.7",
                "4.0.7"
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

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Replication, RavenPlatform.Windows, Skip = "WIP")]
        public async Task MixedCluster_OutgoingReplicationFrom41To40_ShouldStopAfterUsingCounters()
        {
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.0.7",
                "4.0.7"
            }, 1);

            var stores = await GetStores(leader, peers, local);
            using (stores.Disposable)
            {
                var storeA = stores.Stores[0]; // 4.1
                var storeB = stores.Stores[1]; // 4.1
                var storeC = stores.Stores[2]; // 4.0
                var storeD = stores.Stores[3]; // 4.0

                var dbName = await CreateDatabase(storeA, 4);
                await Task.Delay(500);

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
                    TimeSpan.FromSeconds(15),
                    stores.Stores,
                    dbName));

                using (var session = storeA.OpenSession(dbName))
                {
                    // using Counters here should stop the outgoing 
                    // replication from this node (A) to the 4.0 nodes (C, D)
                    session.CountersFor("users/1").Increment("likes", 100);
                    session.SaveChanges();
                }

                foreach (var store in new[] { storeA, storeB })
                {
                    using (var session = store.OpenSession(dbName))
                    {
                        var val = session.CountersFor("users/1").Get("likes");
                        Assert.Equal(100, val);
                    }
                }

                foreach (var store in new[] { storeC, storeD })
                {
                    using (var session = store.OpenSession(dbName))
                    {
                        Assert.Throws<ClientVersionMismatchException>(() => session.CountersFor("users/1").Get("likes"));
                    }
                }

                using (var session = storeA.OpenSession(dbName))
                {
                    // should only be replicated to node B 
                    session.Store(new User
                    {
                        Name = "aviv2"
                    }, "users/2");
                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(
                    storeB,
                    "users/2",
                    u => true,
                    (int)TimeSpan.FromSeconds(5).TotalMilliseconds,
                    dbName));

                Assert.False(WaitForDocument<User>(
                    storeC,
                    "users/2",
                    u => true,
                    (int)TimeSpan.FromSeconds(5).TotalMilliseconds,
                    dbName));

                Assert.False(WaitForDocument<User>(
                    storeD,
                    "users/2",
                    u => true,
                    (int)TimeSpan.FromSeconds(5).TotalMilliseconds,
                    dbName));

                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(dbName, true));
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Replication, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task ReplicationInMixedCluster_60Leader_with_two_54()
        {
            var nodes = await CreateCluster(new[] { "5.4.101", "5.4.101", "5.4.101" });
            var database = GetDatabaseName();

            var stores = await GetStores(database, nodes);

            using (stores.Disposable)
            {
                var storeA = stores.Stores[0];
                
                ProcessNode leader;
                using (var requestExecutor = ClusterRequestExecutor.CreateForShortTermUse(storeA.Urls[0], certificate : null, DocumentConventions.DefaultForServer))
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var clusterTopology = new GetClusterTopologyCommand();
                    await requestExecutor.ExecuteAsync(clusterTopology, context);

                    Assert.True(clusterTopology.Result.Topology.Members.TryGetValue(clusterTopology.Result.Leader, out var leaderUrl));
                    leader = nodes.Single(n => n.Url == leaderUrl);
                }

                await UpgradeServerAsync("current", leader);

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

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Subscriptions, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task SingleSubscriptionMixedClusterEnsureDocumentResend54()
        {
            var proccess526List = await CreateCluster(new string[] { "5.4.101", "5.4.101" });
            await UpgradeServerAsync("current", proccess526List[0]);

            using (var store53 = await GetStore(proccess526List[0].Url, proccess526List[0].Process, null,
                       new InterversionTestOptions() { ReplicationFactor = 2, CreateDatabase = true }))
            {
                var subscriptionId = await store53.Subscriptions.CreateAsync<User>(new SubscriptionCreationOptions<User>()
                {
                    MentorNode = "A",
                    Name = Guid.NewGuid().ToString()
                });

                using (var session = store53.OpenAsyncSession(store53.Database))
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }

                await using (var worker = store53.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(subscriptionId)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(3),
                    MaxDocsPerBatch = 1,
                }))
                {
                    try
                    {
                        await worker.Run(x =>
                        {
                            if (x.Items[0].Id == "users/1")
                            {
                                throw new SubscriptionClosedException("stop the subscription worker without acking users/1 and reconnect", false);
                            }
                        });
                    }
                    catch
                    {
                        //suppress the error
                    }
                }

                using (var session = store53.OpenAsyncSession(store53.Database))
                {
                    await session.StoreAsync(new User(), "users/2");
                    await session.SaveChangesAsync();
                }

                await using (var worker = store53.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(subscriptionId)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(3),
                    MaxDocsPerBatch = 1,
                }))
                {
                    AsyncManualResetEvent amre = new();
                    var users1Resent = false;

                    _ = worker.Run(x =>
                    {
                        if (x.Items[0].Id == "users/1")
                        {
                            //should get users/1 again since it didn't get to ack
                            users1Resent = true;
                        }

                        if (x.Items[0].Id == "users/2")
                        {
                            amre.Set();
                        }
                    });

                    await amre.WaitAsync(TimeSpan.FromSeconds(15));
                    Assert.True(users1Resent, "users/1 hasn't been resent");
                }
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Subscriptions, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task SingleSubscriptionMixedClusterStartAgainFromSpecificChangeVector()
        {
            var proccess526List = await CreateCluster(new string[] { "5.4.101", "5.4.101" });
            await UpgradeServerAsync("current", proccess526List[0]);

            using (var store53 = await GetStore(proccess526List[0].Url, proccess526List[0].Process, null,
                       new InterversionTestOptions() { ReplicationFactor = 2, CreateDatabase = true }))
            {
                var subscriptionName = await store53.Subscriptions.CreateAsync<User>(new SubscriptionCreationOptions<User>()
                {
                    MentorNode = "A"
                });

                using (var session = store53.OpenAsyncSession(store53.Database))
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.StoreAsync(new User(), "users/2");
                    await session.SaveChangesAsync();
                }

                await using (var worker = store53.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(subscriptionName)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(3),
                    MaxDocsPerBatch = 1,
                }))
                {
                    var amre = new AsyncManualResetEvent();
                    amre.Set();
                    var filesReceived = 0;
                    var changeVectorOfFirstDoc = "";
                    var idOf2ndDoc = "";
                    var idOf3rdDoc = "";

                    worker.AfterAcknowledgment += async batch =>
                    {
                        if (batch.Items[0].Id == idOf2ndDoc && filesReceived == 2)
                        {
                            await store53.Subscriptions.UpdateAsync(new SubscriptionUpdateOptions()
                            {
                                ChangeVector = changeVectorOfFirstDoc,
                                Name = worker.SubscriptionName
                            });
                            amre.Set();
                        }
                    };

                    _ = worker.Run(async x =>
                    {
                        await amre.WaitAsync(_reasonableWaitTime);

                        filesReceived++;

                        if (filesReceived == 1)
                            changeVectorOfFirstDoc = x.Items[0].ChangeVector;

                        if (filesReceived == 2)
                        {
                            idOf2ndDoc = x.Items[0].Id;
                            amre.Reset();
                        }

                        if (filesReceived >= 3)
                            idOf3rdDoc = x.Items[0].Id;

                    });
                    await WaitForValueAsync(() => Task.FromResult(idOf2ndDoc == idOf3rdDoc && string.IsNullOrWhiteSpace(idOf3rdDoc) == false), true);
                }
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Subscriptions, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task SingleSubscriptionMixedClusterV54StartAgainFromSpecificChangeVector()
        {
            var proccess541List = await CreateCluster(new string[] { "5.4.101", "5.4.101" });
            await UpgradeServerAsync("current", proccess541List[0]);

            using (var store = await GetStore(proccess541List[0].Url, proccess541List[0].Process, null,
                       new InterversionTestOptions() { ReplicationFactor = 2, CreateDatabase = true }))
            {
                var subscriptionName = await store.Subscriptions.CreateAsync<User>(new SubscriptionCreationOptions<User>()
                {
                    MentorNode = "A"
                });

                using (var session = store.OpenAsyncSession(store.Database))
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.StoreAsync(new User(), "users/2");
                    await session.SaveChangesAsync();
                }

                await using (var worker = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(subscriptionName)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(3),
                    MaxDocsPerBatch = 1,
                }))
                {
                    var amre = new AsyncManualResetEvent();
                    amre.Set();
                    var filesReceived = 0;
                    var changeVectorOfFirstDoc = "";
                    var idOf2ndDoc = "";
                    var idOf3rdDoc = "";

                    worker.AfterAcknowledgment += async batch =>
                    {
                        if (batch.Items[0].Id == idOf2ndDoc && filesReceived == 2)
                        {
                            await store.Subscriptions.UpdateAsync(new SubscriptionUpdateOptions()
                            {
                                ChangeVector = changeVectorOfFirstDoc,
                                Name = worker.SubscriptionName
                            });
                            amre.Set();
                        }
                    };

                    _ = worker.Run(async x =>
                    {
                        await amre.WaitAsync(_reasonableWaitTime);

                        filesReceived++;

                        if (filesReceived == 1)
                            changeVectorOfFirstDoc = x.Items[0].ChangeVector;

                        if (filesReceived == 2)
                        {
                            idOf2ndDoc = x.Items[0].Id;
                            amre.Reset();
                        }

                        if (filesReceived >= 3)
                            idOf3rdDoc = x.Items[0].Id;

                    });
                    await WaitForValueAsync(() => Task.FromResult(idOf2ndDoc == idOf3rdDoc && string.IsNullOrWhiteSpace(idOf3rdDoc) == false), true);
                }
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.ClientApi, RavenPlatform.Windows, Skip = "WIP")]
        public async Task ClientFailoverInMixedCluster_V41Store()
        {
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.0.7",
                "4.0.7"
            });

            var stores = await GetStores(leader, peers,
                modifyDocumentStore: s => s.Conventions.ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin);

            using (stores.Disposable)
            {
                var storeA = stores.Stores[0]; //4.1

                var dbName = await CreateDatabase(storeA, 3);
                await Task.Delay(500);

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

                // kill node A (4.1)              
                DisposeServerAndWaitForFinishOfDisposal(leader);

                using (var session = storeA.OpenSession(dbName))
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

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.ClientApi, RavenPlatform.Windows, Skip = "WIP")]
        public async Task ClientFailoverInMixedCluster_V40Store()
        {
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.0.7"
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

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Subscriptions, RavenPlatform.Windows, Skip = "WIP")]
        public async Task SubscriptionsInMixedCluster_FailoverFrom41To40()
        {
            var batchSize = 5;
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.0.7",
                "4.0.7"
            }, customSettings: new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "1"
            });

            var stores = await GetStores(leader, peers, modifyDocumentStore: s => s.Conventions.DisableTopologyUpdates = false);
            using (stores.Disposable)
            {
                var storeA = stores.Stores[0];

                var dbName = await CreateDatabase(storeA, 3);
                await Task.Delay(500);

                var usersCount = new List<User>();
                var reachedMaxDocCountMre = new AsyncManualResetEvent();

                await CreateDocuments(storeA, dbName, 10);

                var mentor = "A";
                var subscription = await CreateAndInitiateSubscription(leader, storeA, dbName, usersCount, reachedMaxDocCountMre, batchSize, mentor);

                Assert.True(await reachedMaxDocCountMre.WaitAsync(_reasonableWaitTime), $"Reached {usersCount.Count}/10");

                usersCount.Clear();
                reachedMaxDocCountMre.Reset();

                // kill mentor node (4.1)
                var tag = await GetTagOfServerWhereSubscriptionWorks(storeA, dbName, subscription.SubscriptionName);
                Assert.Equal(mentor, tag);
                DisposeServerAndWaitForFinishOfDisposal(leader);

                await CreateDocuments(storeA, dbName, 10);

                Assert.True(await reachedMaxDocCountMre.WaitAsync(_reasonableWaitTime), $"Reached {usersCount.Count}/10");

                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20)))
                    await storeA.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(dbName, true), cts.Token);
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Subscriptions, RavenPlatform.Windows, Skip = "WIP")]
        public async Task SubscriptionsInMixedCluster_FailoverFrom410To41()
        {
            var batchSize = 5;
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.0.7"
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
                var tag = await GetTagOfServerWhereSubscriptionWorks(storeA, dbName, subscription.SubscriptionName);
                Assert.Equal(tag, mentor);
                var nodeC = peers[0];
                Assert.Equal(storeC.Urls[0], nodeC.Url);
                Assert.Equal("4.0.7", nodeC.Version);

                KillSlavedServerProcess(nodeC.Process);

                await CreateDocuments(storeA, dbName, 10);

                Assert.True(await reachedMaxDocCountMre.WaitAsync(_reasonableWaitTime), $"Reached {usersCount.Count}/10");

                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20)))
                    await storeA.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(dbName, true), cts.Token);
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Replication, RavenPlatform.Windows, Skip = "WIP")]
        public async Task V40Cluster_V41Client_BasicReplication()
        {
            var nodeA = await GetServerAsync("4.0.7");
            var nodeB = await GetServerAsync("4.0.7");
            var nodeC = await GetServerAsync("4.0.7");

            using (var storeA = await GetStore(nodeA.Url, nodeA.Process, null, new InterversionTestOptions
            {
                ModifyDocumentStore = store => store.Conventions.DisableTopologyUpdates = true
            }))
            using (var storeB = await GetStore(nodeB.Url, nodeB.Process, null, new InterversionTestOptions
            {
                CreateDatabase = false,
                ModifyDocumentStore = store => store.Conventions.DisableTopologyUpdates = true
            }))
            using (var storeC = await GetStore(nodeC.Url, nodeC.Process, null, new InterversionTestOptions
            {
                CreateDatabase = false,
                ModifyDocumentStore = store => store.Conventions.DisableTopologyUpdates = true
            }))
            {
                await AddNodeToCluster(storeA, storeB.Urls[0]);
                await Task.Delay(2500);
                await AddNodeToCluster(storeA, storeC.Urls[0]);
                await Task.Delay(1000);

                var dbName = await CreateDatabase(storeA, 3);
                await Task.Delay(500);

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
                    new List<DocumentStore>
                    {
                        storeA, storeB, storeC
                    },
                    dbName));

                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(storeA.Database, true));
                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(dbName, true));
            }

        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Cluster, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task ClusterTcpCompressionTest()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            
            var nodes = await CreateCluster(new[] { "5.4.0", "5.4.0", "5.4.0" });
            await UpgradeServerAsync("current", nodes[0]);

            var database = GetDatabaseName();
            var (disposable, stores) = await GetStores(database, nodes);
            using (disposable)
            {
                await CreateDatabase(stores, database, nodes.Count);
            }

        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Counters, RavenPlatform.Windows, Skip = "WIP")]
        public async Task V40Cluster_V41Client_Counters()
        {
            var nodeA = await GetServerAsync("4.0.7");
            var nodeB = await GetServerAsync("4.0.7");
            var nodeC = await GetServerAsync("4.0.7");

            using (var storeA = await GetStore(nodeA.Url, nodeA.Process, null, new InterversionTestOptions
            {
                ModifyDocumentStore = store => store.Conventions.DisableTopologyUpdates = true
            }))
            using (var storeB = await GetStore(nodeB.Url, nodeB.Process, null, new InterversionTestOptions
            {
                CreateDatabase = false,
                ModifyDocumentStore = store => store.Conventions.DisableTopologyUpdates = true
            }))
            using (var storeC = await GetStore(nodeC.Url, nodeC.Process, null, new InterversionTestOptions
            {
                CreateDatabase = false,
                ModifyDocumentStore = store => store.Conventions.DisableTopologyUpdates = true
            }))
            {
                await AddNodeToCluster(storeA, storeB.Urls[0]);
                await Task.Delay(2500);
                await AddNodeToCluster(storeA, storeC.Urls[0]);
                await Task.Delay(1000);

                var dbName = await CreateDatabase(storeA, 3);
                await Task.Delay(500);

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
                    new List<DocumentStore>
                    {
                        storeA, storeB, storeC
                    },
                    dbName));

                using (var session = storeA.OpenSession(dbName))
                {
                    session.CountersFor("users/1").Increment("likes");
                    Assert.Throws<RavenException>(() => session.SaveChanges());
                }
                using (var session = storeA.OpenSession(dbName))
                {
                    session.CountersFor("users/1").Delete("likes");
                    Assert.Throws<RavenException>(() => session.SaveChanges());
                }

                using (var session = storeA.OpenSession(dbName))
                {
                    Assert.Throws<ClientVersionMismatchException>(() => session.CountersFor("users/1").Get("likes"));
                }

                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(storeA.Database, true));
                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(dbName, true));
            }

        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.ClusterTransactions, RavenPlatform.Windows, Skip = "WIP")]
        public async Task V40Cluster_V41Client_ClusterTransactions()
        {
            var nodeA = await GetServerAsync("4.0.7");
            var nodeB = await GetServerAsync("4.0.7");
            var nodeC = await GetServerAsync("4.0.7");

            using (var storeA = await GetStore(nodeA.Url, nodeA.Process, null, new InterversionTestOptions
            {
                ModifyDocumentStore = store => store.Conventions.DisableTopologyUpdates = true
            }))
            using (var storeB = await GetStore(nodeB.Url, nodeB.Process, null, new InterversionTestOptions
            {
                CreateDatabase = false,
                ModifyDocumentStore = store => store.Conventions.DisableTopologyUpdates = true
            }))
            using (var storeC = await GetStore(nodeC.Url, nodeC.Process, null, new InterversionTestOptions
            {
                CreateDatabase = false,
                ModifyDocumentStore = store => store.Conventions.DisableTopologyUpdates = true
            }))
            {
                await AddNodeToCluster(storeA, storeB.Urls[0]);
                await Task.Delay(2500);
                await AddNodeToCluster(storeA, storeC.Urls[0]);
                await Task.Delay(1000);

                var dbName = await CreateDatabase(storeA, 3);
                await Task.Delay(500);

                var user1 = new User
                {
                    Name = "Karmel"
                };
                var user3 = new User
                {
                    Name = "Indych"
                };

                using (var session = storeA.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("usernames/ayende", user1);
                    await session.StoreAsync(user3, "foo/bar");
                    await Assert.ThrowsAsync<RavenException>(async () => await session.SaveChangesAsync());

                    var value = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("usernames/ayende");
                    Assert.Null(value);
                }

                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(storeA.Database, true));
                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(dbName, true));
            }

        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Revisions, RavenPlatform.Windows, Skip = "WIP")]
        public async Task RevisionsInMixedCluster()
        {
            var company = new Company { Name = "Company Name" };

            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.0.7",
                "4.0.7"
            });

            var stores = await GetStores(leader, peers);

            using (stores.Disposable)
            {
                var storeA = stores.Stores[0];

                var dbName = await CreateDatabase(storeA, 3);
                await Task.Delay(500);

                await RevisionsHelper.SetupRevisionsAsync(storeA, dbName);
                using (var session = storeA.OpenAsyncSession(dbName))
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }
                using (var session = storeA.OpenAsyncSession(dbName))
                {
                    var company2 = await session.LoadAsync<Company>(company.Id);
                    company2.Name = "Hibernating Rhinos";
                    await session.SaveChangesAsync();
                }

                Assert.True(await WaitForDocumentInClusterAsync<Company>(
                    company.Id,
                    u => u.Name.Equals("Hibernating Rhinos"),
                    TimeSpan.FromSeconds(10),
                    stores.Stores,
                    dbName));

                foreach (var store in stores.Stores)
                {
                    using (var session = store.OpenAsyncSession(dbName))
                    {
                        var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                        Assert.Equal(2, companiesRevisions.Count);
                        Assert.Equal("Hibernating Rhinos", companiesRevisions[0].Name);
                        Assert.Equal("Company Name", companiesRevisions[1].Name);
                    }
                }

                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20)))
                    await storeA.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(dbName, true), cts.Token);
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion, RavenPlatform.Windows, Skip = "WIP")]
        public async Task MixedCluster_ClusterWideIdentity()
        {

            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.0.7",
                "4.0.7"
            });

            var stores = await GetStores(leader, peers);
            var leaderStore = stores.Stores.Single(s => s.Urls[0] == leader.WebUrl);
            var nonLeader = stores.Stores.First(s => s.Urls[0] != leader.WebUrl);

            using (stores.Disposable)
            {
                var dbName = await CreateDatabase(leaderStore, 3);
                await Task.Delay(500);

                using (var session = nonLeader.OpenAsyncSession(dbName))
                {
                    var result = nonLeader.Maintenance.Send(new SeedIdentityForOperation("users", 1990));
                    Assert.Equal(1990, result);
                    var user = new User
                    {
                        Name = "Adi",
                        LastName = "Async"
                    };
                    await session.StoreAsync(user, "users|");
                    await session.SaveChangesAsync();
                    var id = session.Advanced.GetDocumentId(user);
                    Assert.Equal("users/1991", id);
                }
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Cluster, RavenPlatform.Windows, Skip = "WIP")]
        public async Task MixedCluster_CanReorderDatabaseNodes()
        {
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.0.7",
                "4.0.7"
            });

            var stores = await GetStores(leader, peers);

            using (stores.Disposable)
            {
                var storeA = stores.Stores[0];

                var dbName = await CreateDatabase(storeA, 3);
                await Task.Delay(500);

                await ClusterOperationTests.ReverseOrderSuccessfully(storeA, dbName);
                await ClusterOperationTests.FailSuccessfully(storeA, dbName);
            }

        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Subscriptions | RavenTestCategory.Revisions, RavenPlatform.Windows, Skip = "WIP")]
        public async Task MixedCluster_DistributedRevisionsSubscription()
        {
            var uniqueRevisions = new HashSet<string>();
            var uniqueDocs = new HashSet<string>();
            var nodesAmount = 5;
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.0.7",
                "4.0.7"
            }, 2, new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "1"
            });
            var stores = await GetStores(leader, peers, local, modifyDocumentStore: s => s.Conventions.DisableTopologyUpdates = false);

            using (stores.Disposable)
            {
                var storeA = stores.Stores[0];
                var dbName = await CreateDatabase(storeA, nodesAmount);
                await Task.Delay(500);

                await RevisionsHelper.SetupRevisionsAsync(storeA, dbName).ConfigureAwait(false);

                var reachedMaxDocCountMre = new AsyncManualResetEvent();
                var ackSent = new AsyncManualResetEvent();

                var continueMre = new AsyncManualResetEvent();
                await GenerateDistributedRevisionsDataAsync(dbName, stores.Stores);

                var subscriptionId = await storeA.Subscriptions.CreateAsync<Revision<User>>(database: dbName).ConfigureAwait(false);
                var docsCount = 0;
                var revisionsCount = 0;
                var expectedRevisionsCount = 0;
                SubscriptionWorker<Revision<User>> subscription = null;
                int i;
                for (i = 0; i < 10; i++)
                {
                    subscription = storeA.Subscriptions.GetSubscriptionWorker<Revision<User>>(new SubscriptionWorkerOptions(subscriptionId)
                    {
                        MaxDocsPerBatch = 1,
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(100)
                    }, dbName);

                    subscription.AfterAcknowledgment += async b =>
                    {
                        Assert.True(await continueMre.WaitAsync(TimeSpan.FromSeconds(60)));

                        try
                        {
                            if (revisionsCount == expectedRevisionsCount)
                            {
                                continueMre.Reset();
                                ackSent.Set();
                            }

                            Assert.True(await continueMre.WaitAsync(TimeSpan.FromSeconds(60)));
                        }
                        catch (Exception)
                        {

                        }
                    };
                    var started = new AsyncManualResetEvent();

                    var task = subscription.Run(b =>
                    {
                        started.Set();
                        HandleSubscriptionBatch(nodesAmount, b, uniqueDocs, ref docsCount, uniqueRevisions, reachedMaxDocCountMre, ref revisionsCount);
                    });

                    await Task.WhenAny(task, started.WaitAsync(TimeSpan.FromSeconds(60)));

                    if (started.IsSet)
                        break;

                    Assert.IsType<SubscriptionDoesNotExistException>(task.Exception.InnerException);

                    subscription.Dispose();
                }

                Assert.NotEqual(i, 10);

                expectedRevisionsCount = nodesAmount + 2;
                continueMre.Set();

                Assert.True(await ackSent.WaitAsync(_reasonableWaitTime).ConfigureAwait(false), $"Doc count is {docsCount} with revisions {revisionsCount}/{expectedRevisionsCount} (1st assert)");
                ackSent.Reset(true);

                await KillServerWhereSubscriptionWorks(storeA, dbName, subscription.SubscriptionName, (leader, peers, local)).ConfigureAwait(false);

                continueMre.Set();
                expectedRevisionsCount += 2;

                Assert.True(await ackSent.WaitAsync(_reasonableWaitTime).ConfigureAwait(false), $"Doc count is {docsCount} with revisions {revisionsCount}/{expectedRevisionsCount} (2nd assert)");
                ackSent.Reset(true);
                continueMre.Set();
                expectedRevisionsCount = (int)Math.Pow(nodesAmount, 2);

                await KillServerWhereSubscriptionWorks(storeA, dbName, subscription.SubscriptionName, (leader, peers, local)).ConfigureAwait(false);

                Assert.True(await reachedMaxDocCountMre.WaitAsync(_reasonableWaitTime).ConfigureAwait(false), $"Doc count is {docsCount} with revisions {revisionsCount}/{expectedRevisionsCount} (3rd assert)");
            }
        }

        private async Task KillServerWhereSubscriptionWorks(IDocumentStore store, string databaseName, string subscriptionName, (RavenServer Leader, List<ProcessNode> Peers, List<RavenServer> LocalPeers) servers)
        {
            var tag = await GetTagOfServerWhereSubscriptionWorks(store, databaseName, subscriptionName);
            Assert.NotNull(tag);

            switch (tag)
            {
                case "A":
                    DisposeServerAndWaitForFinishOfDisposal(servers.Leader);
                    break;
                case "B":
                    DisposeServerAndWaitForFinishOfDisposal(servers.LocalPeers[0]);
                    break;
                case "C":
                    DisposeServerAndWaitForFinishOfDisposal(servers.LocalPeers[1]);
                    break;
                case "D":
                    KillSlavedServerProcess(servers.Peers[0].Process);
                    break;
                case "E":
                    KillSlavedServerProcess(servers.Peers[1].Process);
                    break;
                default:
                    throw new InvalidOperationException($"Unexpected node tag : {tag} ");
            }
        }

        private async Task GenerateDistributedRevisionsDataAsync(string databaseName, List<DocumentStore> stores)
        {
            using (var store = new DocumentStore
            {
                Urls = stores[0].Urls,
                Database = databaseName
            }.Initialize())
            {
                await store.GetRequestExecutor().UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(new ServerNode
                {
                    Url = store.Urls[0],
                    Database = databaseName,
                })
                {
                    TimeoutInMs = Timeout.Infinite
                });
            }

            var storesWithoutTopologyUpdates = new List<DocumentStore>();
            foreach (var s in stores)
            {
                storesWithoutTopologyUpdates.Add((DocumentStore)new DocumentStore
                {
                    Urls = s.Urls,
                    Database = databaseName,
                    Conventions = new DocumentConventions
                    {
                        DisableTopologyUpdates = true
                    }
                }.Initialize());
            }


            var rnd = new Random(1);
            for (var index = 0; index < stores.Count; index++)
            {
                var curVer = 0;
                foreach (var s in stores.OrderBy(x => rnd.Next()))
                {
                    using (var curStore = new DocumentStore
                    {
                        Urls = s.Urls,
                        Database = databaseName,
                        Conventions = new DocumentConventions
                        {
                            DisableTopologyUpdates = true
                        }
                    }.Initialize())
                    {
                        var curDocName = $"user {index} revision {curVer}";
                        using (var session = (DocumentSession)curStore.OpenSession(databaseName))
                        {
                            if (curVer == 0)
                            {
                                session.Store(new User
                                {
                                    Name = curDocName,
                                    Age = curVer,
                                    Id = $"users/{index}"
                                }, $"users/{index}");
                            }
                            else
                            {
                                var user = session.Load<User>($"users/{index}");
                                user.Age = curVer;
                                user.Name = curDocName;
                                session.Store(user, $"users/{index}");
                            }

                            session.SaveChanges();

                            Assert.True(
                                AsyncHelpers.RunSync(() => WaitForDocumentInClusterAsync<User>(
                                    "users/" + index,
                                    x => x.Name == curDocName,
                                    _reasonableWaitTime,
                                    storesWithoutTopologyUpdates,
                                    databaseName))
                                );
                        }
                    }
                    curVer++;
                }

            }

            foreach (var s in storesWithoutTopologyUpdates)
            {
                s.Dispose();
            }
        }

        private static void HandleSubscriptionBatch(int nodesAmount, SubscriptionBatch<Revision<User>> b, HashSet<string> uniqueDocs, ref int docsCount, HashSet<string> uniqueRevisions,
            AsyncManualResetEvent reachedMaxDocCountMre, ref int revisionsCount)
        {
            foreach (var item in b.Items)
            {
                var x = item.Result;
                try
                {
                    if (x == null)
                    {
                    }
                    else if (x.Previous == null)
                    {
                        if (uniqueDocs.Add(x.Current.Id))
                            docsCount++;
                        if (uniqueRevisions.Add(x.Current.Name))
                            revisionsCount++;
                    }
                    else if (x.Current == null)
                    {
                    }
                    else
                    {
                        if (x.Current.Age > x.Previous.Age)
                        {
                            if (uniqueRevisions.Add(x.Current.Name))
                                revisionsCount++;
                        }
                    }

                    if (docsCount == nodesAmount && revisionsCount == Math.Pow(nodesAmount, 2))
                        reachedMaxDocCountMre.Set();
                }
                catch (Exception)
                {
                }
            }
        }

        private static async Task AddNodeToCluster(DocumentStore store, string url)
        {
            var addNodeRequest = await store.GetRequestExecutor().HttpClient.SendAsync(
                new HttpRequestMessage(HttpMethod.Put, $"{store.Urls[0]}/admin/cluster/node?url={url}").WithConventions(store.Conventions));
            Assert.True(addNodeRequest.IsSuccessStatusCode);
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

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(15);

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

        private static async Task<string> GetTagOfServerWhereSubscriptionWorks(IDocumentStore store, string database, string subscriptionName)
        {
            var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database));
            var subscriptionState = await store.Subscriptions.GetSubscriptionStateAsync(subscriptionName, database);
            return databaseRecord.Topology.WhoseTaskIsIt(RachisState.Follower, subscriptionState, null);
        }
    }
}
