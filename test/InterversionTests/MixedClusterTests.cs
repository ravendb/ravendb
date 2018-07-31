using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace InterversionTests
{
    public class MixedClusterTests : InterversionTestBase
    {
        public class ProcessNode
        {
            public string Version;
            public Process Process;
            public string Url;
        }

        public async Task<(RavenServer Leader, List<ProcessNode> Peers, List<RavenServer> LocalPeers)> CreateMixedCluster(string[] peers, int localPeers = 0)
        {
            var leaderServer = GetNewServer();
            leaderServer.ServerStore.Engine.Bootstrap(leaderServer.WebUrl);

            var nodeAdded = new ManualResetEvent(false);
            leaderServer.ServerStore.Engine.TopologyChanged += (sender, clusterTopology) =>
            {
                if(clusterTopology.Promotables.Count == 0)
                    nodeAdded.Set();
            };

            var local = new List<RavenServer>();
            for (int i = 0; i < localPeers; i++)
            {
                var peer = GetNewServer();
                await leaderServer.ServerStore.AddNodeToClusterAsync(peer.WebUrl);
                Assert.True(nodeAdded.WaitOne(TimeSpan.FromSeconds(30)));
                nodeAdded.Reset();
                local.Add(peer);
            }

            var processes = new List<ProcessNode>();
            foreach (var peer in peers)
            {
                var (url, process) = await GetServerAsync(peer);
                await leaderServer.ServerStore.AddNodeToClusterAsync(url);
                Assert.True(nodeAdded.WaitOne(TimeSpan.FromSeconds(30)));
                nodeAdded.Reset();
                processes.Add(new ProcessNode
                {
                    Version = peer,
                    Process = process,
                    Url = url
                });
            }

            return (leaderServer, processes, local);
        }

        public async Task<(IDisposable Disposable, List<DocumentStore> Stores)> GetStores(RavenServer leader, List<ProcessNode> peers, List<RavenServer> local = null)
        {
            var stores = new List<DocumentStore>();

            var leaderStore = GetDocumentStore(new Options
            {
                Server = leader,
                CreateDatabase = false,
                ModifyDocumentStore = s => s.Conventions.DisableTopologyUpdates = true
            });

            stores.Add(leaderStore);

            if (local != null)
                foreach (var ravenServer in local)
                {
                    var peerStore = GetDocumentStore(new Options
                    {
                        Server = ravenServer,
                        CreateDatabase = false,
                        ModifyDocumentStore = s => s.Conventions.DisableTopologyUpdates = true
                    });
                    stores.Add(peerStore);
                }

            foreach (var peer in peers)
            {
                var peerStore = await GetStore(peer.Url, peer.Process, null, new InterversionTestOptions
                {
                    CreateDatabase = false,
                    ModifyDocumentStore = s => s.Conventions.DisableTopologyUpdates = true
                });
                stores.Add(peerStore);
            }
            
            return (new DisposableAction(() =>
            {
                foreach (var documentStore in stores)
                {
                    documentStore.Dispose();
                }

                if (local != null)
                    foreach (var ravenServer in local)
                    {
                        ravenServer.Dispose();
                    }

                leaderStore.Dispose();
            }), stores);
        }

        [Fact]
        public async Task ReplicationInMixedCluster_40Leader_with_two_41_nodes()
        {
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.0.6-patch-40047",
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

                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(storeA.Database, true));
                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(dbName, true));
            }
        }

        [Fact]
        public async Task ReplicationInMixedCluster_40Leader_with_one_41_node_and_two_40_nodes()
        {
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.0.6-patch-40047",
                "4.0.6-patch-40047"
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

                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(storeA.Database, true));
                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(dbName, true));
            }
        }

        [Fact]
        public async Task ReplicationInMixedCluster_41Leader_with_406_patch40047()
        {
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.0.6-patch-40047",
                "4.0.6-patch-40047"
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

                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(storeA.Database, true));
                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(dbName, true));
            }
        }

        [Fact]
        public async Task ReplicationInMixedCluster_ShouldFail_41Leader_with_406_nightly20180727_1202()
        {
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.0.6-nightly-20180727-1202",
                "4.0.6-nightly-20180727-1202"
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

                Assert.False(await WaitForDocumentInClusterAsync<User>(
                    "users/1",
                    u => u.Name.Equals("aviv"),
                    TimeSpan.FromSeconds(10),
                    stores.Stores,
                    dbName));

                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(storeA.Database, true));
                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(dbName, true));
            }
        }

        [Fact]
        public async Task ReplicationInMixedCluster_41Leader_with_406_nightly20180730_1118()
        {
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.0.6-nightly-20180730-1118",
                "4.0.6-nightly-20180730-1118"
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

                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(storeA.Database, true));
                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(dbName, true));
            }
        }

        private static async Task<string> CreateDatabase(IDocumentStore store, int replicationFactor = 1, [CallerMemberName] string dbName = null)
        {
            var doc = new DatabaseRecord(dbName)
            {
                Settings =
                {
                    [RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat)] = "1",
                    [RavenConfiguration.GetKey(x => x.Replication.RetryReplicateAfter)] = "1",
                    [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = true.ToString(),
                    [RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = true.ToString(),
                    [RavenConfiguration.GetKey(x => x.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)] = int.MaxValue.ToString()
                }
            };

            var databasePutResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, replicationFactor));
            Assert.Equal(replicationFactor, databasePutResult.NodesAddedTo.Count);
            return dbName;
        }

        private static async Task AddNodeToCluster(DocumentStore store, string url)
        {
            var addNodeRequest = await store.GetRequestExecutor().HttpClient.SendAsync(
                new HttpRequestMessage(HttpMethod.Put, $"{store.Urls[0]}/admin/cluster/node?url={url}"));
            Assert.True(addNodeRequest.IsSuccessStatusCode);
        }

        private async Task<DocumentStore> GetStore(string serverUrl, Process serverProcess = null, [CallerMemberName] string database = null, InterversionTestOptions options = null)
        {
            options = options ?? InterversionTestOptions.Default;
            var name = database ?? GetDatabaseName(null);

            if (options.ModifyDatabaseName != null)
                name = options.ModifyDatabaseName(name) ?? name;

            var store = new DocumentStore
            {
                Urls = new[] { serverUrl },
                Database = name
            };

            options.ModifyDocumentStore?.Invoke(store);

            store.Initialize();

            if (options.CreateDatabase)
            {
                var dbs = await store.Maintenance.Server.SendAsync(new GetDatabaseNamesOperation(0, 10));
                foreach (var db in dbs)
                {
                    if (db == name)
                    {
                        throw new InvalidOperationException($"Database '{name}' already exists.");
                    }
                }

                var doc = new DatabaseRecord(name)
                {
                    Settings =
                    {
                        [RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat)] = "1",
                        [RavenConfiguration.GetKey(x => x.Replication.RetryReplicateAfter)] = "1",
                        [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = true.ToString(),
                        [RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = true.ToString(),
                        [RavenConfiguration.GetKey(x => x.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)] = int.MaxValue.ToString()
                    }
                };

                options.ModifyDatabaseRecord?.Invoke(doc);


                DatabasePutResult result;
                result = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, options.ReplicationFactor));
            }

            if (serverProcess != null)
            {
                store.AfterDispose += (sender, e) =>
                {
                    KillSlavedServerProcess(serverProcess);
                };
            }
            return store;
        }
    }
}
