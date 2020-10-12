using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Commands.Cluster;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace InterversionTests
{
    public abstract class MixedClusterTestBase : InterversionTestBase
    {
        protected MixedClusterTestBase(ITestOutputHelper output) : base(output)
        {
        }

        protected override RavenServer GetNewServer(ServerCreationOptions options = null, [CallerMemberName]string caller = null)
        {
            if (options == null)
            {
                options = new ServerCreationOptions();
            }

            if (options.CustomSettings == null)
                options.CustomSettings = new Dictionary<string, string>();
            var key = RavenConfiguration.GetKey(x => x.Http.UseLibuv);
            if (options.CustomSettings.ContainsKey(key) == false)
                options.CustomSettings[key] = "true";
            return base.GetNewServer(options, caller);
        }

        protected async Task<List<ProcessNode>> CreateCluster(string[] peers, IDictionary<string, string> customSettings = null, X509Certificate2 certificate = null)
        {
            var processes = new List<ProcessNode>();
            foreach (var peer in peers)
            {
                processes.Add(await GetServerAsync(peer));
            }

            var chosenOne = processes[0];

            using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(chosenOne.Url, certificate))
            using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                foreach (var processNode in processes)
                {
                    if (processNode == chosenOne)
                        continue;

                    var addCommand = new AddClusterNodeCommand(processNode.Url);
                    await requestExecutor.ExecuteAsync(addCommand, context);
                }

                var clusterCreated = await WaitForValueAsync(async () =>
                {
                    var clusterTopology = new GetClusterTopologyCommand();
                    await requestExecutor.ExecuteAsync(clusterTopology, context);
                    return clusterTopology.Result.Topology.Members.Count;
                }, peers.Length);

                Assert.True(clusterCreated == peers.Length, "Failed to create initial cluster");
            }

            return processes;
        }

        protected async Task<(RavenServer Leader, List<ProcessNode> Peers, List<RavenServer> LocalPeers)> CreateMixedCluster(
            string[] peers, int localPeers = 0, IDictionary<string, string> customSettings = null, X509Certificate2 certificate = null)
        {
            var leaderServer = GetNewServer(new ServerCreationOptions { CustomSettings = customSettings });
            await leaderServer.ServerStore.EnsureNotPassiveAsync(leaderServer.WebUrl);

            var nodeAdded = new ManualResetEvent(false);
            var expectedMembers = 2;
            leaderServer.ServerStore.Engine.TopologyChanged += (sender, clusterTopology) =>
            {
                var count = expectedMembers;
                if (clusterTopology.Promotables.Count == 0 && clusterTopology.Members.Count == count)
                {
                    var result = Interlocked.CompareExchange(ref expectedMembers, count + 1, count);
                    if (result == count)
                    {
                        nodeAdded.Set();
                    }
                }
            };

            using (leaderServer.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(leaderServer.WebUrl, certificate))
            {
                var local = new List<RavenServer>();

                for (int i = 0; i < localPeers; i++)
                {
                    var peer = GetNewServer(new ServerCreationOptions { CustomSettings = customSettings });
                    var addCommand = new AddClusterNodeCommand(peer.WebUrl);
                    await requestExecutor.ExecuteAsync(addCommand, context);
                    Assert.True(nodeAdded.WaitOne(TimeSpan.FromSeconds(30)));
                    nodeAdded.Reset();
                    local.Add(peer);
                }

                var processes = new List<ProcessNode>();
                foreach (var peer in peers)
                {
                    processes.Add(await GetServerAsync(peer));
                }

                foreach (var processNode in processes)
                {
                    var addCommand = new AddClusterNodeCommand(processNode.Url);
                    await requestExecutor.ExecuteAsync(addCommand, context);
                    Assert.True(nodeAdded.WaitOne(TimeSpan.FromSeconds(30)));
                    nodeAdded.Reset();
                }

                Assert.Equal(peers.Length + localPeers + 1, leaderServer.ServerStore.GetClusterTopology().Members.Count);
                return (leaderServer, processes, local);
            }
        }

        protected async Task<(IDisposable Disposable, List<DocumentStore> Stores)> GetStores(string database, List<ProcessNode> peers,
            Action<DocumentStore> modifyDocumentStore = null)
        {
            if (modifyDocumentStore == null)
            {
                modifyDocumentStore = s => s.Conventions.DisableTopologyUpdates = true;
            }

            var stores = new List<DocumentStore>();

            foreach (var peer in peers)
            {
                var peerStore = await GetStore(peer.Url, peer.Process, database, new InterversionTestOptions
                {
                    CreateDatabase = false,
                    ModifyDocumentStore = modifyDocumentStore,
                });
                stores.Add(peerStore);
            }

            return (new DisposableAction(() =>
            {
                foreach (var documentStore in stores)
                {
                    documentStore.Dispose();
                }
            }), stores);
        }

        protected async Task<(IDisposable Disposable, List<DocumentStore> Stores)> GetStores(RavenServer leader, List<ProcessNode> peers,
            List<RavenServer> local = null, Action<DocumentStore> modifyDocumentStore = null)
        {
            if (modifyDocumentStore == null)
            {
                modifyDocumentStore = s => s.Conventions.DisableTopologyUpdates = true;
            }

            var stores = new List<DocumentStore>();

            var leaderStore = GetDocumentStore(new Options
            {
                Server = leader,
                CreateDatabase = false,
                ModifyDocumentStore = modifyDocumentStore
            });

            stores.Add(leaderStore);

            if (local != null)
                foreach (var ravenServer in local)
                {
                    var peerStore = GetDocumentStore(new Options
                    {
                        Server = ravenServer,
                        CreateDatabase = false,
                        ModifyDocumentStore = modifyDocumentStore
                    });
                    stores.Add(peerStore);
                }

            foreach (var peer in peers)
            {
                var peerStore = await GetStore(peer.Url, peer.Process, null, new InterversionTestOptions
                {
                    CreateDatabase = false,
                    ModifyDocumentStore = modifyDocumentStore
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

        protected static async Task<string> CreateDatabase(IDocumentStore store, int replicationFactor = 1, [CallerMemberName] string dbName = null)
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

        protected async Task<DocumentStore> GetStore(string serverUrl, Process serverProcess = null, [CallerMemberName] string database = null, InterversionTestOptions options = null)
        {
            options = options ?? InterversionTestOptions.Default;
            var name = database ?? GetDatabaseName();

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

                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, options.ReplicationFactor));
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
