using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Platform;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Infrastructure
{
    [Trait("Category", "Cluster")]
    public abstract partial class ClusterTestBase : RavenTestBase
    {
        static ClusterTestBase()
        {
            using (var currentProcess = Process.GetCurrentProcess())
                Console.WriteLine($"\tTo attach debugger to test process ({(PlatformDetails.Is32Bits ? "x86" : "x64")}), use proc-id: {currentProcess.Id}.");
        }

        protected ClusterTestBase(ITestOutputHelper output) : base(output)
        {
            ShardingCluster = new ShardingClusterTestBase(this);
        }

        private int _electionTimeoutInMs = 300;

        protected readonly ConcurrentBag<IDisposable> _toDispose = new ConcurrentBag<IDisposable>();

        private readonly Random _random = new Random();

        // workaround until RavenDB-16760 resolved
        protected DocumentStore GetDocumentStoreForRollingIndexes(Options options = null, [CallerMemberName] string caller = null)
        {
            Assert.NotNull(options?.Server);
            options.RunInMemory = false;

            return base.GetDocumentStore(options, caller);
        }

        protected void NoTimeouts()
        {
            foreach (var server in Servers)
            {
                server.ServerStore.Engine.Timeout.Disable = true;
            }
        }

        protected void SetTimeouts()
        {
            foreach (var server in Servers)
            {
                server.ServerStore.Engine.Timeout.Disable = false;
            }
        }

        protected static DatabasePutResult CreateClusterDatabase(string databaseName, IDocumentStore store, int replicationFactor = 2)
        {
            return store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(databaseName), replicationFactor));
        }

        protected static async Task<bool> WaitUntilDatabaseHasState(DocumentStore store, TimeSpan timeout, bool isLoaded)
        {
            var requestExecutor = store.GetRequestExecutor();
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var shouldContinue = true;
                var timeoutTask = Task.Delay(timeout);
                while (shouldContinue && timeoutTask.IsCompleted == false)
                {
                    try
                    {
                        var databaseIsLoadedCommand = new IsDatabaseLoadedCommand();
                        await requestExecutor.ExecuteAsync(databaseIsLoadedCommand, context);
                        shouldContinue = databaseIsLoadedCommand.Result.IsLoaded != isLoaded;
                        await Task.Delay(100);
                    }
                    catch (OperationCanceledException)
                    {
                        //OperationCanceledException is thrown if the database is currently shutting down
                    }
                }

                return timeoutTask.IsCompleted == false;
            }
        }

        protected void EnsureReplicating(DocumentStore src, DocumentStore dst, string id = null)
        {
            id ??= "marker/" + Guid.NewGuid();
            using (var s = src.OpenSession())
            {
                s.Store(new { }, id);
                s.SaveChanges();
            }
            Assert.NotNull(WaitForDocumentToReplicate<object>(dst, id, 15 * 1000));
        }

        public async Task EnsureReplicatingAsync(IDocumentStore src, IDocumentStore dst)
        {
            var sharding = await Sharding.GetShardingConfigurationAsync(src);
            if (sharding == null)
            {
                var id = "marker/" + Guid.NewGuid();
                using (var s = src.OpenSession())
                {
                    s.Store(new { }, id);
                    s.SaveChanges();
                }
                Assert.NotNull(await WaitForDocumentToReplicateAsync<object>(dst, id, 15 * 1000));
                return;
            }

            foreach (var shardNumber in sharding.Shards.Keys)
            {
                var database = ShardHelper.ToShardName(src.Database, shardNumber);
                var id = $"marker/{Guid.NewGuid()}${Sharding.GetRandomIdForShard(sharding, shardNumber)}";

                using (var s = src.OpenSession(database))
                {
                    s.Store(new { }, id);
                    s.SaveChanges();
                }

                var r = await Replication.WaitForDocumentToReplicateAsync<object>(dst, id, 30 * 1000);
                Assert.NotNull(r);
            }
        }

        protected static async Task<T> WaitForDocumentToReplicateAsync<T>(IDocumentStore store, string id, TimeSpan timeout)
            where T : class
        {
            var sw = Stopwatch.StartNew();
            while (sw.Elapsed <= timeout)
            {
                using (var session = store.OpenAsyncSession(store.Database))
                {
                    var doc = await session.LoadAsync<T>(id);
                    if (doc != null)
                        return doc;
                }

                await Task.Delay(100);
            }

            return null;
        }

        protected Task<T> WaitForDocumentToReplicateAsync<T>(IDocumentStore store, string id, int timeoutInMs)
            where T : class
        {
            return WaitForDocumentToReplicateAsync<T>(store, id, TimeSpan.FromMilliseconds(timeoutInMs));
        }

        protected T WaitForDocumentToReplicate<T>(IDocumentStore store, string id, int timeout)
            where T : class
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds <= timeout)
            {
                using (var session = store.OpenSession(store.Database))
                {
                    var doc = session.Load<T>(id);
                    if (doc != null)
                        return doc;
                }
                Thread.Sleep(100);
            }

            return null;
        }

        protected static bool WaitForDocumentDeletion(IDocumentStore store, string id, int timeout = 10000)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds <= timeout)
            {
                using (var session = store.OpenSession(store.Database))
                {
                    if (session.Advanced.Exists(id) == false)
                        return true;
                }
                Thread.Sleep(100);
            }

            return false;
        }

        public async Task RemoveDatabaseNode(List<RavenServer> cluster, string database, string toDeleteTag)
        {
            var deleted = cluster.Single(n => n.ServerStore.NodeTag == toDeleteTag);
            var nonDeleted = cluster.Where(n => n != deleted).ToArray();

            using var store = new DocumentStore
            {
                Database = database,
                Urls = new[] { nonDeleted[0].WebUrl },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize();

            var deleteResult = await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(database, hardDelete: true,
                fromNode: toDeleteTag, timeToWaitForConfirmation: TimeSpan.FromSeconds(15)));
            await Task.WhenAll(nonDeleted.Select(n =>
                n.ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, deleteResult.RaftCommandIndex)));

            Assert.True(await WaitForDatabaseToBeDeleted(store, database, TimeSpan.FromSeconds(15)), await Task.Run(async () =>
            {
                var sb = new StringBuilder($"database '{database}' was not deleted after 15 seconds");
                sb.AppendLine("debug logs : ");
                await GetClusterDebugLogsAsync(sb);
                return sb.ToString();
            }));

            await WaitAndAssertForValueAsync(async () =>
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database));
                return record.UnusedDatabaseIds.Count;
            }, expectedVal: 1, timeout: 10_000);
        }

        public static async Task<bool> WaitForDatabaseToBeDeleted(IDocumentStore store, string databaseName, TimeSpan timeout)
        {
            var pollingInterval = timeout.TotalSeconds < 1 ? timeout : TimeSpan.FromSeconds(1);
            var sw = Stopwatch.StartNew();
            while (true)
            {
                var delayTask = Task.Delay(pollingInterval);
                var dbTask = store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                var doneTask = await Task.WhenAny(dbTask, delayTask);
                if (doneTask == delayTask)
                {
                    if (sw.Elapsed > timeout)
                    {
                        return false;
                    }
                    continue;
                }
                var dbRecord = await dbTask;
                if (dbRecord == null || dbRecord.DeletionInProgress == null || dbRecord.DeletionInProgress.Count == 0)
                {
                    return true;
                }
            }
        }

        public Task EnsureNoReplicationLoop(RavenServer server, string database) => Replication.EnsureNoReplicationLoopAsync(server, database);

        private class GetDatabaseDocumentTestCommand : RavenCommand<DatabaseRecord>
        {
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/databases?name={node.Database}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get,
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationCluster.DatabaseRecord(response);
            }

            public override bool IsReadRequest => true;
        }

        protected static async Task<bool> WaitUntilDatabaseHasState(DocumentStore store, TimeSpan timeout, Func<DatabaseRecord, bool> predicate)
        {
            var requestExecutor = store.GetRequestExecutor();
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var shouldContinue = true;
                var timeoutTask = Task.Delay(timeout);
                while (shouldContinue && timeoutTask.IsCompleted == false)
                {
                    try
                    {
                        var databaseIsLoadedCommand = new GetDatabaseDocumentTestCommand();
                        await requestExecutor.ExecuteAsync(databaseIsLoadedCommand, context);
                        shouldContinue = predicate(databaseIsLoadedCommand.Result) == false;
                        await Task.Delay(100);
                    }
                    catch (OperationCanceledException)
                    {
                        //OperationCanceledException is thrown if the database is currently shutting down
                    }
                }

                return timeoutTask.IsCompleted == false;
            }
        }

        protected Task<RavenServer> ActionWithLeader(Action<RavenServer> act, List<RavenServer> servers = null)
        {
            return ActionWithLeader(l =>
            {
                act(l);
                return Task.CompletedTask;
            }, servers);
        }

        protected async Task<RavenServer> ActionWithLeader(Func<RavenServer, Task> act, List<RavenServer> servers = null)
        {
            var retries = 5;
            var exceptions = new List<Exception>();

            while (retries-- > 0)
            {
                Exception err = null;

                try
                {
                    servers ??= Servers;
                    var leader = servers.FirstOrDefault(s => s.ServerStore.IsLeader());
                    if (leader != null)
                    {
                        await act(leader);
                        return leader;
                    }
                }
                catch (RachisTopologyChangeException e)
                {
                    // The leader cannot remove itself, so we stepdown and try again to remove this node.
                    err = e;
                    var leader = Servers.FirstOrDefault(s => s.ServerStore.IsLeader());
                    leader?.ServerStore.Engine.CurrentLeader?.StepDown();
                }
                catch (Exception e)
                {
                    err = e;
                }

                if(err != null)
                    exceptions.Add(err);

                await Task.Delay(TimeSpan.FromSeconds(1));
            }

            throw new AggregateException($"Failed to get leader after 5 retries. {Environment.NewLine}{GetNodesStatus(servers ?? Servers)}", exceptions);
        }

        private string GetNodesStatus(List<RavenServer> servers)
        {
            var servers2 = servers.Select(s =>
            {
                var engine = s.ServerStore.Engine;
                return $"{s.ServerStore.NodeTag} in {engine.CurrentCommittedState.State} at term {engine.CurrentCommittedState.Term}";
            });
            return string.Join(Environment.NewLine, servers2);
        }

        protected async Task<T> WaitForValueOnGroupAsync<T>(DatabaseTopology topology, Func<ServerStore, Task<T>> func, T expected, int timeout = 15000)
        {
            var nodes = topology.AllNodes;
            var servers = new List<ServerStore>();
            var tasks = new Dictionary<string, Task<T>>();
            foreach (var node in nodes)
            {
                var server = Servers.Single(s => s.ServerStore.NodeTag == node);
                servers.Add(server.ServerStore);
            }
            foreach (var server in servers)
            {
                var task = WaitForValueAsync(() => func(server), expected, timeout);
                tasks.Add(server.NodeTag, task);
            }

            var res = await Task.WhenAll(tasks.Values);
            var hasExpectedVals = res.Where(t => t?.Equals(expected) ?? false);

            if (hasExpectedVals.Count() == servers.Count)
                return expected;

            var lookup = tasks.ToLookup(key => key.Value.Result, val => val.Key);

            var otherValues = "";
            foreach (var val in lookup)
            {
                otherValues += $"\n the value {val.Key} appears on ";
                foreach (string str in val)
                {
                    otherValues += str + ", ";
                }
            }
            throw new Exception($"Not all node in the group have the expected value of {expected}. {otherValues}");
        }

        protected bool WaitForChangeVectorInCluster(List<RavenServer> nodes, string database, int timeout = 15000)
        {
            return AsyncHelpers.RunSync(() => WaitForChangeVectorInClusterAsync(nodes, database, timeout));
        }

        protected async Task<bool> WaitForChangeVectorInClusterAsync(List<RavenServer> nodes, string database, int timeout = 15000)
        {
            return await WaitForValueAsync(async () =>
            {
                var cvs = new List<string>();
                foreach (var server in nodes)
                {
                    var storage = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                    using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        cvs.Add(DocumentsStorage.GetDatabaseChangeVector(context));
                    }
                }

                return cvs.Any(x => x != cvs.FirstOrDefault()) == false;
            }, true, timeout: timeout, interval: 333);
        }

        protected async Task<bool> WaitForChangeVectorInClusterForModeAsync(List<RavenServer> nodes, string database, RavenDatabaseMode mode, int replicationFactor = 3, int timeout = 15000)
        {
            if (mode == RavenDatabaseMode.Single)
                return await WaitForChangeVectorInClusterAsync(nodes, database, timeout);

            return await ShardingCluster.WaitForShardedChangeVectorInClusterAsync(nodes, database, replicationFactor, timeout);
        }

        protected async Task<bool> WaitForDocumentInClusterAsync<T>(DocumentSession session, string docId, Func<T, bool> predicate, TimeSpan timeout, X509Certificate2 certificate = null)
        {
            var nodes = session.RequestExecutor.TopologyNodes;
            var stores = GetDocumentStores(nodes, disableTopologyUpdates: true, certificate: certificate);
            return await WaitForDocumentInClusterAsyncInternal(docId, predicate, timeout, stores);
        }

        protected async Task<bool> WaitForDocumentInClusterAsync<T>(DatabaseTopology topology, string db, string docId, Func<T, bool> predicate, TimeSpan timeout, X509Certificate2 certificate = null)
        {
            var allNodes = topology.Members;
            var serversTopology = Servers.Where(s => allNodes.Contains(s.ServerStore.NodeTag));
            var nodes = serversTopology.Select(x => new ServerNode
            {
                Url = x.WebUrl,
                Database = db
            });
            var stores = GetDocumentStores(nodes, disableTopologyUpdates: true, certificate: certificate);
            return await WaitForDocumentInClusterAsyncInternal(docId, predicate, timeout, stores);
        }

        protected async Task<bool> WaitForDocumentInClusterAsync<T>(IReadOnlyList<ServerNode> topology, string docId, Func<T, bool> predicate, TimeSpan timeout)
        {
            var stores = GetDocumentStores(topology, disableTopologyUpdates: true);
            return await WaitForDocumentInClusterAsyncInternal(docId, predicate, timeout, stores);
        }

        protected async Task<bool> WaitForDocumentInClusterAsync<T>(List<RavenServer> nodes, string database, string docId, Func<T, bool> predicate, TimeSpan timeout, X509Certificate2 certificate = null)
        {
            var stores = GetDocumentStores(nodes, database, disableTopologyUpdates: true, certificate: certificate);
            return await WaitForDocumentInClusterAsyncInternal(docId, predicate, timeout, stores);
        }

        private async Task<bool> WaitForDocumentInClusterAsyncInternal<T>(string docId, Func<T, bool> predicate, TimeSpan timeout, List<DocumentStore> stores)
        {
            var tasks = new List<Task<bool>>();

            foreach (var store in stores)
                tasks.Add(Task.Run(() => WaitForDocument(store, docId, predicate, (int)timeout.TotalMilliseconds)));

            await Task.WhenAll(tasks);

            return tasks.All(x => x.Result);
        }

        private List<DocumentStore> GetDocumentStores(IEnumerable<ServerNode> nodes, bool disableTopologyUpdates, X509Certificate2 certificate = null)
        {
            var stores = new List<DocumentStore>();
            foreach (var node in nodes)
            {
                var store = new DocumentStore
                {
                    Urls = new[] { node.Url },
                    Database = node.Database,
                    Certificate = certificate,
                    Conventions =
                    {
                        DisableTopologyUpdates = disableTopologyUpdates,
                        DisposeCertificate = false
                    }
                };
                store.Initialize();
                stores.Add(store);
                _toDispose.Add(store);
            }

            return stores;
        }

        public List<DocumentStore> GetDocumentStores(List<RavenServer> nodes, string database, bool disableTopologyUpdates, X509Certificate2 certificate = null)
        {
            var stores = new List<DocumentStore>();
            foreach (var node in nodes)
            {
                var store = new DocumentStore
                {
                    Urls = new[] { node.WebUrl },
                    Database = database,
                    Certificate = certificate,
                    Conventions =
                    {
                        DisableTopologyUpdates = disableTopologyUpdates,
                        DisposeCertificate = false
                    }
                };
                store.Initialize();
                stores.Add(store);
                _toDispose.Add(store);
            }

            return stores;
        }

        protected bool WaitForDocument(IDocumentStore store,
            string docId,
            int timeout = 10000,
            string database = null)
        {
            return WaitForDocument<dynamic>(store, docId, predicate: null, timeout: timeout, database);
        }



        public async Task<T[]> AssertClusterWaitForNotNull<T>(
            List<RavenServer> nodes,
            string database,
            Func<IDocumentStore, Task<T>> act,
            int timeout = 15000,
            int interval = 100) where T : class
        {
            return await ClusterWaitFor(nodes, database, s => AssertWaitForNotNullAsync(() => act(s), timeout, interval));
        }
        public async Task<T[]> ClusterWaitForNotNull<T>(
            List<RavenServer> nodes,
            string database,
            Func<IDocumentStore, Task<T>> act,
            int timeout = 15000,
            int interval = 100) where T : class
        {
            return await ClusterWaitFor(nodes, database, s => WaitForNotNullAsync(() => act(s), timeout, interval));
        }

        public async Task<T[]> AssertClusterWaitForValue<T>(
            List<RavenServer> nodes,
            string database,
            Func<IDocumentStore, Task<T>> act,
            T expectedVal,
            int timeout = 15000,
            int interval = 100) where T : class
        {
            return await ClusterWaitFor(nodes, database, s => AssertWaitForValueAsync(() => act(s), expectedVal, timeout, interval));
        }

        public async Task<T[]> ClusterWaitFor<T>(
            List<RavenServer> nodes,
            string database,
            Func<IDocumentStore, Task<T>> waitFunc)
        {
            var stores = nodes.Select(n => new DocumentStore
            {
                Database = database,
                Urls = new[]
                {
                    n.WebUrl
                },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize()).ToArray();

            using (new DisposableAction(Action))
            {
                var tasks = stores.Select(waitFunc).ToArray();
                await Task.WhenAll(tasks);
                return tasks.Select(t => t.Result).ToArray();
            }

            void Action()
            {
                foreach (var store in stores)
                {
                    try
                    {
                        store.Dispose();
                    }
                    catch
                    {
                        // ignored
                    }
                }
            }
        }

        protected async Task<RavenServer> ReviveNodeAsync((string DataDirectory, string Url, string NodeTag) info, ServerCreationOptions options = default)
        {
            options ??= new ServerCreationOptions
            {
                RunInMemory = false,
                DeletePrevious = false,
                RegisterForDisposal = true,
                CustomSettings = DefaultClusterSettings
            };

            options.CustomSettings ??= new Dictionary<string, string>();

            options.DataDirectory = info.DataDirectory;
            options.NodeTag = info.NodeTag;
            options.CustomSettings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = info.Url;

            var server = GetNewServer(options);
            await server.ServerStore.InitializationCompleted.WaitAsync();
            return server;
        }

        protected static (string DataDirectory, string Url, string NodeTag) DisposeServerAndWaitForFinishOfDisposal(RavenServer serverToDispose)
        {
            var dataDirectory = serverToDispose.Configuration.Core.DataDirectory.FullPath;
            var url = serverToDispose.WebUrl;
            var nodeTag = serverToDispose.ServerStore.NodeTag;

            DisposeServer(serverToDispose);

            return (dataDirectory, url, nodeTag);
        }

        protected static async Task<(string DataDirectory, string Url, string NodeTag)> DisposeServerAndWaitForFinishOfDisposalAsync(RavenServer serverToDispose)
        {
            var dataDirectory = serverToDispose.Configuration.Core.DataDirectory.FullPath;
            var url = serverToDispose.WebUrl;
            var nodeTag = serverToDispose.ServerStore.NodeTag;

            await DisposeServerAsync(serverToDispose);

            return (dataDirectory, url, nodeTag);
        }

        protected async Task DisposeAndRemoveServer(RavenServer serverToDispose)
        {
            await DisposeServerAndWaitForFinishOfDisposalAsync(serverToDispose);
            Servers.Remove(serverToDispose);
        }

        protected Dictionary<string, string> DefaultClusterSettings = new Dictionary<string, string>
        {
            [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "1",
            [RavenConfiguration.GetKey(x => x.Cluster.AddReplicaTimeout)] = "1",
            [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
        };

        protected async Task<(List<RavenServer> Nodes, RavenServer Leader)> CreateRaftCluster(
            int numberOfNodes,
            bool? shouldRunInMemory = null,
            int? leaderIndex = null,
            IDictionary<string, string> customSettings = null,
            List<IDictionary<string, string>> customSettingsList = null,
            bool watcherCluster = false,
            bool useReservedPorts = false,
            [CallerMemberName] string caller = null)
        {
            var result = await CreateRaftClusterInternalAsync(numberOfNodes, shouldRunInMemory, leaderIndex, useSsl: false, customSettings, customSettingsList, watcherCluster, useReservedPorts, caller);
            return (result.Nodes, result.Leader);
        }

        protected Task<(List<RavenServer> Nodes, RavenServer Leader, TestCertificatesHolder Certificates)> CreateRaftClusterWithSsl(
            int numberOfNodes,
            bool shouldRunInMemory = true,
            int? leaderIndex = null,
            IDictionary<string, string> customSettings = null,
            List<IDictionary<string, string>> customSettingsList = null,
            bool watcherCluster = false,
            bool useReservedPorts = false)
        {
            return CreateRaftClusterInternalAsync(numberOfNodes, shouldRunInMemory, leaderIndex, useSsl: true, customSettings, customSettingsList, watcherCluster, useReservedPorts);
        }

        protected async Task<(RavenServer Leader, Dictionary<RavenServer, ProxyServer> Proxies)> CreateRaftClusterWithProxiesAsync(
            int numberOfNodes, bool shouldRunInMemory = true, int? leaderIndex = null, int delay = 0, [CallerMemberName] string caller = null)
        {
            var result = await CreateRaftClusterWithProxiesAndGetLeaderInternalAsync(numberOfNodes, shouldRunInMemory, leaderIndex, useSsl: false, delay, caller);
            return (result.Leader, result.Proxies);
        }

        protected async Task<(RavenServer Leader, Dictionary<RavenServer, ProxyServer> Proxies, TestCertificatesHolder Certificates)> CreateRaftClusterWithSslAndProxiesAsync(
            int numberOfNodes, bool shouldRunInMemory = true, int? leaderIndex = null, int delay = 0, [CallerMemberName] string caller = null)
        {
            var result = await CreateRaftClusterWithProxiesAndGetLeaderInternalAsync(numberOfNodes, shouldRunInMemory, leaderIndex, useSsl: true, delay, caller);
            return (result.Leader, result.Proxies, result.Certificates);
        }

        private async Task<(RavenServer Leader, Dictionary<RavenServer, ProxyServer> Proxies, TestCertificatesHolder Certificates)> CreateRaftClusterWithProxiesAndGetLeaderInternalAsync(int numberOfNodes, bool shouldRunInMemory = true, int? leaderIndex = null, bool useSsl = false, int delay = 0, [CallerMemberName] string caller = null)
        {
            leaderIndex ??= _random.Next(0, numberOfNodes);
            RavenServer leader = null;
            var serversToPorts = new Dictionary<RavenServer, string>();
            var serversToProxies = new Dictionary<RavenServer, ProxyServer>();

            var customSettings = GetServerSettingsForPort(useSsl, out var serverUrl, out var certificates);

            for (var i = 0; i < numberOfNodes; i++)
            {
                int proxyPort = 10000;
                var co = new ServerCreationOptions
                {
                    CustomSettings = customSettings,
                    RunInMemory = shouldRunInMemory,
                    RegisterForDisposal = false
                };
                var server = GetNewServer(co, caller);
                var proxy = new ProxyServer(ref proxyPort, Convert.ToInt32(server.ServerStore.GetNodeHttpServerUrl()), delay);
                serversToProxies.Add(server, proxy);

                if (Servers.Any(s => s.WebUrl.Equals(server.WebUrl, StringComparison.OrdinalIgnoreCase)) == false)
                {
                    Servers.Add(server);
                }

                serversToPorts.Add(server, serverUrl);
                if (i == leaderIndex)
                {
                    await server.ServerStore.EnsureNotPassiveAsync();
                    leader = server;
                }
            }
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            {
                for (var i = 0; i < numberOfNodes; i++)
                {
                    if (i == leaderIndex)
                    {
                        continue;
                    }
                    var follower = Servers[i];
                    // ReSharper disable once PossibleNullReferenceException
                    await leader.ServerStore.AddNodeToClusterAsync(serversToPorts[follower], token: cts.Token);
                    await follower.ServerStore.WaitForTopology(Leader.TopologyModification.Voter, cts.Token);
                }
            }
            // ReSharper disable once PossibleNullReferenceException
            var condition = await leader.ServerStore.WaitForState(RachisState.Leader, CancellationToken.None).WaitWithoutExceptionAsync(numberOfNodes * _electionTimeoutInMs * 5);
            var states = string.Empty;
            if (condition == false)
            {
                states = Cluster.GetLastStatesFromAllServersOrderedByTime();
            }
            Assert.True(condition, "The leader has changed while waiting for cluster to become stable. All nodes status: " + states);
            return (leader, serversToProxies, certificates);
        }

        protected async Task<(List<RavenServer> Nodes, RavenServer Leader, TestCertificatesHolder Certificates)> CreateRaftClusterInternalAsync(
            int numberOfNodes,
            bool? shouldRunInMemory = null,
            int? leaderIndex = null,
            bool useSsl = false,
            IDictionary<string, string> commonCustomSettings = null,
            List<IDictionary<string, string>> customSettingsList = null,
            bool watcherCluster = false,
            bool useReservedPorts = false,
            [CallerMemberName] string caller = null)
        {
            string[] allowedNodeTags = { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z" };
            var actualLeaderIndex = leaderIndex;
            leaderIndex ??= _random.Next(0, numberOfNodes);
            RavenServer leader = null;
            var serversToPorts = new Dictionary<RavenServer, string>();
            var clusterNodes = new List<RavenServer>(); // we need this in case we create more than 1 cluster in the same test

            _electionTimeoutInMs = Math.Max(300, numberOfNodes * 80);

            if (customSettingsList != null && customSettingsList.Count != numberOfNodes)
            {
                throw new InvalidOperationException("The number of custom settings must equal the number of nodes.");
            }

            TestCertificatesHolder certificates = null;

            for (var i = 0; i < numberOfNodes; i++)
            {
                IDictionary<string, string> customSettings;
                if (customSettingsList == null)
                {
                    customSettings = new Dictionary<string, string>(commonCustomSettings ?? DefaultClusterSettings);
                    
                    var electionKey = RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout);
                    if (customSettings.ContainsKey(electionKey) == false)
                    {
                        customSettings[electionKey] = _electionTimeoutInMs.ToString();
                    }
                }
                else
                {
                    customSettings = customSettingsList[i];
                }

                customSettings.TryGetValue(RavenConfiguration.GetKey(x => x.Core.ServerUrls), out string serverUrl);
                var port = 0;
                if (useReservedPorts)
                {
                    port = GetReservedPort();
                }

                if (useSsl)
                {
                    serverUrl ??= UseFiddlerUrl($"https://127.0.0.1:{port}");
                    if (customSettings.TryGetValue(RavenConfiguration.GetKey(x => x.Core.SetupMode), out var setupMode) == false || setupMode != nameof(SetupMode.LetsEncrypt))
                        certificates = Certificates.SetupServerAuthentication(customSettings, serverUrl);
                }
                else
                {
                    serverUrl ??= UseFiddlerUrl($"http://127.0.0.1:{port}");
                    customSettings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = serverUrl;
                }
                var co = new ServerCreationOptions
                {
                    CustomSettings = customSettings,
                    RunInMemory = shouldRunInMemory,
                    RegisterForDisposal = false,
                    NodeTag = allowedNodeTags[i]
                };
                var server = GetNewServer(co, caller);
                port = Convert.ToInt32(server.ServerStore.GetNodeHttpServerUrl().Split(':')[2]);
                var prefix = useSsl ? "https" : "http";
                var ip = serverUrl.Split(':')[1].Replace("//", "");
                serverUrl = UseFiddlerUrl($"{prefix}://{ip}:{port}");
                Servers.Add(server);
                clusterNodes.Add(server);

                serversToPorts.Add(server, serverUrl);
                if (i == leaderIndex)
                {
                    await server.ServerStore.EnsureNotPassiveAsync(null, nodeTag: co.NodeTag);
                    leader = server;
                }
            }

            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            {
                for (var i = 0; i < numberOfNodes; i++)
                {
                    if (i == leaderIndex)
                    {
                        continue;
                    }
                    var follower = clusterNodes[i];
                    // ReSharper disable once PossibleNullReferenceException
                    leader = await ActionWithLeader(l =>
                        l.ServerStore.AddNodeToClusterAsync(serversToPorts[follower], nodeTag: allowedNodeTags[i], asWatcher: watcherCluster, token: cts.Token), clusterNodes);

                    if (watcherCluster)
                    {
                        await follower.ServerStore.WaitForTopology(Leader.TopologyModification.NonVoter, cts.Token);
                    }
                    else
                    {
                        await follower.ServerStore.WaitForTopology(Leader.TopologyModification.Voter, cts.Token);
                    }

                    leader.ServerStore.Engine.GetLastCommitIndex(out var index, out _);
                    await follower.ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, index, cts.Token);
                }
            }

            await WaitForClusterTopologyOnAllNodes(clusterNodes);

            // ReSharper disable once PossibleNullReferenceException
            var condition = await leader.ServerStore.WaitForState(RachisState.Leader, CancellationToken.None).WaitWithoutExceptionAsync(numberOfNodes * _electionTimeoutInMs * 5);
            var states = "The leader has changed while waiting for cluster to become stable. All nodes status: ";
            if (condition == false)
            {
                InvalidOperationException e = null;
                if (actualLeaderIndex == null)
                {
                    // leader changed, try get the new leader if no leader index was selected
                    try
                    {
                        leader = await ActionWithLeader(_ => Task.CompletedTask, clusterNodes);
                        Assert.True(await WaitForNotHavingPromotables(clusterNodes));
                        return (clusterNodes, leader, certificates);
                    }
                    catch (InvalidOperationException ex)
                    {
                        e = ex;
                    }
                }
                states += Cluster.GetLastStatesFromAllServersOrderedByTime();
                if (e != null)
                    states += $"{Environment.NewLine}{e}";
            }
            Assert.True(condition, states);
            Assert.True(await WaitForNotHavingPromotables(clusterNodes));

            var votersCount = watcherCluster ? 0 : numberOfNodes - 1;
            if (votersCount > 0)
                await ActionWithLeader(async (leader1) =>
                {
                    var sw = Stopwatch.StartNew();
                    while (leader1.ServerStore.Engine.CurrentLeader.CurrentVoters.Count < votersCount)
                    {
                        await Task.Delay(100);
                        if (sw.ElapsedMilliseconds > 15_000)
                        {
                            throw new TimeoutException("waited too much to leader voters.");
                        }
                    }
                }, clusterNodes);

            return (clusterNodes, leader, certificates);
        }

        private async Task WaitForClusterTopologyOnAllNodes(List<RavenServer> clusterNodes)
        {
            foreach (var node in clusterNodes)
            {
                var nodesInTopology = await WaitForValueAsync(async () => await Task.FromResult(node.ServerStore.GetClusterTopology().AllNodes.Count), clusterNodes.Count, interval: 444);
                Assert.Equal(clusterNodes.Count, nodesInTopology);
            }
        }

        private static async Task<bool> WaitForNotHavingPromotables(List<RavenServer> clusterNodes, long timeout = 15_000)
        {
            // Waiting for not having Promotables and all nodes topologies will be updated

            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeout)
            {
                bool havePromotables = false;
                foreach (var server in clusterNodes)
                {
                    var t1 = server.ServerStore.GetClusterTopology();
                    if (t1.Promotables.Count > 0)
                    {
                        havePromotables = true;
                        break;
                    }
                }

                if (havePromotables == false)
                {
                    return true;
                }

                await Task.Delay(200);
            }

            return false;
        }

        protected Dictionary<string, string> GetServerSettingsForPort(bool useSsl, out string serverUrl, out TestCertificatesHolder certificates)
        {
            var customSettings = new Dictionary<string, string>();

            if (useSsl)
            {
                serverUrl = UseFiddlerUrl("https://127.0.0.1:0");
                certificates = Certificates.SetupServerAuthentication(customSettings, serverUrl);
            }
            else
            {
                serverUrl = UseFiddlerUrl("http://127.0.0.1:0");
                customSettings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = serverUrl;
                certificates = null;
            }

            return customSettings;
        }

        public async Task WaitForLeader(TimeSpan timeout)
        {
            using var cts = new CancellationTokenSource(timeout);
            var tasks = Servers
                .Select(server => server.ServerStore.WaitForState(RachisState.Leader, cts.Token))
                .ToList();

            var t = await Task.WhenAny(tasks);

            if (t.Result == false)
                throw new TimeoutException(Cluster.GetLastStatesFromAllServersOrderedByTime());
        }

        public async Task<(long Index, List<RavenServer> Servers)> CreateDatabaseInCluster(DatabaseRecord record, int replicationFactor, string leadersUrl, X509Certificate2 certificate = null)
        {
            var tuple = await CreateDatabaseInClusterInner(record, replicationFactor, leadersUrl, certificate);
            return (tuple.Result.RaftCommandIndex, tuple.Servers);
        }

        public async Task<(DatabasePutResult Result, List<RavenServer> Servers)> CreateDatabaseInClusterInner(DatabaseRecord record, int replicationFactor, string leadersUrl, X509Certificate2 certificate)
        {
            var serverCount = Servers.Count(s => s.Disposed == false);
            if (serverCount < replicationFactor)
            {
                throw new InvalidOperationException($"Cannot create database with replication factor = {replicationFactor} when there is only {serverCount} servers in the cluster.");
            }

            DatabasePutResult databaseResult;
            string[] urls;
            using (var store = new DocumentStore()
            {
                Urls = new[] { leadersUrl },
                Database = record.DatabaseName,
                Certificate = certificate,
                Conventions =
                {
                    DisposeCertificate = false
                }
            }.Initialize())
            {
                try
                {
                    databaseResult = store.Maintenance.Server.Send(new CreateDatabaseOperation(record, replicationFactor));
                }
                catch (ConcurrencyException inner)
                {
                    //catch debug logs
                    var sb = new StringBuilder();
                    await GetClusterDebugLogsAsync(sb);
                    throw new ConcurrencyException($"Couldn't create database on time, cluster debug logs: {sb}", inner);
                }
                urls = await GetClusterNodeUrlsAsync(leadersUrl, store);
            }

            var firstUrlNode = databaseResult.NodesAddedTo.First();
            var currentCluster = Servers.Where(s => s.Disposed == false && s.ServerStore.GetClusterTopology().TryGetNodeTagByUrl(firstUrlNode).HasUrl).ToArray();

            int numberOfInstances = 0;
            foreach (var server in currentCluster)
            {
                await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.RaftCommandIndex);
            }

            var relevantServers = currentCluster.Where(s => databaseResult.Topology.RelevantFor(s.ServerStore.NodeTag)).ToArray();
            foreach (var server in relevantServers)
            {
                var result = server.ServerStore.DatabasesLandlord.TryGetOrCreateDatabase(record.DatabaseName);
                if (result.DatabaseStatus != DatabasesLandlord.DatabaseSearchResult.Status.Missing)
                    numberOfInstances++;
            }

            if (numberOfInstances != replicationFactor)
            {
                throw new InvalidOperationException($"Couldn't create the db on all nodes, just on {numberOfInstances} out of {replicationFactor}{Environment.NewLine}" +
                                                    $"Server urls are {string.Join(",", Servers.Select(x => $"[{x.WebUrl}|{x.Disposed}]"))};{Environment.NewLine}" +
                                                    $"Current cluster (members) urls are : {string.Join(",", urls)};{Environment.NewLine}" +
                                                    $"The relevant servers are : {string.Join(",", relevantServers.Select(x => x.WebUrl))};{Environment.NewLine}" +
                                                    $"current servers are: {string.Join(",", currentCluster.Select(x => x.WebUrl))}");
            }

            return (databaseResult, relevantServers.ToList());
        }

        private static async Task<string[]> GetClusterNodeUrlsAsync(string leadersUrl, IDocumentStore store)
        {
            string[] urls;
            using (var requestExecutor = ClusterRequestExecutor.CreateForShortTermUse(leadersUrl, store.Certificate, DocumentConventions.DefaultForServer))
            {
                try
                {
                    await requestExecutor.UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(new ServerNode
                    {
                        Url = leadersUrl
                    })
                    {
                        TimeoutInMs = 15000,
                        ForceUpdate = true
                    });
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }

                urls = requestExecutor.Topology.Nodes.Select(x => x.Url).ToArray();
            }

            return urls;
        }

        public Task<(long Index, List<RavenServer> Servers)> CreateDatabaseInCluster(string databaseName, int replicationFactor, string leadersUrl, X509Certificate2 certificate = null)
        {
            return CreateDatabaseInCluster(new DatabaseRecord(databaseName), replicationFactor, leadersUrl, certificate);
        }

        public Task<(long Index, List<RavenServer> Servers)> CreateDatabaseInClusterForMode(string databaseName, int replicationFactor, (List<RavenServer> Nodes, RavenServer Leader) tuple, RavenDatabaseMode mode, int shards = 3, X509Certificate2 certificate = null)
        {
            if (mode == RavenDatabaseMode.Single)
                return CreateDatabaseInCluster(databaseName, replicationFactor, tuple.Leader.WebUrl, certificate);

            return ShardingCluster.CreateShardedDatabaseInCluster(databaseName, replicationFactor, tuple, shards, certificate);
        }

        public void WaitForIndexingInTheCluster(IDocumentStore store, string dbName = null, TimeSpan? timeout = null, bool allowErrors = false)
        {
            var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(dbName ?? store.Database));
            foreach (var nodeTag in record.Topology.AllNodes)
            {
                Indexes.WaitForIndexing(store, dbName, timeout, allowErrors, nodeTag);
            }
        }

        public async Task StoreInTransactionMode(IDocumentStore store, int n)
        {
            for (int i = 0; i < n; i++)
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    await session.StoreAsync(new User() { Name = "userT" + i }, "users/1");
                    await session.SaveChangesAsync();
                }
            }
        }

        public async Task DeleteInTransactionMode(IDocumentStore store, int n)
        {
            for (int i = 0; i < n; i++)
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Delete("users/1");
                    await session.SaveChangesAsync();
                }
            }

        }
        public async Task DeleteAndStoreInTransactionMode(IDocumentStore store, int n)
        {
            for (int i = 0; i < n; i++)
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Delete("users/1");
                    await session.StoreAsync(new User() { Name = "userT" + i }, "users/2");
                    await session.SaveChangesAsync();
                }
            }

        }
        public async Task StoreInRegularMode(IDocumentStore store, int n)
        {
            for (int i = 0; i < n; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User() { Name = "userR" + i }, "users/1");
                    await session.SaveChangesAsync();
                }
            }

        }

        public async Task DeleteInRegularMode(IDocumentStore store, int n)
        {
            for (int i = 0; i < n; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.Delete("users/1");
                    await session.SaveChangesAsync();
                }
            }

        }

        internal async Task GetClusterDebugLogsAsync(StringBuilder sb)
        {
            NodeDebugInfo debugInfo = null;
            await ActionWithLeader((l) =>
            {
                debugInfo = GetDebugInfoForNode(l);
                return Task.CompletedTask;
            });

            AppendDebugInfo(sb, debugInfo);
        }

        internal static void GetDebugLogsForNode(RavenServer node, StringBuilder sb) => AppendDebugInfo(sb, GetDebugInfoForNode(node));

        private static NodeDebugInfo GetDebugInfoForNode(RavenServer node)
        {
            var debugInfo = new NodeDebugInfo
            {
                ClusterObserverLogs = node.ServerStore.Observer?.ReadDecisionsForDatabase().List, 
                PrevStates = node.ServerStore.Engine.PrevStates.Select(s => s.ToString()).ToList()
            };

            using (node.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                debugInfo.HistoryLogs = node.ServerStore.Engine.LogHistory.GetHistoryLogs(context).ToList();
                debugInfo.InMemoryDebug = node.ServerStore.Engine.InMemoryDebug.ToJson();
            }

            return debugInfo;
        }

        private static void AppendDebugInfo(StringBuilder sb, NodeDebugInfo debugInfo)
        {
            if (debugInfo.PrevStates != null)
            {
                sb.AppendLine($"{Environment.NewLine}States:{Environment.NewLine}-----------------------");
                foreach (var state in debugInfo.PrevStates)
                {
                    sb.AppendLine($"{state}{Environment.NewLine}");
                }
                sb.AppendLine();
            }

            if (debugInfo.HistoryLogs != null)
            {
                sb.AppendLine($"HistoryLogs:{Environment.NewLine}-----------------------");
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var c = 0;
                    foreach (var log in debugInfo.HistoryLogs)
                    {
                        var json = context.ReadObject(log, nameof(log) + $"{c++}");
                        sb.AppendLine(json.ToString());
                    }
                }
                sb.AppendLine();
            }

            if (debugInfo.ClusterObserverLogs?.Length > 0)
            {
                sb.AppendLine($"Cluster Observer Log Entries:{Environment.NewLine}-----------------------");
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var c = 0;
                    foreach (var log in debugInfo.ClusterObserverLogs)
                    {
                        var json = context.ReadObject(log.ToJson(), nameof(log) + $"{c++}");
                        sb.AppendLine(json.ToString());
                    }
                }
            }

            if (debugInfo.InMemoryDebug != null)
            {
                sb.AppendLine($"RachisDebug:{Environment.NewLine}-----------------------");
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var json = context.ReadObject(debugInfo.InMemoryDebug, nameof(NodeDebugInfo.InMemoryDebug));
                    sb.AppendLine(json.ToString());
                }
            }
        }

        private class NodeDebugInfo
        {
            public ClusterObserverLogEntry[] ClusterObserverLogs;

            public List<DynamicJsonValue> HistoryLogs;

            public DynamicJsonValue InMemoryDebug;

            public List<string> PrevStates;
        }


        public override void Dispose()
        {
            foreach (var disposable in _toDispose)
                disposable.Dispose();

            foreach (var server in Servers)
            {
                if (IsGlobalServer(server))
                    continue; // must not dispose the global server

                if (ServersForDisposal.Contains(server) == false)
                    ServersForDisposal.Add(server);
            }

            base.Dispose();
        }
    }
}
