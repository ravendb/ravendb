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
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
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
    public abstract class ClusterTestBase : RavenTestBase
    {
        static ClusterTestBase()
        {
            using (var currentProcess = Process.GetCurrentProcess())
                Console.WriteLine($"\tTo attach debugger to test process ({(PlatformDetails.Is32Bits ? "x86" : "x64")}), use proc-id: {currentProcess.Id}.");
        }

        protected ClusterTestBase(ITestOutputHelper output) : base(output)
        {
        }

        private int _electionTimeoutInMs = 300;

        protected readonly ConcurrentBag<IDisposable> _toDispose = new ConcurrentBag<IDisposable>();

        private readonly Random _random = new Random();

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

        protected async Task<bool> WaitUntilDatabaseHasState(DocumentStore store, TimeSpan timeout, bool isLoaded)
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

        protected void EnsureReplicating(DocumentStore src, DocumentStore dst)
        {
            var id = "marker/" + Guid.NewGuid();
            using (var s = src.OpenSession())
            {
                s.Store(new { }, id);
                s.SaveChanges();
            }
            Assert.NotNull(WaitForDocumentToReplicate<object>(dst, id, 15 * 1000));
        }

        protected async Task EnsureReplicatingAsync(DocumentStore src, DocumentStore dst)
        {
            var id = "marker/" + Guid.NewGuid();
            using (var s = src.OpenSession())
            {
                s.Store(new { }, id);
                s.SaveChanges();
            }
            Assert.NotNull(await WaitForDocumentToReplicateAsync<object>(dst, id, 15 * 1000));
        }

        protected async Task<T> WaitForDocumentToReplicateAsync<T>(IDocumentStore store, string id, int timeout)
            where T : class
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds <= timeout)
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

        protected bool WaitForDocumentDeletion(IDocumentStore store, string id, int timeout = 10000)
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
                n.ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, deleteResult.RaftCommandIndex + 1)));
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database));
            Assert.Equal(1, record.UnusedDatabaseIds.Count);
        }

        public async Task EnsureNoReplicationLoop(RavenServer server, string database)
        {
            var storage = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
            using (var collector = new LiveReplicationPulsesCollector(storage))
            {
            var etag1 = storage.DocumentsStorage.GenerateNextEtag();

            await Task.Delay(3000);

            var etag2 = storage.DocumentsStorage.GenerateNextEtag();

            Assert.True(etag1 + 1 == etag2, "Replication loop found :(");

                var groups = collector.Pulses.GetAll().GroupBy(p => p.Direction);
                foreach (var group in groups)
                {
                    var key = group.Key;
                    var count = group.Count();
                    Assert.True(count < 50,$"{key} seems to be excessive ({count})");
        }
            }
        }

        public class GetDatabaseDocumentTestCommand : RavenCommand<DatabaseRecord>
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

        protected async Task<bool> WaitUntilDatabaseHasState(DocumentStore store, TimeSpan timeout, Func<DatabaseRecord, bool> predicate)
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

        protected async Task<RavenServer> ActionWithLeader(Func<RavenServer, Task> act, List<RavenServer> servers = null)
        {
            var retries = 5;
            Exception err = null;
            while (retries-- > 0)
            {
                try
                {
                    var leader = servers == null ? Servers.FirstOrDefault(s => s.ServerStore.IsLeader()) : servers.FirstOrDefault(s => s.ServerStore.IsLeader());
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
                catch (Exception e) when (e is NotLeadingException || e is ObjectDisposedException)
                {
                    err = e;
                }

                await Task.Delay(TimeSpan.FromSeconds(1));
            }

            throw new InvalidOperationException($"Failed to get leader after 5 retries. {Environment.NewLine}{GetNodesStatus()}", err);
        }

        private string GetNodesStatus()
        {
            var servers = Servers.Select(s =>
            {
                var engine = s.ServerStore.Engine;
                return $"{s.ServerStore.NodeTag} in {engine.CurrentState} at term {engine.CurrentTerm}";
            });
            return string.Join(Environment.NewLine, servers);
        }

        protected async Task<T> WaitForValueOnGroupAsync<T>(DatabaseTopology topology, Func<ServerStore, T> func, T expected, int timeout = 15000)
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

        protected async Task<bool> WaitForDocumentInClusterAsync<T>(List<RavenServer> nodes, string database, string docId, Func<T, bool> predicate, TimeSpan timeout, X509Certificate2 certificate = null)
        {
            var stores = GetDocumentStores(nodes, database, disableTopologyUpdates: true, certificate: certificate);
            return await WaitForDocumentInClusterAsyncInternal(docId, predicate, timeout, stores);
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
                        DisableTopologyUpdates = disableTopologyUpdates
                    }
                };
                store.Initialize();
                stores.Add(store);
                _toDispose.Add(store);
            }

            return stores;
        }

        private List<DocumentStore> GetDocumentStores(List<RavenServer> nodes, string database, bool disableTopologyUpdates, X509Certificate2 certificate = null)
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
                        DisableTopologyUpdates = disableTopologyUpdates
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

        protected bool WaitForDocument<T>(IDocumentStore store,
            string docId,
            Func<T, bool> predicate,
            int timeout = 10000,
            string database = null)
        {
            if (DebuggerAttachedTimeout.DisableLongTimespan == false &&
                Debugger.IsAttached)
                timeout *= 100;

            var sw = Stopwatch.StartNew();
            Exception ex = null;
            while (sw.ElapsedMilliseconds < timeout)
            {
                using (var session = store.OpenSession(database ?? store.Database))
                {
                    try
                    {
                        var doc = session.Load<T>(docId);
                        if (doc != null)
                        {
                            if (predicate == null || predicate(doc))
                                return true;
                        }
                    }
                    catch (Exception e)
                    {
                        ex = e;
                        // expected that we might get conflict, ignore and wait
                    }
                }

                Thread.Sleep(100);
            }

            using (var session = store.OpenSession(database ?? store.Database))
            {
                //one last try, and throw if there is still a conflict
                var doc = session.Load<T>(docId);
                if (doc != null)
                {
                    if (predicate == null || predicate(doc))
                        return true;
                }
            }
            if (ex != null)
            {
                throw ex;
            }
            return false;
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
            [CallerMemberName] string caller = null)
        {
            var result = await CreateRaftClusterInternalAsync(numberOfNodes, shouldRunInMemory, leaderIndex, useSsl: false, customSettings, customSettingsList, watcherCluster, caller);
            return (result.Nodes, result.Leader);
        }

        protected Task<(List<RavenServer> Nodes, RavenServer Leader, TestCertificatesHolder Certificates)> CreateRaftClusterWithSsl(
            int numberOfNodes,
            bool shouldRunInMemory = true,
            int? leaderIndex = null,
            IDictionary<string, string> customSettings = null,
            List<IDictionary<string, string>> customSettingsList = null,
            bool watcherCluster = false)
        {
            return CreateRaftClusterInternalAsync(numberOfNodes, shouldRunInMemory, leaderIndex, useSsl: true, customSettings, customSettingsList, watcherCluster);
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
            var condition = await leader.ServerStore.WaitForState(RachisState.Leader, CancellationToken.None).WaitAsync(numberOfNodes * _electionTimeoutInMs * 5);
            var states = string.Empty;
            if (condition == false)
            {
                states = GetLastStatesFromAllServersOrderedByTime();
            }
            Assert.True(condition, "The leader has changed while waiting for cluster to become stable. All nodes status: " + states);
            return (leader, serversToProxies, certificates);
        }

        protected async Task<(List<RavenServer> Nodes, RavenServer Leader, TestCertificatesHolder Certificates)> CreateRaftClusterInternalAsync(
            int numberOfNodes,
            bool? shouldRunInMemory = null,
            int? leaderIndex = null,
            bool useSsl = false,
            IDictionary<string, string> customSettings = null,
            List<IDictionary<string, string>> customSettingsList = null,
            bool watcherCluster = false,
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
                if (customSettingsList == null)
                {
                    customSettings ??= new Dictionary<string, string>(DefaultClusterSettings)
                    {
                        [RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = _electionTimeoutInMs.ToString(),
                    };
                }
                else
                {
                    customSettings = customSettingsList[i];
                }

                string serverUrl;

                if (useSsl)
                {
                    serverUrl = UseFiddlerUrl("https://127.0.0.1:0");
                    certificates = SetupServerAuthentication(customSettings, serverUrl);
                }
                else
                {
                    serverUrl = UseFiddlerUrl("http://127.0.0.1:0");
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
                var port = Convert.ToInt32(server.ServerStore.GetNodeHttpServerUrl().Split(':')[2]);
                var prefix = useSsl ? "https" : "http";
                serverUrl = UseFiddlerUrl($"{prefix}://127.0.0.1:{port}");
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
                }
            }

            await WaitForClusterTopologyOnAllNodes(clusterNodes);

            // ReSharper disable once PossibleNullReferenceException
            var condition = await leader.ServerStore.WaitForState(RachisState.Leader, CancellationToken.None).WaitAsync(numberOfNodes * _electionTimeoutInMs * 5);
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
                        return (clusterNodes, leader, certificates);
                    }
                    catch (InvalidOperationException ex)
                    {
                        e = ex;
                    }
                }
                states += GetLastStatesFromAllServersOrderedByTime();
                if (e != null)
                    states += $"{Environment.NewLine}{e}";
            }
            Assert.True(condition, states);
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

        protected Dictionary<string, string> GetServerSettingsForPort(bool useSsl, out string serverUrl, out TestCertificatesHolder certificates)
        {
            var customSettings = new Dictionary<string, string>();

            if (useSsl)
            {
                serverUrl = UseFiddlerUrl("https://127.0.0.1:0");
                certificates = SetupServerAuthentication(customSettings, serverUrl);
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
            var tasks = Servers
                .Select(server => server.ServerStore.WaitForState(RachisState.Leader, CancellationToken.None))
                .ToList();

            tasks.Add(Task.Delay(timeout));
            await Task.WhenAny(tasks);

            if (Task.Delay(timeout).IsCompleted)
                throw new TimeoutException(GetLastStatesFromAllServersOrderedByTime());
        }

        protected override Task<DocumentDatabase> GetDocumentDatabaseInstanceFor(IDocumentStore store, string database = null)
        {
            //var index = FindStoreIndex(store);
            //Assert.False(index == -1, "Didn't find store index, most likely it doesn't belong to the cluster. Did you setup Raft cluster properly?");
            //return Servers[index].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            return Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database ?? store.Database);
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
                Certificate = certificate
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
                    await GetClusterDebugLogs(sb);
                    throw new ConcurrencyException($"Couldn't create database on time, cluster debug logs: {sb}", inner);
                }
                urls = await GetClusterNodeUrlsAsync(leadersUrl, store);
            }

            var currentServers = Servers.Where(s => s.Disposed == false &&
                                                    databaseResult.NodesAddedTo.Contains(s.WebUrl, StringComparer.CurrentCultureIgnoreCase)).ToArray();
            int numberOfInstances = 0;
            foreach (var server in currentServers)
            {
                await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.RaftCommandIndex);
            }

            var relevantServers = currentServers.Where(s => databaseResult.Topology.RelevantFor(s.ServerStore.NodeTag)).ToArray();
            foreach (var server in relevantServers)
            {
                await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(record.DatabaseName);
                numberOfInstances++;
            }

            if (numberOfInstances != replicationFactor)
                throw new InvalidOperationException($@"Couldn't create the db on all nodes, just on {numberOfInstances}
                                                    out of {replicationFactor}{Environment.NewLine}
                                                    Server urls are {string.Join(",", Servers.Select(x => $"[{x.WebUrl}|{x.Disposed}]"))}; Current cluster (members) urls are : {string.Join(",", urls)}; The relevant servers are : {string.Join(",", relevantServers.Select(x => x.WebUrl))}; current servers are : {string.Join(",", currentServers.Select(x => x.WebUrl))}");
            return (databaseResult, relevantServers.ToList());
        }

        private static async Task<string[]> GetClusterNodeUrlsAsync(string leadersUrl, IDocumentStore store)
        {
            string[] urls;
            using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(leadersUrl, store.Certificate))
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

        public static void WaitForIndexingInTheCluster(IDocumentStore store, string dbName = null, TimeSpan? timeout = null, bool allowErrors = false)
        {
            var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(dbName ?? store.Database));
            foreach (var nodeTag in record.Topology.AllNodes)
            {
                WaitForIndexing(store, dbName, timeout, allowErrors, nodeTag);
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

        internal async Task GetClusterDebugLogs(StringBuilder sb)
        {
            (ClusterObserverLogEntry[] List, long Iteration) logs;
            List<DynamicJsonValue> historyLogs = null;
            DynamicJsonValue inMemoryDebug = null;
            List<string> prevStates = null;
            logs.List = null;
            await ActionWithLeader((l) =>
            {
                logs = l.ServerStore.Observer.ReadDecisionsForDatabase();
                prevStates = l.ServerStore.Engine.PrevStates.Select(s => s.ToString()).ToList();

                using (l.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    historyLogs = l.ServerStore.Engine.LogHistory.GetHistoryLogs(context).ToList();
                    inMemoryDebug = l.ServerStore.Engine.InMemoryDebug.ToJson();
                }

                return Task.CompletedTask;
            });

            if (prevStates != null)
            {
                sb.AppendLine($"{Environment.NewLine}States:{Environment.NewLine}-----------------------");
                foreach (var state in prevStates)
                {
                    sb.AppendLine($"{state}{Environment.NewLine}");
                }
                sb.AppendLine();
            }

            if (historyLogs != null)
            {
                sb.AppendLine($"HistoryLogs:{Environment.NewLine}-----------------------");
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var c = 0;
                    foreach (var log in historyLogs)
                    {
                        var json = context.ReadObject(log, nameof(log) + $"{c++}");
                        sb.AppendLine(json.ToString());
                    }
                }
                sb.AppendLine();
            }

            if (logs.List.Length > 0)
            {
                sb.AppendLine($"Cluster Observer Log Entries:{Environment.NewLine}-----------------------");
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var c = 0;
                    foreach (var log in logs.List)
                    {
                        var json = context.ReadObject(log.ToJson(), nameof(log) + $"{c++}");
                        sb.AppendLine(json.ToString());
                    }
                }
            }

            if (inMemoryDebug != null)
            {
                sb.AppendLine($"RachisDebug:{Environment.NewLine}-----------------------");
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var json = context.ReadObject(inMemoryDebug, nameof(inMemoryDebug));
                    sb.AppendLine(json.ToString());
                }
            }
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
