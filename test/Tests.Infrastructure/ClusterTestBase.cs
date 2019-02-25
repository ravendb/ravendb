using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Platform;
using Sparrow.Server;
using Xunit;

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

        protected async Task CreateAndWaitForClusterDatabase(string databaseName, IDocumentStore store, int replicationFactor = 2)
        {
            if (Servers.Count == 0)
                throw new InvalidOperationException("You cannot create a database on an empty cluster...");

            var databaseResult = CreateClusterDatabase(databaseName, store, replicationFactor);

            Assert.True(databaseResult.RaftCommandIndex > 0); //sanity check                
            await WaitForRaftIndexToBeAppliedInCluster(databaseResult.RaftCommandIndex, TimeSpan.FromSeconds(5));
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

        protected async Task<RavenServer> ActionWithLeader(Func<RavenServer, Task> act)
        {
            var retries = 5;
            Exception err = null;
            while (retries-- > 0)
            {
                try
                {
                    var leader = Servers.FirstOrDefault(s => s.ServerStore.IsLeader());
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

        protected bool WaitForDocument(IDocumentStore store,
            string docId,
            int timeout = 10000)
        {
            return WaitForDocument<dynamic>(store, docId, predicate: null, timeout: timeout);
        }

        protected bool WaitForDocument<T>(IDocumentStore store,
            string docId,
            Func<T, bool> predicate,
            int timeout = 10000)
        {
            if (DebuggerAttachedTimeout.DisableLongTimespan == false &&
                Debugger.IsAttached)
                timeout *= 100;

            var sw = Stopwatch.StartNew();
            Exception ex = null;
            while (sw.ElapsedMilliseconds < timeout)
            {
                using (var session = store.OpenSession())
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

            using (var session = store.OpenSession())
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


        protected static void DisposeServerAndWaitForFinishOfDisposal(RavenServer serverToDispose)
        {
            var mre = new ManualResetEventSlim();
            serverToDispose.AfterDisposal += () => mre.Set();
            serverToDispose.Dispose();

            mre.Wait();
        }

        protected static async Task DisposeServerAndWaitForFinishOfDisposalAsync(RavenServer serverToDispose, CancellationToken token = default)
        {
            var mre = new AsyncManualResetEvent();
            serverToDispose.AfterDisposal += () => mre.Set();
            serverToDispose.Dispose();

            await mre.WaitAsync(token).ConfigureAwait(false);
        }

        protected async Task DisposeAndRemoveServer(RavenServer serverToDispose)
        {
            await DisposeServerAndWaitForFinishOfDisposalAsync(serverToDispose);
            Servers.Remove(serverToDispose);
        }

        protected List<DocumentStore> GetStoresFromTopology(IReadOnlyList<ServerNode> topologyNodes)
        {
            var stores = new List<DocumentStore>();
            var tokenToUrl = Servers.ToDictionary(x => x.ServerStore.NodeTag, x => x.WebUrl);
            foreach (var node in topologyNodes)
            {
                string url;
                if (!tokenToUrl.TryGetValue(node.ClusterTag, out url)) //precaution
                    continue;

                var store = new DocumentStore
                {
                    Database = node.Database,
                    Urls = new[] { url },
                    Conventions =
                    {
                        DisableTopologyUpdates = true
                    }
                };

                //_toDispose.Add(store);

                store.Initialize();
                stores.Add(store);
            }
            return stores;
        }

        protected async Task<RavenServer> SetupRaftClusterOnExistingServers(params RavenServer[] servers)
        {
            RavenServer leader = null;
            var numberOfNodes = servers.Length;
            var serversToPorts = new Dictionary<RavenServer, string>();
            var leaderIndex = _random.Next(0, numberOfNodes);
            for (var i = 0; i < numberOfNodes; i++)
            {
                var server = servers[i];
                serversToPorts.Add(server, server.WebUrl);
                if (i == leaderIndex)
                {
                    server.ServerStore.EnsureNotPassive();
                    leader = server;
                    break;
                }
            }

            for (var i = 0; i < numberOfNodes; i++)
            {
                if (i == leaderIndex)
                {
                    continue;
                }
                var follower = Servers[i];
                // ReSharper disable once PossibleNullReferenceException
                await leader.ServerStore.AddNodeToClusterAsync(serversToPorts[follower]);
                await follower.ServerStore.WaitForTopology(Leader.TopologyModification.Voter);
            }
            // ReSharper disable once PossibleNullReferenceException
            Assert.True(await leader.ServerStore.WaitForState(RachisState.Leader, CancellationToken.None).WaitAsync(numberOfNodes * _electionTimeoutInMs),
                "The leader has changed while waiting for cluster to become stable. Status: " + leader.ServerStore.LastStateChangeReason());
            return leader;
        }

        protected async Task<(List<RavenServer> Nodes, RavenServer Leader)> CreateRaftCluster(int numberOfNodes, bool shouldRunInMemory = true, int? leaderIndex = null, bool useSsl = false,
            IDictionary<string, string> customSettings = null)
        {
            leaderIndex = leaderIndex ?? _random.Next(0, numberOfNodes);
            RavenServer leader = null;
            var serversToPorts = new Dictionary<RavenServer, string>();
            var clustersServers = new List<RavenServer>();
            _electionTimeoutInMs = Math.Max(300, numberOfNodes * 80);
            for (var i = 0; i < numberOfNodes; i++)
            {
                customSettings = customSettings ?? new Dictionary<string, string>()
                {
                    [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "1",
                    [RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = _electionTimeoutInMs.ToString(),
                    [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
                };
                string serverUrl;

                if (useSsl)
                {
                    serverUrl = UseFiddlerUrl("https://127.0.0.1:0");
                    SetupServerAuthentication(customSettings, serverUrl);
                }
                else
                {
                    serverUrl = UseFiddlerUrl("http://127.0.0.1:0");
                    customSettings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = serverUrl;
                }

                var server = GetNewServer(customSettings, runInMemory: shouldRunInMemory);
                var port = Convert.ToInt32(server.ServerStore.GetNodeHttpServerUrl().Split(':')[2]);
                var prefix = useSsl ? "https" : "http";
                serverUrl = UseFiddlerUrl($"{prefix}://127.0.0.1:{port}");
                Servers.Add(server);
                clustersServers.Add(server);

                serversToPorts.Add(server, serverUrl);
                if (i == leaderIndex)
                {
                    server.ServerStore.EnsureNotPassive();
                    leader = server;
                }
            }
            for (var i = 0; i < numberOfNodes; i++)
            {
                if (i == leaderIndex)
                {
                    continue;
                }
                var follower = clustersServers[i];
                // ReSharper disable once PossibleNullReferenceException
                await leader.ServerStore.AddNodeToClusterAsync(serversToPorts[follower]);
                await follower.ServerStore.WaitForTopology(Leader.TopologyModification.Voter);
            }
            // ReSharper disable once PossibleNullReferenceException
            var condition = await leader.ServerStore.WaitForState(RachisState.Leader, CancellationToken.None).WaitAsync(numberOfNodes * _electionTimeoutInMs * 5);
            var states = string.Empty;
            if (condition == false)
            {
                states = GetLastStatesFromAllServersOrderedByTime();
            }
            Assert.True(condition, "The leader has changed while waiting for cluster to become stable. All nodes status: " + states);
            return (clustersServers, leader);
        }

        protected async Task<RavenServer> CreateRaftClusterAndGetLeader(int numberOfNodes, bool shouldRunInMemory = true, int? leaderIndex = null, bool useSsl = false, IDictionary<string, string> customSettings = null)
        {
            return (await CreateRaftCluster(numberOfNodes, shouldRunInMemory, leaderIndex, useSsl, customSettings)).Leader;
        }

        protected async Task<(RavenServer, Dictionary<RavenServer, ProxyServer>)> CreateRaftClusterWithProxiesAndGetLeader(int numberOfNodes, bool shouldRunInMemory = true, int? leaderIndex = null, bool useSsl = false, int delay = 0)
        {
            leaderIndex = leaderIndex ?? _random.Next(0, numberOfNodes);
            RavenServer leader = null;
            var serversToPorts = new Dictionary<RavenServer, string>();
            var serversToProxies = new Dictionary<RavenServer, ProxyServer>();
            for (var i = 0; i < numberOfNodes; i++)
            {
                string serverUrl;
                var customSettings = GetServerSettingsForPort(useSsl, out serverUrl);

                int proxyPort = 10000;
                var server = GetNewServer(customSettings, runInMemory: shouldRunInMemory);
                var proxy = new ProxyServer(ref proxyPort, Convert.ToInt32(server.ServerStore.GetNodeHttpServerUrl()), delay);
                serversToProxies.Add(server, proxy);

                if (Servers.Any(s => s.WebUrl.Equals(server.WebUrl, StringComparison.OrdinalIgnoreCase)) == false)
                {
                    Servers.Add(server);
                }

                serversToPorts.Add(server, serverUrl);
                if (i == leaderIndex)
                {
                    server.ServerStore.EnsureNotPassive();
                    leader = server;
                }
            }
            for (var i = 0; i < numberOfNodes; i++)
            {
                if (i == leaderIndex)
                {
                    continue;
                }
                var follower = Servers[i];
                // ReSharper disable once PossibleNullReferenceException
                await leader.ServerStore.AddNodeToClusterAsync(serversToPorts[follower]);
                await follower.ServerStore.WaitForTopology(Leader.TopologyModification.Voter);
            }
            // ReSharper disable once PossibleNullReferenceException
            var condition = await leader.ServerStore.WaitForState(RachisState.Leader, CancellationToken.None).WaitAsync(numberOfNodes * _electionTimeoutInMs * 5);
            var states = string.Empty;
            if (condition == false)
            {
                states = GetLastStatesFromAllServersOrderedByTime();
            }
            Assert.True(condition, "The leader has changed while waiting for cluster to become stable. All nodes status: " + states);
            return (leader, serversToProxies);
        }


        protected Dictionary<string, string> GetServerSettingsForPort(bool useSsl, out string serverUrl)
        {
            var customSettings = new Dictionary<string, string>();

            if (useSsl)
            {
                serverUrl = UseFiddlerUrl("https://127.0.0.1:0");
                SetupServerAuthentication(customSettings, serverUrl);
            }
            else
            {
                serverUrl = UseFiddlerUrl("http://127.0.0.1:0");
                customSettings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = serverUrl;
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
            var serverCount = Servers.Count(s => s.Disposed == false);
            if(serverCount < replicationFactor)
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
                databaseResult = store.Maintenance.Server.Send(new CreateDatabaseOperation(record, replicationFactor));
                urls = await GetClusterNodeUrlsAsync(leadersUrl, store);
            }

            var currentServers = Servers.Where(s => s.Disposed == false && 
                                                    urls.Contains(s.WebUrl,StringComparer.CurrentCultureIgnoreCase)).ToArray();
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
                                                    Server urls are {string.Join(",",Servers.Select(x => $"[{x.WebUrl}|{x.Disposed}]"))}; Current cluster urls are : {string.Join(",",urls)}; The relevant servers are : {string.Join(",",relevantServers.Select(x => x.WebUrl))}; current servers are : {string.Join(",",currentServers.Select(x => x.WebUrl))}");
            return (databaseResult.RaftCommandIndex,
                relevantServers.ToList());
        }

        private static async Task<string[]> GetClusterNodeUrlsAsync(string leadersUrl, IDocumentStore store)
        {
            string[] urls;
            using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(leadersUrl, store.Certificate))
            {
                try
                {
                    await requestExecutor.UpdateTopologyAsync(new ServerNode
                        {Url = leadersUrl}, 15000, true);
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

        public override void Dispose()
        {
            foreach (var disposable in _toDispose)
                disposable.Dispose();

            foreach (var server in Servers)
            {
                if (IsGlobalServer(server))
                    continue; // must not dispose the global server
                server?.Dispose();
            }

            base.Dispose();
        }
    }
}
