using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Replication;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Raven.Server;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Xunit;
using Constants = Raven.Client.Constants;

namespace Tests.Infrastructure
{
    [Trait("Category", "Cluster")]
    public abstract class ClusterTestBase : RavenTestBase
    {
        private const int PortRangeStart = 9000;
        private const int ElectionTimeoutInMs = 300;
        private static int numberOfPortRequests;

        internal static int GetPort()
        {
            var portRequest = Interlocked.Increment(ref numberOfPortRequests);
            return PortRangeStart - (portRequest % 500);
        }

        protected readonly ConcurrentBag<IDisposable> _toDispose = new ConcurrentBag<IDisposable>();

        protected List<RavenServer> Servers = new List<RavenServer>();
        private readonly Random _random = new Random();

        protected void NoTimeouts()
        {
            TimeoutEvent.Disable = true;
        }

        protected async Task<bool> WaitForDocumentInClusterAsync<T>(IReadOnlyList<ServerNode> topology,string docId, Func<T, bool> predicate, TimeSpan timeout)
        {
            var stores = GetStoresFromTopology(topology);
            var tasks = new List<Task<bool>>();

            foreach(var store in stores)
                tasks.Add(Task.Run(() => WaitForDocument(store, docId, predicate,(int)timeout.TotalMilliseconds)));

            var timeoutTask = Task.Delay(timeout);
            await Task.WhenAny(Task.WhenAll(tasks), timeoutTask);

            if(timeoutTask.IsCompleted)
                throw new TimeoutException();

            return tasks.All(x => x.Result);
        }

        protected bool WaitForDocument<T>(DocumentStore store,
            string docId,
            Func<T, bool> predicate,
            int timeout = 10000)
        {
            if (Debugger.IsAttached)
                timeout *= 100;

            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeout)
            {
                using (var session = store.OpenSession())
                {
                    try
                    {
                        var doc = session.Load<T>(docId);
                        if (doc != null && predicate(doc))
                        {
                            return true;
                        }
                    }
                    catch (ConflictException)
                    {
                        // expected that we might get conflict, ignore and wait
                    }
                }
            }
            using (var session = store.OpenSession())
            {
                //one last try, and throw if there is still a conflict
                var doc = session.Load<T>(docId);
                if (doc != null && predicate(doc))
                    return true;
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

        protected List<DocumentStore> GetStoresFromTopology(IReadOnlyList<ServerNode> topologyNodes)
        {
            var stores = new List<DocumentStore>();
            var tokenToUrl = Servers.ToDictionary(x => x.ServerStore.NodeTag, x => x.WebUrls[0]);
            foreach (var node in topologyNodes)
            {
                string url;
                if(!tokenToUrl.TryGetValue(node.ClusterToken,out url)) //precaution
                    continue;

                var store = new DocumentStore
                {
                    DefaultDatabase = node.Database,
                    Url = url
                };

                _toDispose.Add(store);

                store.Initialize();
                stores.Add(store);
            }
            return stores;
        }

        protected async Task<RavenServer> CreateRaftClusterAndGetLeader(RavenServer leader, params RavenServer[] serverNodes)
        {
            var serversToPorts = new Dictionary<RavenServer, string>();
            leader.ServerStore.EnsureNotPassive();
            Servers.Add(leader);
            serversToPorts.Add(leader, leader.WebUrls[0]);
            foreach (var ravenServer in serverNodes)
            {
                serversToPorts.Add(ravenServer, ravenServer.WebUrls[0]);
                Servers.Add(ravenServer);
            }

            var numberOfNodes = serverNodes.Length + 1;
            if (numberOfNodes % 2 == 0) // ensure odd number of nodes
            {
                numberOfNodes++;
                var serverUrl = $"http://localhost:{GetPort()}";
                var server = GetNewServer(new Dictionary<string, string>()
                {
                    {"Raven/ServerUrl", serverUrl}
                });
                serversToPorts.Add(server, serverUrl);
                Servers.Add(server);
            }
            // starts from 1 to skip the leader
            for (var index = 1; index < Servers.Count; index++)
            {
                RavenServer server = Servers[index];
                await leader.ServerStore.AddNodeToClusterAsync(serversToPorts[server]);
                await server.ServerStore.WaitForTopology(Leader.TopologyModification.Voter);
            }

            // ReSharper disable once PossibleNullReferenceException
            Assert.True(leader.ServerStore.WaitForState(RachisConsensus.State.Leader).Wait(numberOfNodes * ElectionTimeoutInMs),
                "The leader has changed while waiting for cluster to become stable");
            return leader;
        }


        protected async Task<RavenServer> CreateRaftClusterAndGetLeader(int numberOfNodes)
        {
            var leaderIndex = _random.Next(0, numberOfNodes);
            RavenServer leader = null;
            var serversToPorts = new Dictionary<RavenServer,string>();
            for (var i = 0; i < numberOfNodes; i++)
            {
                var serverUrl = $"http://localhost:{GetPort()}";
                var server = GetNewServer(new Dictionary<string, string>()
                {                    
                    {"Raven/ServerUrl", serverUrl}
                });
                serversToPorts.Add(server, serverUrl);
                Servers.Add(server);
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
            Assert.True(leader.ServerStore.WaitForState(RachisConsensus.State.Leader).Wait(numberOfNodes* ElectionTimeoutInMs),
                "The leader has changed while waiting for cluster to become stable");
            return leader;
        }

        protected async Task<IDocumentStore> CreateRaftClusterWithDatabaseAndGetLeaderStore(int numberOfNodes, int replicationFactor = 2, [CallerMemberName] string callerName = null)
        {
            var leader = await CreateRaftClusterAndGetLeader(numberOfNodes);
            string databaseName = callerName ?? "Test";
            var store = new DocumentStore
            {
                DefaultDatabase = databaseName,
                Url = leader.WebUrls[0]
            }.Initialize();

            var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
            var databaseResult = store.Admin.Server.Send(new CreateDatabaseOperation(doc, replicationFactor));

            Assert.True((databaseResult.ETag ?? 0) > 0); //sanity check                
            await WaitForEtagInCluster(databaseResult.ETag ?? 0, TimeSpan.FromSeconds(5));
            await ((DocumentStore)store).ForceUpdateTopologyFor(databaseName);

            TransactionOperationContext context;
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();
                var record = leader.ServerStore.Cluster.ReadDatabase(context,databaseName);             
            }
            return store;
        }

        public async Task WaitForLeader(TimeSpan timeout)
        {
            var tasks = Servers
                .Select(server => server.ServerStore.WaitForState(RachisConsensus.State.Leader))
                .ToList();

            tasks.Add(Task.Delay(timeout));
            await Task.WhenAny(tasks);

            if (Task.Delay(timeout).IsCompleted)
                throw new TimeoutException();
        }

        public async Task WaitForEtagInCluster(long etag,  TimeSpan timeout)
        {
            var tasks = 
                Servers
                .Select(server => server.ServerStore.Cluster.WaitForIndexNotification(etag))
                .ToList();

            var timeoutTask = Task.Delay(timeout);
            
            await Task.WhenAny(timeoutTask, Task.WhenAll(tasks));

            if(timeoutTask.IsCompleted)
                throw new TimeoutException();
        }

        public override void Dispose()
        {
            foreach (var disposable in _toDispose)
                disposable.Dispose();

            foreach (var server in Servers)
            {
                server.Dispose();
            }

            base.Dispose();
        }
    }
}
