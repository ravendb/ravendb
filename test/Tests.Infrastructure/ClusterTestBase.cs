using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Replication;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.Server.Commands;
using Raven.Server;
using Raven.Server.Rachis;
using Sparrow.Json;
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

        protected async Task<bool> WaitUntilDatabaseHasState(DocumentStore store, TimeSpan timeout, bool isLoaded, string databaseName = null)
        {
            var requestExecutor = store.GetRequestExecuter();
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

        protected void SetupReplicationOnDatabaseTopology(IReadOnlyList<ServerNode> topologyNodes)
        {
            var stores = GetStoresFromTopology(topologyNodes);

            //setup replication -> all nodes to all nodes
            foreach (var store in stores)
            {
                SetupReplication_TEMP(store, new ReplicationDocument(), stores.Except(new[] { store }).ToArray());
            }
        }

        protected List<DocumentStore> GetStoresFromTopology(IReadOnlyList<ServerNode> topologyNodes)
        {
            var stores = new List<DocumentStore>();
            var tokenToUrl = Servers.ToDictionary(x => x.ServerStore.NodeTag, x => x.WebUrls[0]);
            foreach (var node in topologyNodes)
            {
                string url;
                if(!tokenToUrl.TryGetValue(node.ClusterTag, out url)) //precaution
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

        protected void SetupReplication_TEMP(DocumentStore fromStore, ReplicationDocument configOptions, params DocumentStore[] toStores)
        {
            using (var session = fromStore.OpenSession())
            {
                var destinations = new List<ReplicationDestination>();
                foreach (var store in toStores)
                {
                    var replicationDestination = new ReplicationDestination
                    {
                        Database = store.DefaultDatabase,
                        Url = store.Url
                    };
                    destinations.Add(replicationDestination);
                }

                configOptions.Destinations = destinations;
                session.Store(configOptions, Constants.Documents.Replication.ReplicationConfigurationDocument);
                session.SaveChanges();
            }
        }

        protected async Task<RavenServer> CreateRaftClusterAndGetLeader(int numberOfNodes, bool shouldRunInMemory = true)
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
                },runInMemory:shouldRunInMemory);
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
