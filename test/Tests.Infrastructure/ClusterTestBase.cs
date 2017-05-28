using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.Server;
using Raven.Client.Server.Commands;
using Raven.Client.Server.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Platform;
using Xunit;

namespace Tests.Infrastructure
{
    [Trait("Category", "Cluster")]
    public abstract class ClusterTestBase : RavenTestBase
    {
        static ClusterTestBase()
        {
            Console.WriteLine($"\tTo attach debugger to test process ({(PlatformDetails.Is32Bits ? "x86" : "x64")}), use proc-id: {Process.GetCurrentProcess().Id}.");
        }

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

        protected IDisposable NoTimeouts()
        {
            TimeoutEvent.Disable = true;
            return new DisposableAction(() => TimeoutEvent.Disable = false);
        }

        protected async Task CreateAndWaitForClusterDatabase(string databaseName, IDocumentStore store)
        {
            if (Servers.Count == 0)
                throw new InvalidOperationException("You cannot create a database on an empty cluster...");

            var databaseResult = CreateClusterDatabase(databaseName, store);

            Assert.True((databaseResult.ETag ?? 0) > 0); //sanity check                

            await WaitForRaftIndexToBeAppliedInCluster(databaseResult.ETag ?? 0, TimeSpan.FromSeconds(5));
        }

        protected static CreateDatabaseResult CreateClusterDatabase(string databaseName, IDocumentStore store, int replicationFactor = 2)
        {
            var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
            var databaseResult = store.Admin.Server.Send(new CreateDatabaseOperation(doc, replicationFactor));
            return databaseResult;
        }


        protected async Task<bool> WaitUntilDatabaseHasState(DocumentStore store, TimeSpan timeout, bool isLoaded, string databaseName = null)
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

        protected async Task<T> WaitForValueOnGroupAsync<T>(DatabaseTopology topology, Func<ServerStore, T> func, T excpected)
        {
            var nodes = topology.AllReplicationNodes();
            var servers = new List<ServerStore>();
            var tasks = new Dictionary<string,Task<T>>();
            foreach (var node in nodes)
            {
                var server = Servers.Single(s => s.ServerStore.NodeTag == node.NodeTag);
                servers.Add(server.ServerStore);
               
            }
            foreach (var server in servers)
            {
                var task = WaitForValueAsync(() => func(server), excpected);
                tasks.Add(server.NodeTag, task);
            }

            var res = await Task.WhenAll(tasks.Values);
            var hasExpectedVals = res.Where(t => t?.Equals(excpected) ?? false);
           
            if(hasExpectedVals.Count() == servers.Count)
                return excpected;

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
            throw new Exception($"Not all node in the group have the expected value of {excpected}. {otherValues}");
        }

        protected async Task<bool> WaitForDocumentInClusterAsync<T>(DatabaseTopology topology, string docId, Func<T, bool> predicate, TimeSpan timeout)
        {
            var stores = GetDocumentStores(topology);
            return await WaitForDocumentInClusterAsyncInternal(docId, predicate, timeout, stores);
        }

        protected async Task<bool> WaitForDocumentInClusterAsync<T>(IReadOnlyList<ServerNode> topology,string docId, Func<T, bool> predicate, TimeSpan timeout)
        {
            var stores = GetStoresFromTopology(topology);
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

        private List<DocumentStore> GetDocumentStores(DatabaseTopology topology)
        {
            var stores = new List<DocumentStore>();
            foreach (var node in topology.AllReplicationNodes())
            {
                stores.Add(new DocumentStore
                {
                    Url = node.Url,
                    Database = node.Database
                });
            }
            return stores;
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
                Task.Delay(100);
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
                if(!tokenToUrl.TryGetValue(node.ClusterTag, out url)) //precaution
                    continue;

                var store = new DocumentStore
                {
                    Database = node.Database,
                    Url = url
                };

                _toDispose.Add(store);

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
                serversToPorts.Add(server, server.WebUrls[0]);
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
            Assert.True(await leader.ServerStore.WaitForState(RachisConsensus.State.Leader).WaitAsync(numberOfNodes * ElectionTimeoutInMs),
                "The leader has changed while waiting for cluster to become stable. Status: " + leader.ServerStore.ClusterStatus());
            return leader;
        }

        protected async Task<RavenServer> CreateRaftClusterAndGetLeader(int numberOfNodes, bool shouldRunInMemory = true, int? leaderIndex = null, bool useSsl = false)
        {
            leaderIndex = leaderIndex ?? _random.Next(0, numberOfNodes);
            RavenServer leader = null;
            var serversToPorts = new Dictionary<RavenServer,string>();
            for (var i = 0; i < numberOfNodes; i++)
            {
                var serverUrl = UseFiddler($"http://127.0.0.1:{GetPort()}");

                var customSettings = new Dictionary<string, string>{{"Raven/ServerUrl", serverUrl}};

                if (useSsl)
                {
                    var certificatePath = GenerateAndSaveSelfSignedCertificate();
                    serverUrl = serverUrl.Replace("http:", "https:");

                    customSettings["Raven/Certificate/Path"] = certificatePath;
                    customSettings["Raven/ServerUrl"] = serverUrl;
                }

                var server = GetNewServer(customSettings ,runInMemory:shouldRunInMemory);

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
            var condition = await leader.ServerStore.WaitForState(RachisConsensus.State.Leader).WaitAsync(numberOfNodes* ElectionTimeoutInMs * 5);
            Assert.True(condition,
                "The leader has changed while waiting for cluster to become stable. Status: " + leader.ServerStore.ClusterStatus() );
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

        protected override Task<DocumentDatabase> GetDocumentDatabaseInstanceFor(IDocumentStore store)
        {
            //var index = FindStoreIndex(store);
            //Assert.False(index == -1, "Didn't find store index, most likely it doesn't belong to the cluster. Did you setup Raft cluster properly?");
            //return Servers[index].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            return Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
        }

        public async Task WaitForRaftIndexToBeAppliedInCluster(long index,  TimeSpan timeout)
        {
            if (Servers.Count == 0)
                return;

            var tasks = 
                Servers
                .Select(server => server.ServerStore.Cluster.WaitForIndexNotification(index))
                .ToList();


            if (await Task.WhenAll(tasks).WaitAsync(timeout))
                return;


            var message = $"Timed out waiting for {index} after {timeout} because out of {Servers.Count} " + 
                          $" we got confirmations that it was applied only on {tasks.Count(x=>x.IsCompleted)}";

            //Console.ForegroundColor = ConsoleColor.Blue;

            //Console.WriteLine(message);

            //Console.ReadLine();

            throw new TimeoutException(
                message
                );
        }

        public async Task<Tuple<long, List<RavenServer>>> CreateDatabaseInCluster(string databaseName, int replicationFactor, string leasderUrl)
        {
            CreateDatabaseResult databaseResult;
            using (var store = new DocumentStore()
            {
                Url = leasderUrl,
                Database = databaseName
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
                databaseResult = store.Admin.Server.Send(new CreateDatabaseOperation(doc, replicationFactor));
            }
            int numberOfInstances = 0;
            foreach (var server in Servers.Where(s => databaseResult.Topology.RelevantFor(s.ServerStore.NodeTag)))
            {
                await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.ETag ?? 0);
                try
                {
                    await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                    numberOfInstances++;
                }
                catch
                {

                }
            }
            if (numberOfInstances != replicationFactor)
                throw new InvalidOperationException("Couldn't create the db on all nodes, just on " + numberOfInstances + " out of " + replicationFactor);
            return Tuple.Create(databaseResult.ETag.Value,
                Servers.Where(s => databaseResult.Topology.RelevantFor(s.ServerStore.NodeTag)).ToList());


        }

        public override void Dispose()
        {
            foreach (var disposable in _toDispose)
                disposable.Dispose();

            foreach (var server in Servers)
            {
                server?.Dispose();
            }

            base.Dispose();
        }
    }
}
