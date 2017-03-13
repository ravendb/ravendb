using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data;
using Xunit;

namespace Tests.Infrastructure
{
    [Trait("Category", "Rachis")]
    public class RachisConsensusTestBase : IDisposable
    {
        protected bool PredictableSeeds;

        protected async Task<RachisConsensus<CountingStateMachine>> CreateNetworkAndGetLeader(int nodeCount)
        {
            var initialCount = RachisConsensuses.Count;
            var leaderIndex = _random.Next(0, nodeCount);
            for (var i = 0; i < nodeCount; i++)
            {
                SetupServer(i == leaderIndex);
            }
            var leader = RachisConsensuses[leaderIndex + initialCount];
            for (var i = 0; i < nodeCount; i++)
            {
                if (i == leaderIndex)
                {
                    continue;
                }
                var follower = RachisConsensuses[i + initialCount];
                await leader.AddToClusterAsync(follower.Url);
                await follower.WaitForTopology(Leader.TopologyModification.Voter);
            }
            var currentState = RachisConsensuses[leaderIndex + initialCount].CurrentState;
            Assert.True(currentState == RachisConsensus.State.Leader ||
                        currentState == RachisConsensus.State.LeaderElect,
                "The leader has changed while waiting for cluster to become stable, it is now " + currentState + " Beacuse: " + leader.LastStateChangeReason);
            return leader;
        }

        protected RachisConsensus<CountingStateMachine> GetRandomFollower()
        {
            var followers = GetFollowers();
            var indexOfFollower = _random.Next(followers.Count);
            return followers[indexOfFollower];
        }

        protected List<RachisConsensus<CountingStateMachine>> GetFollowers()
        {
            return RachisConsensuses.Where(
                     x => x.CurrentState != RachisConsensus.State.Leader &&
                     x.CurrentState != RachisConsensus.State.LeaderElect).ToList();
        }


        protected void DisconnectFromNode(RachisConsensus<CountingStateMachine> node)
        {
            foreach (var follower in RachisConsensuses.Where(x => x.Url != node.Url))
            {
                Disconnect(follower.Url, node.Url);
            }
        }

        protected void ReconnectToNode(RachisConsensus<CountingStateMachine> node)
        {
            foreach (var follower in RachisConsensuses.Where(x => x.Url != node.Url))
            {
                Reconnect(follower.Url, node.Url);
            }
        }

        protected RachisConsensus<CountingStateMachine> WaitForAnyToBecomeLeader(IEnumerable<RachisConsensus<CountingStateMachine>> nodes)
        {
            var waitingTasks = new List<Task>();

            foreach (var ndoe in nodes)
            {
                waitingTasks.Add(ndoe.WaitForState(RachisConsensus.State.Leader));
            }
            Assert.True(Task.WhenAny(waitingTasks).Wait(3000 * nodes.Count()), "Waited too long for a node to become a leader but no leader was elected.");
            return nodes.FirstOrDefault(x => x.CurrentState == RachisConsensus.State.Leader);
        }

        protected RachisConsensus<CountingStateMachine> SetupServer(bool bootstrap = false, int port = 0)
        {
            var tcpListener = new TcpListener(IPAddress.Loopback, port);
            tcpListener.Start();
            var url = "http://localhost:" + ((IPEndPoint)tcpListener.LocalEndpoint).Port;

            var server = StorageEnvironmentOptions.CreateMemoryOnly();
        
            int seed = PredictableSeeds ? _random.Next(int.MaxValue) : _count;
            var rachis = new RachisConsensus<CountingStateMachine>(seed);
            rachis.Initialize(new StorageEnvironment(server));
            if (bootstrap)
                rachis.Bootstarp(url);
            rachis.Url = url;
            _listeners.Add(tcpListener);
            RachisConsensuses.Add(rachis);
            var task = AcceptConnection(tcpListener, rachis);
            _mustBeSuccessfulTasks.Add(task);
            return rachis;
        }

        private async Task AcceptConnection(TcpListener tcpListener, RachisConsensus rachis)
        {
            rachis.OnDispose += (sender, args) => tcpListener.Stop();

            while (true)
            {
                TcpClient tcpClient;
                try
                {
                    tcpClient = await tcpListener.AcceptTcpClientAsync();
                }
                catch (InvalidOperationException)
                {
                    break;
                }
                rachis.AcceptNewConnection(tcpClient, hello =>
                {
                    lock (this)
                    {
                        ConcurrentSet<string> set;
                        if (_rejectionList.TryGetValue(rachis.Url, out set) && set.Contains(hello.DebugSourceIdentifier))
                            throw new InvalidComObjectException("Simulated failure");
                        var connections = _connections.GetOrAdd(rachis.Url, _ => new ConcurrentSet<Tuple<string, TcpClient>>());
                        connections.Add(Tuple.Create(hello.DebugSourceIdentifier, tcpClient));
                    }
                });
            }
        }

        protected void Disconnect(string to, string from)
        {
            lock (this)
            {
                var rejections = _rejectionList.GetOrAdd(to, _ => new ConcurrentSet<string>());
                rejections.Add(from);

                ConcurrentSet<Tuple<string, TcpClient>> set;
                if (_connections.TryGetValue(to, out set))
                {
                    foreach (var tuple in set)
                    {
                        if (tuple.Item1 == from)
                        {
                            set.TryRemove(tuple);
                            tuple.Item2.Dispose();
                        }
                    }
                }
            }
        }

        protected void Reconnect(string to, string from)
        {
            lock (this)
            {
                ConcurrentSet<string> rejectionList;
                if (_rejectionList.TryGetValue(to, out rejectionList) == false)
                    return;

                rejectionList.TryRemove(from);
            }
        }

        protected async Task<long> IssueCommandsAndWaitForCommit(RachisConsensus<CountingStateMachine> leader, int numberOfCommands, String Name, int value)
        {
            Assert.True(leader.CurrentState == RachisConsensus.State.Leader || leader.CurrentState == RachisConsensus.State.LeaderElect, "Can't append commands from non leader");
            TransactionOperationContext context;
            using (leader.ContextPool.AllocateOperationContext(out context))
            {
                for (var i = 0; i < 3; i++)
                {
                    await leader.PutAsync(context.ReadObject(new DynamicJsonValue
                    {
                        ["Name"] = Name,
                        ["Value"] = value
                    }, Name));
                }
                using (context.OpenReadTransaction())
                    return leader.GetLastEntryIndex(context);
            }
        }

        protected List<Task> IssueCommandsWithoutWaitingForCommits(RachisConsensus<CountingStateMachine> leader, int inumberOfCommands, string Name, int value)
        {
            Assert.True(leader.CurrentState == RachisConsensus.State.Leader, "Can't append commands from non leader");
            TransactionOperationContext context;
            List<Task> waitingList = new List<Task>();
            using (leader.ContextPool.AllocateOperationContext(out context))
            {
                for (var i = 0; i < 3; i++)
                {
                    waitingList.Add(leader.PutAsync(context.ReadObject(new DynamicJsonValue
                    {
                        ["Name"] = Name,
                        ["Value"] = value
                    }, Name)));
                }
            }
            return waitingList;
        }
        private readonly ConcurrentDictionary<string, ConcurrentSet<string>> _rejectionList = new ConcurrentDictionary<string, ConcurrentSet<string>>();
        private readonly ConcurrentDictionary<string, ConcurrentSet<Tuple<string, TcpClient>>> _connections = new ConcurrentDictionary<string, ConcurrentSet<Tuple<string, TcpClient>>>();
        private readonly List<TcpListener> _listeners = new List<TcpListener>();
        protected readonly List<RachisConsensus<CountingStateMachine>> RachisConsensuses = new List<RachisConsensus<CountingStateMachine>>();
        private readonly List<Task> _mustBeSuccessfulTasks = new List<Task>();
        private readonly Random _random = new Random();
        private int _count;

        public void Dispose()
        {
            foreach (var rc in RachisConsensuses)
            {
                rc.Dispose();
            }

            foreach (var listener in _listeners)
            {
                listener.Stop();
            }

            foreach (var mustBeSuccessfulTask in _mustBeSuccessfulTasks)
            {
                mustBeSuccessfulTask.Wait();
            }
        }

        public class CountingStateMachine : RachisStateMachine
        {
            public long Read(TransactionOperationContext context, string name)
            {
                var tree = context.Transaction.InnerTransaction.ReadTree("values");
                var read = tree.Read(name);
                if (read == null)
                    return 0;
                return read.Reader.ReadLittleEndianInt64();
            }

            protected override void Apply(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader)
            {
                int val;
                string name;
                Assert.True(cmd.TryGet("Name", out name));
                Assert.True(cmd.TryGet("Value", out val));
                var tree = context.Transaction.InnerTransaction.CreateTree("values");
                tree.Increment(name, val);

            }

            public override void OnSnapshotInstalled(TransactionOperationContext context)
            {

            }

            public override bool ShouldSnapshot(Slice slice, RootObjectType type)
            {
                return slice.ToString() == "values";
            }
        }
    }
}
