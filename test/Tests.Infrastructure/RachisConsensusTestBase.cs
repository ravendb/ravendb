using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Raven.Server.Config.Settings;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Data;
using Xunit;

namespace Tests.Infrastructure
{
    [Trait("Category", "Rachis")]
    public class RachisConsensusTestBase : IDisposable
    {
        protected bool PredictableSeeds;

        protected Logger Log = LoggingSource.Instance.GetLogger<RachisConsensusTestBase>("RachisConsensusTest");

        protected int LongWaitTime = 15000; //under stress the thread pool may take time to schedule the task to complete the set of the TCS

        protected async Task<RachisConsensus<CountingStateMachine>> CreateNetworkAndGetLeader(int nodeCount, [CallerMemberName] string caller = null)
        {
            var initialCount = RachisConsensuses.Count;
            var leaderIndex = _random.Next(0, nodeCount);
            for (var i = 0; i < nodeCount; i++)
            {
                // ReSharper disable once ExplicitCallerInfoArgument
                SetupServer(i == leaderIndex,caller: caller);
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

        protected RachisConsensus<CountingStateMachine> SetupServer(bool bootstrap = false, int port = 0, [CallerMemberName] string caller = null)
        {
            var tcpListener = new TcpListener(IPAddress.Loopback, port);
            tcpListener.Start();
            var ch = (char)(66 + _count++);
            if (bootstrap)
            {
                ch = (char)65;
                _count--;
            }


            var url = "tcp://localhost:" + ((IPEndPoint)tcpListener.LocalEndpoint).Port + "/?" + caller + "#" + ch;


            var server = StorageEnvironmentOptions.CreateMemoryOnly();
        
            int seed = PredictableSeeds ? _random.Next(int.MaxValue) : _count;
            var rachis = new RachisConsensus<CountingStateMachine>(seed);
            var storageEnvironment = new StorageEnvironment(server);
            rachis.Initialize(storageEnvironment,new ClusterConfiguration
            {
                ElectionTimeout = new TimeSetting(300,TimeUnit.Milliseconds)
            });
            rachis.OnDispose += (sender, args) =>
            {
                storageEnvironment.Dispose();
            };
            if (bootstrap)
            {
                rachis.Bootstarp(url);
            }
                
            rachis.Url = url;
            _listeners.Add(tcpListener);
            RachisConsensuses.Add(rachis);
            var task = AcceptConnection(tcpListener, rachis);
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
                rachis.AcceptNewConnection(tcpClient.GetStream(), hello =>
                {
                    lock (this)
                    {
                        ConcurrentSet<string> set;
                        if (_rejectionList.TryGetValue(rachis.Url, out set))
                        {
                            if (set.Contains(hello.DebugSourceIdentifier))
                            {
                                throw new InvalidComObjectException("Simulated failure");
                            }
                        }                            
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
                var fromTag = from.Substring(from.IndexOf('#') + 1);
                rejections.Add(fromTag);
                rejections.Add(from);
                ConcurrentSet<Tuple<string, TcpClient>> set;
                if (_connections.TryGetValue(to, out set))
                {                    
                    foreach (var tuple in set)
                    {
                        if (tuple.Item1 == from || tuple.Item1 == fromTag)
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
                var fromTag = from.Substring(from.IndexOf('#') + 1);
                rejectionList.TryRemove(from);
                rejectionList.TryRemove(fromTag);
            }
        }

        protected async Task<long> IssueCommandsAndWaitForCommit(RachisConsensus<CountingStateMachine> leader, int numberOfCommands, String Name, int value)
        {
            Assert.True(leader.CurrentState == RachisConsensus.State.Leader || leader.CurrentState == RachisConsensus.State.LeaderElect, "Can't append commands from non leader");
            using (leader.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
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

                Assert.True(mustBeSuccessfulTask.Wait(250));
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
            

            public override bool ShouldSnapshot(Slice slice, RootObjectType type)
            {
                return slice.ToString() == "values";
            }

            public override async Task<Stream> ConnectToPeer(string url, string apiKey)
            {
                var tcpInfo = new Uri(url);
                var tcpClient = new TcpClient();
                await tcpClient.ConnectAsync(tcpInfo.Host, tcpInfo.Port);
                return tcpClient.GetStream();
            }
        }
    }
}
