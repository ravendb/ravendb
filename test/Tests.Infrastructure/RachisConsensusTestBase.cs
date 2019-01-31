using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
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
        static RachisConsensusTestBase()
        {
            JsonDeserializationCluster.Commands.Add(nameof(TestCommand), JsonDeserializationBase.GenerateJsonDeserializationRoutine<TestCommand>());
        }

        protected bool PredictableSeeds;

        protected readonly Logger Log = LoggingSource.Instance.GetLogger<RachisConsensusTestBase>("RachisConsensusTest");

        protected int LongWaitTime = 15000; //under stress the thread pool may take time to schedule the task to complete the set of the TCS

        protected async Task<RachisConsensus<CountingStateMachine>> CreateNetworkAndGetLeader(int nodeCount, [CallerMemberName] string caller = null)
        {
            var initialCount = RachisConsensuses.Count;
            var leaderIndex = _random.Next(0, nodeCount);
            var timeout = TimeSpan.FromSeconds(10);
            var electionTimeout = Math.Max(300, nodeCount * 60); // We want to make it easier for the tests, since we are running multiple servers on the same machine. 
            for (var i = 0; i < nodeCount; i++)
            {
                // ReSharper disable once ExplicitCallerInfoArgument
                SetupServer(i == leaderIndex, electionTimeout: electionTimeout, caller: caller);
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
                var done = await follower.WaitForTopology(Leader.TopologyModification.Voter).WaitAsync(timeout);
                Assert.True(done, "Waited for node to become a follower for too long");
            }
            var currentState = RachisConsensuses[leaderIndex + initialCount].CurrentState;
            Assert.True(currentState == RachisState.Leader ||
                        currentState == RachisState.LeaderElect,
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
                     x => x.CurrentState != RachisState.Leader &&
                     x.CurrentState != RachisState.LeaderElect).ToList();
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

            foreach (var node in nodes)
            {
                waitingTasks.Add(node.WaitForState(RachisState.Leader, CancellationToken.None));
            }
            Assert.True(Task.WhenAny(waitingTasks).Wait(3000 * nodes.Count()), "Waited too long for a node to become a leader but no leader was elected.");
            return nodes.FirstOrDefault(x => x.CurrentState == RachisState.Leader);
        }

        protected RachisConsensus<CountingStateMachine> SetupServer(bool bootstrap = false, int port = 0, int electionTimeout = 300, [CallerMemberName] string caller = null)
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
            var configuration = RavenConfiguration.CreateForServer(caller);
            configuration.Initialize();
            configuration.Core.RunInMemory = true;
            configuration.Core.PublicServerUrl = new UriSetting($"http://localhost:{((IPEndPoint)tcpListener.LocalEndpoint).Port}");
            configuration.Cluster.ElectionTimeout = new TimeSetting(electionTimeout, TimeUnit.Milliseconds);
            var serverStore = new RavenServer(configuration) { ThrowOnLicenseActivationFailure = true }.ServerStore;
            serverStore.Initialize();
            var rachis = new RachisConsensus<CountingStateMachine>(serverStore, seed);
            var storageEnvironment = new StorageEnvironment(server);
            rachis.Initialize(storageEnvironment, configuration, configuration.Core.ServerUrls[0]);
            rachis.OnDispose += (sender, args) =>
            {
                serverStore.Dispose();
                storageEnvironment.Dispose();
            };
            if (bootstrap)
            {
                rachis.Bootstrap(url, "A");
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
                var stream = tcpClient.GetStream();
                try
                {
                    rachis.AcceptNewConnection(stream, () => tcpClient.Client.Disconnect(false), tcpClient.Client.RemoteEndPoint, hello =>
                    {
                        if (rachis.Url == null)
                            return;

                        lock (this)
                        {
                            if (_rejectionList.TryGetValue(rachis.Url, out var set))
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
                catch
                {
                    // expected
                }
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
                if (_connections.TryGetValue(to, out var set))
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
                if (_rejectionList.TryGetValue(to, out var rejectionList) == false)
                    return;
                var fromTag = from.Substring(from.IndexOf('#') + 1);
                rejectionList.TryRemove(from);
                rejectionList.TryRemove(fromTag);
            }
        }

        protected async Task<long> IssueCommandsAndWaitForCommit(int numberOfCommands, string name, int value)
        {
            for (var i = 0; i < numberOfCommands; i++)
            {
                await ActionWithLeader(l => l.PutAsync(new TestCommand
                {
                    Name = name,
                    Value = value
                }));
            }

            long index = -1;

            await ActionWithLeader(l =>
            {

                using (l.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                    index = l.GetLastEntryIndex(context);
                return Task.CompletedTask;
            });

            return index;
        }

        protected List<Task> IssueCommandsWithoutWaitingForCommits(RachisConsensus<CountingStateMachine> leader, int numberOfCommands, string name, int value)
        {
            List<Task> waitingList = new List<Task>();
            for (var i = 0; i < numberOfCommands; i++)
            {
                var task = leader.PutAsync(new TestCommand
                {
                    Name = name,
                    Value = value
                });

                waitingList.Add(task);
            }
            return waitingList;
        }


        protected async Task<Task> ActionWithLeader(Func<RachisConsensus<CountingStateMachine>, Task> action)
        {
            var retires = 5;
            Exception lastException;
            
            do
            {
                try
                {
                    using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
                    {
                        var tasks = RachisConsensuses.Select(x => x.WaitForState(RachisState.Leader, cts.Token));
                        await Task.WhenAny(tasks);
                        var leader = RachisConsensuses.Single(x => x.CurrentState == RachisState.Leader);
                        return action(leader);
                    }
                }
                catch (Exception e)
                {
                    lastException = e;
                }
            } while (retires-- > 0);

            if (lastException != null)
                throw new InvalidOperationException("Gave up after 5 retires", lastException);

            throw new InvalidOperationException("Should never happened!");
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

        public class CountingValidator : RachisVersionValidation
        {
            public override void AssertPutCommandToLeader(CommandBase cmd)
            {
            }

            public override void AssertEntryBeforeSendToFollower(BlittableJsonReaderObject entry, int version, string follower)
            {
            }
        }

        public class CountingStateMachine : RachisStateMachine
        {
            public string Read(TransactionOperationContext context, string name)
            {
                var tree = context.Transaction.InnerTransaction.ReadTree("values");
                var read = tree.Read(name);
                return read?.Reader.ToStringValue();
            }

            protected override void Apply(TransactionOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader, ServerStore serverStore)
            {
                Assert.True(cmd.TryGet(nameof(TestCommand.Name), out string name));
                Assert.True(cmd.TryGet(nameof(TestCommand.Value), out int val));

                var tree = context.Transaction.InnerTransaction.CreateTree("values");
                var current = tree.Read(name)?.Reader.ToStringValue();
                tree.Add(name, current + val);
            }

            protected override RachisVersionValidation InitializeValidator()
            {
                return new CountingValidator();
            }

            public override bool ShouldSnapshot(Slice slice, RootObjectType type)
            {
                return slice.ToString() == "values";
            }

            public override async Task<RachisConnection> ConnectToPeer(string url, string tag, X509Certificate2 certificate)
            {
                TimeSpan time;
                using (ContextPoolForReadOnlyOperations.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    time = _parent.ElectionTimeout * (_parent.GetTopology(ctx).AllNodes.Count - 2);
                }
                var tcpClient = await TcpUtils.ConnectAsync(url, time);
                return new RachisConnection
                {
                    Stream = tcpClient.GetStream(),

                    SupportedFeatures = new TcpConnectionHeaderMessage.SupportedFeatures(TcpConnectionHeaderMessage.NoneBaseLine),
                    Disconnect = () => tcpClient.Client.Disconnect(false)
                };
            }
        }

        public class TestCommand : CommandBase
        {
            public string Name;

            public object Value;

            public override DynamicJsonValue ToJson(JsonOperationContext context)
            {
                var djv = base.ToJson(context);
                djv[nameof(Name)] = Name;
                djv[nameof(Value)] = Value;

                return djv;
            }
        }
    }
}
