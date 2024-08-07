using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Rachis;
using Raven.Server.Rachis.Remote;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Platform;
using Sparrow.Utils;
using Voron;
using Voron.Data;
using Voron.Exceptions;
using Xunit;
using Xunit.Abstractions;
using NativeMemory = Sparrow.Utils.NativeMemory;

namespace Tests.Infrastructure
{
    [Trait("Category", "Rachis")]
    public class RachisConsensusTestBase : XunitLoggingBase, IDisposable
    {
        static unsafe RachisConsensusTestBase()
        {
            XunitLogging.RedirectStreams = false;
            XunitLogging.Init();
            XunitLogging.EnableExceptionCapture();

            NativeMemory.GetCurrentUnmanagedThreadId = () => (ulong)Pal.rvn_get_current_thread_id();
            ZstdLib.CreateDictionaryException = message => new VoronErrorException(message);
            RachisStateMachine.EnableDebugLongCommit = true;
            Lucene.Net.Util.UnmanagedStringArray.Segment.AllocateMemory = NativeMemory.AllocateMemory;
            Lucene.Net.Util.UnmanagedStringArray.Segment.FreeMemory = NativeMemory.Free;
            JsonDeserializationCluster.Commands.Add(nameof(TestCommand), JsonDeserializationBase.GenerateJsonDeserializationRoutine<TestCommand>());
        }

        public RachisConsensusTestBase(ITestOutputHelper output, [CallerFilePath] string sourceFile = "") : base(output, sourceFile)
        {
        }

        protected bool PredictableSeeds;

        protected readonly Logger Log = LoggingSource.Instance.GetLogger<RachisConsensusTestBase>("RachisConsensusTest");

        protected int LongWaitTime = 15000; //under stress the thread pool may take time to schedule the task to complete the set of the TCS

        protected async Task<RachisConsensus<CountingStateMachine>> CreateNetworkAndGetLeader(int nodeCount, [CallerMemberName] string caller = null, bool watcherCluster = false, bool shouldRunInMemory = true)
        {
            string[] allowedNodeTags = { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z" };

            var initialCount = RachisConsensuses.Count;
            var leaderIndex = _random.Next(0, nodeCount);
            var timeout = TimeSpan.FromSeconds(10);
            var electionTimeout = Math.Max(300, nodeCount * 60); // We want to make it easier for the tests, since we are running multiple servers on the same machine. 
            for (var i = 0; i < nodeCount; i++)
            {
                // ReSharper disable once ExplicitCallerInfoArgument
                SetupServer(i == leaderIndex, nodeTag: allowedNodeTags[i], electionTimeout: electionTimeout, caller: caller, shouldRunInMemory: shouldRunInMemory);
            }
            var leader = RachisConsensuses[leaderIndex + initialCount];
            for (var i = 0; i < nodeCount; i++)
            {
                if (i == leaderIndex)
                {
                    continue;
                }
                var follower = RachisConsensuses[i + initialCount];
                await leader.AddToClusterAsync(follower.Url, asWatcher: watcherCluster, nodeTag: allowedNodeTags[i]);
                var done = await follower.WaitForTopology(watcherCluster ? Leader.TopologyModification.NonVoter : Leader.TopologyModification.Voter).WaitWithoutExceptionAsync(timeout);
                Assert.True(done, "Waited for node to become a follower for too long");
            }
            var currentState = RachisConsensuses[leaderIndex + initialCount].CurrentCommittedState.State;
            Assert.True(currentState == RachisState.Leader ||
                        currentState == RachisState.LeaderElect,
                "The leader has changed while waiting for cluster to become stable, it is now " + currentState + " Beacuse: " + leader.LastStateChangeReason);

            await WaitForVoters(nodeCount, watcherCluster);

            return leader;
        }

        private Task WaitForVoters(int nodeCount, bool watcherCluster)
        {
            var votersCount = watcherCluster ? 0 : nodeCount - 1;
            if (votersCount > 0)
                return ActionWithLeader(async (leader1) =>
                {
                    var sw = Stopwatch.StartNew();
                    while (leader1.CurrentLeader.CurrentVoters.Count < votersCount)
                    {
                        await Task.Delay(100);
                        if (sw.ElapsedMilliseconds > 15_000)
                        {
                            throw new TimeoutException("waited too much to leader voters.");
                        }
                    }
                });

            return Task.CompletedTask;
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
                     x => x.CurrentCommittedState.State != RachisState.Leader &&
                     x.CurrentCommittedState.State != RachisState.LeaderElect).ToList();
        }

        protected void DisconnectFromNode(RachisConsensus<CountingStateMachine> node)
        {
            foreach (var follower in RachisConsensuses.Where(x => x.Url != node.Url))
            {
                Disconnect(follower.Url, node.Url);
            }
        }

        protected void DisconnectBiDirectionalFromNode(RachisConsensus<CountingStateMachine> node)
        {
            foreach (var follower in RachisConsensuses.Where(x => x.Url != node.Url))
            {
                Disconnect(follower.Url, node.Url);
                Disconnect(node.Url, follower.Url);
            }
        }

        protected void ReconnectBiDirectionalFromNode(RachisConsensus<CountingStateMachine> node)
        {
            foreach (var follower in RachisConsensuses.Where(x => x.Url != node.Url))
            {
                Reconnect(follower.Url, node.Url);
                Reconnect(node.Url, follower.Url);
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

            RavenTestHelper.AssertTrue(Task.WhenAny(waitingTasks).Wait(3000 * nodes.Count()), () => GetCandidateStatus(nodes));
            return nodes.FirstOrDefault(x => x.CurrentCommittedState.State == RachisState.Leader);
        }

        public static string GetCandidateStatus(IEnumerable<RachisConsensus<CountingStateMachine>> nodes)
        {
            var sb = new StringBuilder();
            sb.AppendLine("Waited too long for a node to become a leader but no leader was elected.");
            foreach (var node in nodes)
            {
                var candidate = node.Candidate;
                var currentCommittedState = node.CurrentCommittedState;
                if (candidate == null)
                {
                    sb.AppendLine($"'{node.Tag}' is {currentCommittedState.State} at term {currentCommittedState.Term}, current candidate is null {node.LastStateChangeReason}");
                    continue;
                }

                sb.AppendLine($"'{node.Tag}' is {currentCommittedState.State} at term {currentCommittedState.Term} (running: {candidate.Running})");
                sb.AppendJoin(Environment.NewLine, candidate.GetStatus().Values);
                sb.AppendLine();
            }

            return sb.ToString();
        }

        protected RachisConsensus<CountingStateMachine> SetupServer(bool bootstrap = false, int electionTimeout = 300, [CallerMemberName] string caller = null, bool shouldRunInMemory = true, string nodeTag = null)
        {
            var tcpListener = new TcpListener(IPAddress.Loopback, 0);
            tcpListener.Start();
            char ch;
            if (bootstrap)
            {
                ch = (char)65;
            }
            else
            {
                ch = (char)(65 + Interlocked.Increment(ref _count));
            }

            var url = $"tcp://localhost:{((IPEndPoint)tcpListener.LocalEndpoint).Port}/?{caller}#{nodeTag ?? $"{ch}"}";

            int seed = PredictableSeeds ? _random.Next(int.MaxValue) : (int)Interlocked.Read(ref _count);
            var configuration = RavenConfiguration.CreateForServer(caller);
            configuration.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType), nameof(SearchEngineType.Lucene));
            configuration.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType), nameof(SearchEngineType.Lucene));

            configuration.Initialize();
            configuration.Core.PublicServerUrl = new UriSetting($"http://localhost:{((IPEndPoint)tcpListener.LocalEndpoint).Port}");
            configuration.Cluster.ElectionTimeout = new TimeSetting(electionTimeout, TimeUnit.Milliseconds);

            configuration.Core.RunInMemory = shouldRunInMemory;
            StorageEnvironmentOptions server = null;
            if (shouldRunInMemory)
                server = StorageEnvironmentOptions.CreateMemoryOnly();
            else
            {
                string dataDirectory = NewDataPath(prefix: $"GetNewServer-{nodeTag ?? "A"}", forceCreateDir: true);
                server = StorageEnvironmentOptions.ForPath(dataDirectory);
                configuration.Core.DataDirectory = new PathSetting(dataDirectory);
            }

            var serverStore = new RavenServer(configuration) { ThrowOnLicenseActivationFailure = true }.ServerStore;

            serverStore.PreInitialize();
            serverStore.Initialize();
            var rachis = new RachisConsensus<CountingStateMachine>(serverStore, seed);
            var storageEnvironment = new StorageEnvironment(server);
            rachis.Initialize(storageEnvironment, configuration, new ClusterChanges(), configuration.Core.ServerUrls[0], new SystemTime(), out _, CancellationToken.None);
            rachis.OnDispose += (sender, args) =>
            {
                serverStore.Dispose();
                storageEnvironment.Dispose();
            };
            if (bootstrap)
            {
                rachis.Bootstrap(url, nodeTag ?? "A");
            }

            rachis.Url = url;
            _listeners.Add(tcpListener);
            RachisConsensuses.Add(rachis);
            rachis.OnDispose += (sender, args) => tcpListener.Stop();

            for (int i = 0; i < 4; i++)
            {
                AcceptConnection(tcpListener, rachis);
            }

            return rachis;
        }

        private readonly ConcurrentSet<string> _localPathsToDelete = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        protected string NewDataPath([CallerMemberName] string prefix = null, string suffix = null, bool forceCreateDir = false)
        {
            if (suffix != null)
                prefix += suffix;
            var path = RavenTestHelper.NewDataPath(prefix, 0, forceCreateDir);

            _localPathsToDelete.Add(path);

            return path;
        }

        private void AcceptConnection(TcpListener tcpListener, RachisConsensus rachis)
        {
            Task.Factory.StartNew(async () =>
            {
                TcpClient tcpClient;
                try
                {
                    tcpClient = await tcpListener.AcceptTcpClientAsync();
                    AcceptConnection(tcpListener, rachis);
                }
                catch (Exception e)
                {
                    if (rachis.IsDisposed)
                        return;

                    Assert.Fail($"Unexpected TCP listener exception{Environment.NewLine}{e}");
                    throw;
                }

                try
                {
                    var stream = tcpClient.GetStream();
                    var remoteConnection = new RemoteConnection(rachis.Tag, rachis.CurrentCommittedState.Term, stream,
                        features: new TcpConnectionHeaderMessage.SupportedFeatures(TcpConnectionHeaderMessage.ClusterTcpVersion)
                        {
                            Cluster = new TcpConnectionHeaderMessage.SupportedFeatures.ClusterFeatures
                            {
                                BaseLine = true,
                                MultiTree = true
                            }
                        }, () => tcpClient.Client?.Disconnect(false));

                    await rachis.AcceptNewConnectionAsync(remoteConnection, tcpClient.Client.RemoteEndPoint, hello =>
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
            });
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
                using (l.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (context.OpenReadTransaction())
                    index = l.GetLastEntryIndex(context);
                return Task.CompletedTask;
            });

            return index;
        }

        protected List<Task> IssueCommandsWithoutWaitingForCommits(RachisConsensus<CountingStateMachine> leader, int numberOfCommands, string name, int? value = null)
        {
            List<Task> waitingList = new List<Task>();
            for (var i = 1; i <= numberOfCommands; i++)
            {
                var task = leader.PutAsync(new TestCommand
                {
                    Name = name,
                    Value = value ?? i
                });

                waitingList.Add(task);
            }
            return waitingList;
        }

        protected async Task ActionWithLeader(Func<RachisConsensus<CountingStateMachine>, Task> action)
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
                        var leader = RachisConsensuses.Single(x => x.CurrentCommittedState.State == RachisState.Leader);
                        await action(leader);
                        return;
                    }
                }
                catch (Exception e)
                {
                    lastException = e;
                    await Task.Delay(50);
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
        private long _count;

        public override void Dispose()
        {
            base.Dispose();

            var exceptionAggregator = new ExceptionAggregator("Could not dispose test");


            foreach (var rc in RachisConsensuses)
            {
                exceptionAggregator.Execute(() =>
                {
                    rc.Dispose();
                });
            }

            foreach (var listener in _listeners)
            {
                exceptionAggregator.Execute(() =>
                {
                    listener.Stop();
                });
            }

            exceptionAggregator.Execute(() =>
            {
                foreach (var mustBeSuccessfulTask in _mustBeSuccessfulTasks)
                {
                    Assert.True(mustBeSuccessfulTask.Wait(250));
                }
            });

            RavenTestHelper.DeletePaths(_localPathsToDelete, exceptionAggregator);

            exceptionAggregator.ThrowIfNeeded();
        }

        public class CountingValidator : RachisVersionValidation
        {
            public CountingValidator(ClusterCommandsVersionManager commandsVersionManager) : base(commandsVersionManager)
            {
            }

            public override void AssertPutCommandToLeader(CommandBase cmd)
            {
            }

            public override void AssertEntryBeforeSendToFollower(BlittableJsonReaderObject entry, int version, string follower)
            {
            }
        }

        public class CountingStateMachine : RachisStateMachine
        {
            public string Read(ClusterOperationContext context, string name)
            {
                var tree = context.Transaction.InnerTransaction.ReadTree("values");
                var read = tree.Read(name);
                return read?.Reader.ToStringValue();
            }

            protected override void Apply(ClusterOperationContext context, BlittableJsonReaderObject cmd, long index, Leader leader, ServerStore serverStore)
            {
                if (cmd.TryGet("Type", out string type) == false)
                {
                    // ReSharper disable once UseNullPropagation
                    leader?.SetStateOf(index, tcs => { tcs.TrySetException(new RachisApplyException("Cannot execute command, wrong format")); });
                    return;
                }

                switch (type)
                {
                    case nameof(TestCommand):
                        Assert.True(cmd.TryGet(nameof(TestCommand.Name), out string name0));
                        Assert.True(cmd.TryGet(nameof(TestCommand.Value), out int val0));
                        var tree0 = context.Transaction.InnerTransaction.CreateTree("values");
                        var current0 = tree0.Read(name0)?.Reader.ToStringValue();
                        tree0.Add(name0, current0 + val0);
                        break;

                    case nameof(TestCommandWithLargeData):
                        Assert.True(cmd.TryGet(nameof(TestCommandWithLargeData.Name), out string name1));
                        Assert.True(cmd.TryGet(nameof(TestCommandWithLargeData.RandomData), out string randomData1));
                        var tree1 = context.Transaction.InnerTransaction.CreateTree("values");
                        tree1.Add(name1, randomData1);
                        break;

                    case nameof(TestCommandWithRaftId):
                        Assert.True(cmd.TryGet(nameof(TestCommandWithRaftId.Name), out string name2));
                        Assert.True(cmd.TryGet(nameof(TestCommandWithRaftId.Value), out int val2));
                        var tree2 = context.Transaction.InnerTransaction.CreateTree("values");
                        var current2 = tree2.Read(name2)?.Reader.ToStringValue();
                        tree2.Add(name2, current2 + val2);
                        break;
                }
            }

            protected override RachisVersionValidation InitializeValidator()
            {
                return new CountingValidator(_parent.CommandsVersionManager);
            }

            public override bool ShouldSnapshot(Slice slice, RootObjectType type)
            {
                return slice.ToString() == "values";
            }

            public override async Task<RachisConnection> ConnectToPeerAsync(string url, string tag, X509Certificate2 certificate, CancellationToken token)
            {
                TimeSpan time;
                using (ContextPoolForReadOnlyOperations.AllocateOperationContext(out ClusterOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    time = _parent.ElectionTimeout * Math.Max(_parent.GetTopology(ctx).AllNodes.Count - 2, 1);
                }

                var tcpClient = await TcpUtils.ConnectAsync(url, time, token: token);
                try
                {
                    var stream = tcpClient.GetStream();
                    var conn = new RachisConnection
                    {
                        Stream = stream,
                        SupportedFeatures = TcpConnectionHeaderMessage.GetSupportedFeaturesFor(
                            TcpConnectionHeaderMessage.OperationTypes.Cluster, TcpConnectionHeaderMessage.ClusterWithMultiTree),
                        Disconnect = () =>
                        {
                            using (tcpClient)
                            {
                                tcpClient.Client.Disconnect(false);
                            }
                        }
                    };
                    return conn;
                }
                catch
                {
                    using (tcpClient)
                    {
                        tcpClient.Client.Disconnect(false);
                    }
                    throw;
                }
            }
        }

        public class TestCommand : CommandBase
        {
            public string Name;

            public object Value;

            public override DynamicJsonValue ToJson(JsonOperationContext context)
            {
                var djv = base.ToJson(context);
                UniqueRequestId ??= Guid.NewGuid().ToString();

                djv[nameof(UniqueRequestId)] = UniqueRequestId;
                djv[nameof(Name)] = Name;
                djv[nameof(Value)] = Value;

                return djv;
            }
        }

        public class TestCommandWithLargeData : CommandBase
        {
            public string Name;

            public string RandomData = "";

            public override DynamicJsonValue ToJson(JsonOperationContext context)
            {
                var djv = base.ToJson(context);
                UniqueRequestId ??= Guid.NewGuid().ToString();

                djv[nameof(UniqueRequestId)] = UniqueRequestId;
                djv[nameof(Name)] = Name;
                djv[nameof(RandomData)] = RandomData;

                return djv;
            }
        }

        internal class TestCommandWithRaftId : CommandBase
        {
            internal string Name;

#pragma warning disable 649
            internal object Value;
#pragma warning restore 649

            public TestCommandWithRaftId(string name, string uniqueRequestId) : base(uniqueRequestId)
            {
                Name = name;
            }

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
