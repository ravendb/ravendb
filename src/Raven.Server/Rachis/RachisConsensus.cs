using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Config;
using Raven.Server.Extensions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Rachis.Remote;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Utils;
using Voron;
using Voron.Data;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Rachis
{
    public class RachisConsensus<TStateMachine> : RachisConsensus
        where TStateMachine : RachisStateMachine, new()
    {
        private readonly ServerStore _serverStore;

        public RachisConsensus(ServerStore serverStore, int? seed = null) : base(serverStore.Server.CipherSuitesPolicy, seed)
        {
            _serverStore = serverStore;
        }

        public TStateMachine StateMachine;

        internal override RachisStateMachine GetStateMachine()
        {
            return StateMachine;
        }

        internal override RachisVersionValidation Validator => StateMachine.Validator;

        public override void Notify(Notification notification)
        {
            _serverStore.NotificationCenter.Add(notification, updateExisting: false);
        }

        protected override void InitializeState(ClusterOperationContext context, ClusterChanges changes)
        {
            StateMachine = new TStateMachine();
            StateMachine.Initialize(this, context, changes);
        }

        public override void Dispose()
        {
            SetNewState(RachisState.Follower, new NullDisposable(), -1, "Disposing Rachis", asyncDispose: false);
            StateMachine?.Dispose();
            base.Dispose();
        }

        public override long Apply(ClusterOperationContext context, long uptoInclusive, Leader leader, Stopwatch duration)
        {
            return StateMachine.Apply(context, uptoInclusive, leader, _serverStore, duration);
        }

        public void EnsureNodeRemovalOnDeletion(ClusterOperationContext context, long term, string nodeTag)
        {
            StateMachine.EnsureNodeRemovalOnDeletion(context, term, nodeTag);
        }

        public override X509Certificate2 ClusterCertificate => _serverStore.Server.Certificate?.Certificate;

        public override bool ShouldSnapshot(Slice slice, RootObjectType type)
        {
            return StateMachine.ShouldSnapshot(slice, type);
        }

        public override void SnapshotInstalled(long lastIncludedIndex, bool fullSnapshot, CancellationToken token)
        {
            StateMachine.OnSnapshotInstalled(lastIncludedIndex, fullSnapshot, _serverStore, token);
        }

        public override Task<RachisConnection> ConnectToPeer(string url, string tag, X509Certificate2 certificate)
        {
            if (_serverStore.Initialized == false)
                throw new InvalidOperationException("Server store isn't initialized.");

            return StateMachine.ConnectToPeer(url, tag, certificate);
        }

        private class NullDisposable : IDisposable
        {
            public void Dispose()
            {
            }
        }

        public unsafe List<BlittableJsonReaderObject> GetLogEntries<TTransaction>(long first, TransactionOperationContext<TTransaction> context, int max)
            where TTransaction : RavenTransaction
        {
            var entries = new List<BlittableJsonReaderObject>();
            var reveredNextIndex = Bits.SwapBytes(first);
            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            using (Slice.External(context.Allocator, (byte*)&reveredNextIndex, sizeof(long), out Slice key))
            {
                foreach (var value in table.SeekByPrimaryKey(key, 0))
                {
                    var entry = FollowerAmbassador.BuildRachisEntryToSend(context, value);
                    Transaction.DebugDisposeReaderAfterTransaction(context.Transaction.InnerTransaction, entry);
                    entries.Add(entry);
                    if (entries.Count >= max)
                        break;
                }
            }
            return entries;
        }
    }

    public class RachisLogEntry : IDynamicJsonValueConvertible
    {
        public DateTime At = DateTime.UtcNow;
        public string Message;
        public long Ticks;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Message)] = Message,
                [nameof(Ticks)] = Ticks
            };
        }
    }

    public class RachisTimings
    {
        public readonly ConcurrentBag<RachisLogEntry> Timings = new ConcurrentBag<RachisLogEntry>();
    }

    public class RachisLogRecorder
    {
        private readonly ConcurrentQueue<RachisTimings> _queue;
        private readonly Stopwatch _sp = Stopwatch.StartNew();
        private RachisTimings _current;
        private ConcurrentBag<RachisLogEntry> Timings => _current.Timings;

        public RachisLogRecorder(ConcurrentQueue<RachisTimings> queue)
        {
            _queue = queue;
        }

        public void Start()
        {
            _current = new RachisTimings();
            _queue.LimitedSizeEnqueue(_current, 10);
            Timings.Add(new RachisLogEntry
            {
                Message = "Start",
                Ticks = 0
            });
            _sp.Restart();
        }

        public void Record(string message)
        {
            if (_current == null)
                return; // ignore - shutting down

            Timings.Add(new RachisLogEntry
            {
                Message = message,
                Ticks = _sp.ElapsedMilliseconds
            });
        }
    }

    public class RachisDebug
    {
        public readonly ConcurrentDictionary<string, RachisTimingsHolder> TimingTracking = new ConcurrentDictionary<string, RachisTimingsHolder>();
        public readonly ConcurrentQueue<string> StateChangeTracking = new ConcurrentQueue<string>();

        public class RachisTimingsHolder
        {
            public ConcurrentQueue<RachisTimings> TimingTracking;
            public DateTime Since = DateTime.UtcNow;
        }

        public bool IsInterVersionTest;

        public RachisLogRecorder GetNewRecorder(string name)
        {
            var holder = new RachisTimingsHolder
            {
                TimingTracking = new ConcurrentQueue<RachisTimings>()
            };

            if (TimingTracking.TryAdd(name, holder) == false)
            {
                throw new ArgumentException($"Recorder with the name '{name}' already exists");
            }
            return new RachisLogRecorder(holder.TimingTracking);
        }

        public void RemoveRecorderOlderThan(DateTime after)
        {
            if (TimingTracking.IsEmpty)
                return;

            foreach (var item in TimingTracking)
            {
                if (item.Value.Since > after)
                    continue;
                TimingTracking.TryRemove(item.Key, out _);
            }
        }

        public void RemoveRecorder(string name)
        {
            if (TimingTracking.Remove(name, out var q))
            {
                q.TimingTracking.Clear();
            }
        }

        public DynamicJsonValue ToJson()
        {
            var timingTracking = new DynamicJsonValue();
            foreach (var tuple in TimingTracking.ForceEnumerateInThreadSafeManner().OrderBy(x => x.Key))
            {
                var key = tuple.Key;
                DynamicJsonArray inner;
                timingTracking[key] = inner = new DynamicJsonArray();
                foreach (var queue in tuple.Value.TimingTracking)
                {
                    inner.Add(new DynamicJsonArray(queue.Timings.OrderBy(x => x.At)));
                }
            }

            var stateTracking = new DynamicJsonArray(StateChangeTracking);

            return new DynamicJsonValue
            {
                [nameof(TimingTracking)] = timingTracking,
                [nameof(StateChangeTracking)] = stateTracking
            };
        }
    }

    public abstract class RachisConsensus : IDisposable
    {
        internal abstract RachisStateMachine GetStateMachine();

        internal abstract RachisVersionValidation Validator { get; }

        public const string InitialTag = "?";

        public readonly RachisDebug InMemoryDebug = new RachisDebug();

        public RachisState CurrentState
        {
            get => _currentState;
            private set => _currentState = value;
        }

        public string LastStateChangeReason
        {
            get { return _lastStateChangeReason; }
            private set
            {
                _lastStateChangeReason = value;
                _lastStateChangeTime = DateTime.UtcNow;
            }
        }

        public DateTime LastStateChangeTime => _lastStateChangeTime;

        public event EventHandler<ClusterTopology> TopologyChanged;

        public event EventHandler<StateTransition> StateChanged;

        public event EventHandler LeaderElected;

        public Action<ClusterOperationContext> SwitchToSingleLeaderAction;

        public Action<ClusterOperationContext, CommandBase> BeforeAppendToRaftLog;

        private string _tag;
        private string _clusterId;

        public ClusterContextPool ContextPool { get; private set; }
        private StorageEnvironment _persistentState;
        internal Logger Log;

        private readonly List<IDisposable> _disposables = new List<IDisposable>();

        public long CurrentTerm { get; private set; }
        public string Tag => _tag;
        public string ClusterId => _clusterId;
        public string ClusterBase64Id => _clusterIdBase64Id;
        public string Url;

        private static readonly Slice GlobalStateSlice;
        private static readonly Slice CurrentTermSlice;
        private static readonly Slice VotedForSlice;
        private static readonly Slice LastCommitSlice;
        private static readonly Slice LastTruncatedSlice;
        private static readonly Slice TopologySlice;
        private static readonly Slice TagSlice;
        private static readonly Slice PreviousTagSlice;
        private static readonly Slice SnapshotRequestSlice;
        internal static readonly Slice EntriesSlice;

        internal static readonly TableSchema LogsTable;
        public readonly RachisLogHistory LogHistory;

        static RachisConsensus()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "GlobalState", out GlobalStateSlice);
                Slice.From(ctx, "Tag", out TagSlice);
                Slice.From(ctx, "PreviousTag", out PreviousTagSlice);
                Slice.From(ctx, "CurrentTerm", out CurrentTermSlice);
                Slice.From(ctx, "VotedFor", out VotedForSlice);
                Slice.From(ctx, "LastCommit", out LastCommitSlice);
                Slice.From(ctx, "Topology", out TopologySlice);
                Slice.From(ctx, "LastTruncated", out LastTruncatedSlice);
                Slice.From(ctx, "Entries", out EntriesSlice);
                Slice.From(ctx, "SnapshotRequest", out SnapshotRequestSlice);
            }
            /*

            index - int64 big endian
            term  - int64 little endian
            entry - blittable value
            flags - cmd, no op, topology, etc

             */
            LogsTable = new TableSchema();
            LogsTable.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0
            });
        }

        public TimeSpan ElectionTimeout
        {
            get => _electionTimeout;
            private set => _electionTimeout = value;
        }

        public TimeSpan TcpConnectionTimeout
        {
            get => _tcpConnectionTimeout;
            private set => _tcpConnectionTimeout = value;
        }

        public TimeoutEvent Timeout { get; private set; }

        public TimeSpan OperationTimeout
        {
            get => _operationTimeout;
            private set => _operationTimeout = value;
        }

        public int? MaximalVersion { get; set; }

        private Leader _currentLeader;
        public Leader CurrentLeader => _currentLeader;
        private TaskCompletionSource<object> _topologyChanged = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        private TaskCompletionSource<object> _stateChanged = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        private TaskCompletionSource<object> _commitIndexChanged = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ManualResetEventSlim _disposeEvent = new ManualResetEventSlim();
        private readonly Random _rand;
        private string _lastStateChangeReason;
        public Candidate Candidate { get; private set; }

        protected RachisConsensus(CipherSuitesPolicy cipherSuitesPolicy, int? seed = null)
        {
            CipherSuitesPolicy = cipherSuitesPolicy;
            _rand = seed.HasValue ? new Random(seed.Value) : new Random();
            LogHistory = new RachisLogHistory();
        }

        public abstract void Notify(Notification notification);

        public void RandomizeTimeout(bool extend = false)
        {
            //We want to be able to reproduce rare issues that are related to timing
            var timeout = (int)ElectionTimeout.TotalMilliseconds;
            if (extend)
                timeout = Math.Max(timeout, timeout * 2); // avoid overflow

            Timeout.TimeoutPeriod = _rand.Next(timeout / 3 * 2, timeout);
        }

        public unsafe void Initialize(StorageEnvironment env, RavenConfiguration configuration, ClusterChanges changes, string myUrl, out long clusterTopologyEtag)
        {
            try
            {
                _persistentState = env;

                OperationTimeout = configuration.Cluster.OperationTimeout.AsTimeSpan;
                ElectionTimeout = configuration.Cluster.ElectionTimeout.AsTimeSpan;
                TcpConnectionTimeout = configuration.Cluster.TcpConnectionTimeout.AsTimeSpan;
                MaximalVersion = configuration.Cluster.MaximalAllowedClusterVersion;

                DebuggerAttachedTimeout.LongTimespanIfDebugging(ref _operationTimeout);
                DebuggerAttachedTimeout.LongTimespanIfDebugging(ref _electionTimeout);

                ContextPool = new ClusterContextPool(changes, _persistentState, configuration.Memory.MaxContextSizeToKeep);

                ClusterTopology topology;
                using (ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    _tag = ReadNodeTag(context);

                    RequestSnapshot = GetSnapshotRequest(context);

                    Log = LoggingSource.Instance.GetLogger<RachisConsensus>(_tag);
                    LogsTable.Create(tx.InnerTransaction, EntriesSlice, 16);

                    CurrentTerm = ReadTerm(context);

                    topology = GetTopology(context);
                    if (topology.AllNodes.Count == 1 && topology.Members.Count == 1)
                    {
                        if (topology.GetUrlFromTag(_tag) != myUrl)
                        {
                            topology.Members.Remove(_tag);
                            topology.Members.Add(_tag, configuration.Core.GetNodeHttpServerUrl(myUrl));
                            SetTopology(this, context, topology);
                        }
                    }

                    clusterTopologyEtag = topology.Etag;
                    _clusterId = topology.TopologyId;
                    SetClusterBase(_clusterId);

                    InitializeState(context, changes);

                    LogHistory.Initialize(tx, configuration, Log);

                    tx.Commit();
                }

                Timeout = new TimeoutEvent(0, "Consensus");
                RandomizeTimeout();

                // if we don't have a topology id, then we are passive
                // an admin needs to let us know that it is fine, either
                // by explicit bootstrapping or by connecting us to a cluster
                if (topology.TopologyId == null ||
                    topology.Contains(_tag) == false)
                {
                    CurrentState = RachisState.Passive;
                    return;
                }

                CurrentState = RachisState.Follower;
                Timeout.Start(SwitchToCandidateStateOnTimeout);
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        private unsafe long ReadTerm(ClusterOperationContext context)
        {
            var state = context.Transaction.InnerTransaction.CreateTree(GlobalStateSlice);

            var read = state.Read(CurrentTermSlice);
            if (read == null || read.Reader.Length != sizeof(long))
            {
                using (state.DirectAdd(CurrentTermSlice, sizeof(long), out byte* ptr))
                    *(long*)ptr = 0;

                return 0L;
            }

            return read.Reader.ReadLittleEndianInt64();
        }

        public string ReadNodeTag(ClusterOperationContext context)
        {
            var state = context.Transaction.InnerTransaction.CreateTree(GlobalStateSlice);

            var readResult = state.Read(TagSlice);
            return readResult == null ? InitialTag : readResult.Reader.ToStringValue();
        }

        public string ReadPreviousNodeTag(ClusterOperationContext context)
        {
            var state = context.Transaction.InnerTransaction.CreateTree(GlobalStateSlice);

            var readResult = state.Read(PreviousTagSlice);
            return readResult?.Reader.ToStringValue();
        }

        private void SwitchToSingleLeader(ClusterOperationContext context)
        {
            var electionTerm = CurrentTerm + 1;
            CastVoteInTerm(context, electionTerm, Tag, "Switching to single leader");

            if (Log.IsInfoEnabled)
            {
                Log.Info("Switching to leader state");
            }
            var leader = new Leader(this, electionTerm);
            SetNewStateInTx(context, RachisState.LeaderElect, leader, electionTerm, "I'm the only one in the cluster, so I'm the leader", () => _currentLeader = leader);
            SwitchToSingleLeaderAction?.Invoke(context);

            Candidate = null;
            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += tx =>
            {
                if (tx is LowLevelTransaction llt && llt.Committed)
                {
                    leader.Start();
                }
            };
        }

        public bool ContainsCommandId(string guid)
        {
            using (ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                return LogHistory.ContainsCommandId(ctx, guid);
            }
        }

        protected abstract void InitializeState(ClusterOperationContext context, ClusterChanges changes);

        public async Task WaitForState(RachisState rachisState, CancellationToken token)
        {
            while (token.IsCancellationRequested == false)
            {
                // we setup the wait _before_ checking the state
                var task = _stateChanged.Task.WithCancellation(token);

                if (CurrentState == rachisState)
                    return;

                await task;
            }
        }

        public async Task WaitForLeaderChange(CancellationToken cts)
        {
            var currentLeader = LeaderTag;
            while (cts.IsCancellationRequested == false)
            {
                // we setup the wait _before_ checking the state
                var task = _stateChanged.Task.WithCancellation(cts);

                if (currentLeader != GetLeaderTag(safe: true))
                    return;

                await task;
            }
        }

        public async Task WaitForLeaveState(RachisState rachisState, CancellationToken cts)
        {
            while (cts.IsCancellationRequested == false)
            {
                // we setup the wait _before_ checking the state
                var task = _stateChanged.Task.WithCancellation(cts);

                if (CurrentState != rachisState)
                    return;

                await task;
            }
        }

        public Task GetTopologyChanged()
        {
            return _topologyChanged.Task;
        }

        public async Task WaitForTopology(Leader.TopologyModification modification, string nodeTag = null, CancellationToken token = default)
        {
            while (true)
            {
                var task = _topologyChanged.Task;
                var tag = nodeTag ?? _tag;
                using (ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var clusterTopology = GetTopology(context);
                    switch (modification)
                    {
                        case Leader.TopologyModification.Voter:
                            if (clusterTopology.Members.ContainsKey(tag))
                                return;
                            break;

                        case Leader.TopologyModification.Promotable:
                            if (clusterTopology.Promotables.ContainsKey(tag))
                                return;
                            break;

                        case Leader.TopologyModification.NonVoter:
                            if (clusterTopology.Watchers.ContainsKey(tag))
                                return;
                            break;

                        case Leader.TopologyModification.Remove:
                            if (clusterTopology.Members.ContainsKey(tag) == false &&
                                clusterTopology.Promotables.ContainsKey(tag) == false &&
                                clusterTopology.Watchers.ContainsKey(tag) == false)
                                return;
                            break;

                        default:
                            throw new ArgumentOutOfRangeException(nameof(modification), modification, null);
                    }
                }

                await task.WithCancellation(token);
            }
        }

        public enum CommitIndexModification
        {
            GreaterOrEqual,
            AnyChange
        }

        public void SetNewState(RachisState rachisState, IDisposable disposable, long expectedTerm, string stateChangedReason, Action beforeStateChangedEvent = null, bool asyncDispose = true)
        {
            using (ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenWriteTransaction()) // we use the write transaction lock here
            {
                SetNewStateInTx(context, rachisState, disposable, expectedTerm, stateChangedReason, beforeStateChangedEvent, asyncDispose);
                context.Transaction.Commit();
            }
            _leadershipTimeChanged.SetAndResetAtomically();
        }

        public class StateTransition
        {
            public RachisState From;
            public RachisState To;
            public string Reason;
            public long CurrentTerm;
            public DateTime When;

            public override string ToString()
            {
                return $"{When:u} {Reason} {From}->{To} at term {CurrentTerm:#,#;;0}";
            }
        }

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff();
        }

        internal class TestingStuff
        {
            internal ManualResetEventSlim Mre = new ManualResetEventSlim(false);

            public void OnLeaderElect()
            {
                if (Mre == null)
                    return;

                Mre.Reset();
                if (Mre.Wait(TimeSpan.FromSeconds(60)) == false)
                {
                    throw new TimeoutException("Something is wrong, throwing to avoid hanging");
                }
                Mre.Reset();
            }

            public void BeforeCastingForRealElection()
            {
                Mre?.Set();
            }

            public void LeaderDispose()
            {
                Mre?.Set();
            }

            public void BeforeNegotiatingWithFollower()
            {
                if (Mre?.Wait(TimeSpan.FromSeconds(60)) == false)
                {
                    throw new TimeoutException("Something is wrong, throwing to avoid hanging");
                }
            }
        }

        internal void SetNewStateInTx(ClusterOperationContext context,
            RachisState rachisState,
            IDisposable parent,
            long expectedTerm,
            string stateChangedReason,
            Action beforeStateChangedEvent = null,
            bool disposeAsync = true)
        {
            if (expectedTerm != CurrentTerm && expectedTerm != -1)
                RachisConcurrencyException.Throw($"Attempted to switch state to {rachisState} on expected term {expectedTerm:#,#;;0} but the real term is {CurrentTerm:#,#;;0}");

            if (rachisState == RachisState.LeaderElect || rachisState == RachisState.Leader)
            {
                if (GetSnapshotRequest(context))
                    throw new RachisInvalidOperationException("We cannot be elected for leadership if snapshot is requested.");
            }

            if (rachisState == RachisState.LeaderElect)
            {
                var noopCmd = new DynamicJsonValue
                {
                    ["Type"] = $"Noop for {Tag} in term {expectedTerm}",
                    ["Command"] = "noop",
                    [nameof(CommandBase.UniqueRequestId)] = Guid.NewGuid().ToString()
                };

                ForTestingPurposes?.OnLeaderElect();
                InsertToLeaderLog(context, expectedTerm, context.ReadObject(noopCmd, "noop-cmd"), RachisEntryFlags.Noop);
            }

            var sp = Stopwatch.StartNew();

            _currentLeader = null;
            LastStateChangeReason = stateChangedReason;
            var toDispose = new List<IDisposable>(_disposables);
            _disposables.Clear();

            if (parent != null)
            {
                _disposables.Add(parent);
            }
            else if (rachisState != RachisState.Passive)
            {
                // if we are back to null state, wait to become candidate if no one talks to us
                Timeout.Start(SwitchToCandidateStateOnTimeout);
            }

            if (rachisState == RachisState.Passive)
            {
                DeleteTopology(context);
            }

            var transition = new StateTransition
            {
                CurrentTerm = expectedTerm,
                From = CurrentState,
                To = rachisState,
                Reason = stateChangedReason,
                When = DateTime.UtcNow
            };

            PrevStates.LimitedSizeEnqueue(transition, 5);

            context.Transaction.InnerTransaction.LowLevelTransaction.AfterCommitWhenNewReadTransactionsPrevented +=
                _ => CurrentState = rachisState; //  we need this to happened while we still under the write lock

            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += tx =>
            {
                if (tx is LowLevelTransaction llt && llt.Committed)
                {
                    try
                    {
                        beforeStateChangedEvent?.Invoke();
                    }
                    catch (Exception e)
                    {
                        if (Log.IsInfoEnabled)
                        {
                            Log.Info("Before state change invocation function failed.", e);
                        }
                    }

                    try
                    {
                        StateChanged?.Invoke(this, transition);
                    }
                    catch (Exception e)
                    {
                        if (Log.IsInfoEnabled)
                        {
                            Log.Info("State change invocation function failed.", e);
                        }
                    }

                    if (disposeAsync)
                    {
                        TaskExecutor.CompleteReplaceAndExecute(ref _stateChanged, () =>
                        {
                            if (Log.IsInfoEnabled)
                            {
                                Log.Info($"Initiate disposing the term _prior_ to {expectedTerm:#,#;;0} with {toDispose.Count} things to dispose.");
                            }

                            ParallelDispose(toDispose);
                        });
                    }
                    else
                    {
                        ParallelDispose(toDispose);
                        TaskExecutor.CompleteAndReplace(ref _stateChanged);
                    }

                    var elapsed = sp.Elapsed;
                    if (elapsed > ElectionTimeout / 2)
                    {
                        if (Log.IsOperationsEnabled)
                        {
                            Log.Operations($"Took way too much time ({elapsed}) to change the state to {rachisState} in term {expectedTerm:#,#;;0}. (Election timeout:{ElectionTimeout})");
                        }
                    }
                }
            };
        }

        private void ParallelDispose(List<IDisposable> toDispose)
        {
            if (toDispose == null || toDispose.Count == 0)
                return;

            Parallel.ForEach(toDispose, d =>
            {
                try
                {
                    d.Dispose();
                }
                catch (ObjectDisposedException)
                {
                    // nothing to do
                }
                catch (Exception e)
                {
                    if (Log.IsInfoEnabled)
                    {
                        Log.Info("Failed to dispose during new rachis state transition", e);
                    }
                }
            });
        }

        public ConcurrentQueue<StateTransition> PrevStates { get; set; } = new ConcurrentQueue<StateTransition>();

        public bool TakeOffice()
        {
            if (CurrentState != RachisState.LeaderElect)
                return false;

            CurrentState = RachisState.Leader;
            TaskExecutor.CompleteAndReplace(ref _stateChanged);
            return true;
        }

        public void AppendStateDisposable(IDisposable parentState, IDisposable disposeOnStateChange)
        {
            using (ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenWriteTransaction()) // using write tx just for the lock here
            {
                if (_disposables.Count == 0 || ReferenceEquals(_disposables[0], parentState) == false)
                    throw new ConcurrencyException(
                        "Could not set the disposeOnStateChange because by the time we did it the parent state has changed");
                _disposables.Add(disposeOnStateChange);
            }
        }

        public void RemoveAndDispose(IDisposable parentState, IDisposable disposable)
        {
            if (disposable == null)
                return;

            using (ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenWriteTransaction()) // using write tx just for the lock here
            using (disposable)
            {
                if (_disposables.Count == 0 || ReferenceEquals(_disposables[0], parentState) == false)
                    throw new RachisConcurrencyException(
                        "Could not remove the disposable because by the time we did it the parent state has changed");
                _disposables.Remove(disposable);
            }
        }

        public void SwitchToLeaderState(long electionTerm, int version, string reason, Dictionary<string, RemoteConnection> connections = null)
        {
            if (Log.IsInfoEnabled)
            {
                Log.Info("Switching to leader state");
            }
            var leader = new Leader(this, electionTerm);
            SetNewState(RachisState.LeaderElect, leader, electionTerm, reason, () =>
            {
                ClusterCommandsVersionManager.SetClusterVersion(version);
                _currentLeader = leader;
            });
            leader.Start(connections);
        }

        public Task<(long Index, object Result)> PutAsync(CommandBase cmd)
        {
            var leader = _currentLeader;
            if (leader == null)
                throw new NotLeadingException("Not a leader, cannot accept commands. " + _lastStateChangeReason);

            Validator.AssertPutCommandToLeader(cmd);
            return leader.PutAsync(cmd, OperationTimeout);
        }

        public void SwitchToCandidateStateOnTimeout()
        {
            SwitchToCandidateState("Election timeout");
        }

        public void SwitchToCandidateState(string reason, bool forced = false)
        {
            var currentTerm = CurrentTerm;
            try
            {
                Timeout.DisableTimeout();
                using (ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (var ctx = context.OpenWriteTransaction())
                {
                    var clusterTopology = GetTopology(context);
                    if (clusterTopology.TopologyId == null ||
                        clusterTopology.AllNodes.ContainsKey(_tag) == false)
                    {
                        if (Log.IsInfoEnabled)
                        {
                            Log.Info($"We are not a part of the cluster so moving to passive (candidate because: {reason})");
                        }

                        SetNewStateInTx(context, RachisState.Passive, null, currentTerm, "We are not a part of the cluster so moving to passive");
                        ctx.Commit();
                        return;
                    }
                    if (clusterTopology.Members.ContainsKey(_tag) == false)
                    {
                        if (Log.IsInfoEnabled)
                        {
                            Log.Info($"Candidate because: {reason}, but while we are part of the cluster, we aren't a member, so we can't be a candidate.");
                        }
                        // we aren't a member, nothing that we can do here
                        return;
                    }
                    if (clusterTopology.AllNodes.Count == 1 &&
                        clusterTopology.Members.Count == 1)
                    {
                        if (Log.IsInfoEnabled)
                        {
                            Log.Info("Trying to switch to candidate when I'm the only node in the cluster, turning into a leader, instead");
                        }

                        SwitchToSingleLeader(context);
                        ctx.Commit();
                        return;
                    }
                }

                if (Log.IsInfoEnabled)
                {
                    Log.Info($"Switching to candidate state because {reason} forced: {forced}");
                }
                var candidate = new Candidate(this)
                {
                    IsForcedElection = forced
                };

                Candidate = candidate;
                SetNewState(RachisState.Candidate, candidate, currentTerm, reason);
                candidate.Start();
            }
            catch (Exception e)
            {
                if (Log.IsInfoEnabled)
                {
                    Log.Info($"An error occurred during switching to candidate state in term {currentTerm:#,#;;0}.", e);
                }
                Timeout.Start(SwitchToCandidateStateOnTimeout);
            }
        }

        public void DeleteTopology(ClusterOperationContext context)
        {
            var topology = GetTopology(context);
            var newTopology = new ClusterTopology(
                topology.TopologyId,
                new Dictionary<string, string>(),
                new Dictionary<string, string>(),
                new Dictionary<string, string>(),
                topology.LastNodeId,
                -1
            );
            SetTopology(context, newTopology);
        }

        public unsafe ClusterTopology GetTopology<TTransaction>(TransactionOperationContext<TTransaction> context)
            where TTransaction : RavenTransaction
        {
            Debug.Assert(context.Transaction != null);
            var state = context.Transaction.InnerTransaction.ReadTree(GlobalStateSlice);
            var read = state.Read(TopologySlice);
            if (read == null)
            {
                return new ClusterTopology(
                    null,
                    new Dictionary<string, string>(),
                    new Dictionary<string, string>(),
                    new Dictionary<string, string>(),
                    "",
                    -1
                );
            }

            var json = new BlittableJsonReaderObject(read.Reader.Base, read.Reader.Length, context);
            return JsonDeserializationRachis<ClusterTopology>.Deserialize(json);
        }

        public unsafe BlittableJsonReaderObject GetTopologyRaw(ClusterOperationContext context)
        {
            Debug.Assert(context.Transaction != null);
            var state = context.Transaction.InnerTransaction.ReadTree(GlobalStateSlice);
            var read = state.Read(TopologySlice);
            if (read == null)
                return null;

            BlittableJsonReaderObject topologyBlittable = new BlittableJsonReaderObject(read.Reader.Base, read.Reader.Length, context);

            Transaction.DebugDisposeReaderAfterTransaction(context.Transaction.InnerTransaction, topologyBlittable);

            return topologyBlittable;
        }

        public BlittableJsonReaderObject SetTopology(ClusterOperationContext context, ClusterTopology topology)
        {
            Debug.Assert(context.Transaction != null);
            var topologyJson = SetTopology(this, context, topology);
            _clusterId = topology.TopologyId;
            SetClusterBase(_clusterId);
            return topologyJson;
        }

        public static unsafe BlittableJsonReaderObject SetTopology(RachisConsensus engine, ClusterOperationContext context,
            ClusterTopology clusterTopology)
        {
            var topologyJson = context.ReadObject(clusterTopology.ToJson(), "topology");
            var state = context.Transaction.InnerTransaction.CreateTree(GlobalStateSlice);
            using (state.DirectAdd(TopologySlice, topologyJson.Size, out byte* ptr))
            {
                topologyJson.CopyTo(ptr);
            }

            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += _ =>
            {
                clusterTopology.AllNodes.TryGetValue(engine.Tag, out var key);
                engine.Url = key;
                TaskExecutor.CompleteAndReplace(ref engine._topologyChanged);
                engine.TopologyChanged?.Invoke(engine, clusterTopology);
            };

            Transaction.DebugDisposeReaderAfterTransaction(context.Transaction.InnerTransaction, topologyJson);

            return topologyJson;
        }

        public void NotifyTopologyChange(bool propagateError = false)
        {
            try
            {
                if (IsDisposed)
                    return;

                using (ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    TopologyChanged?.Invoke(this, GetTopology(ctx));
                }
            }
            catch (Exception)
            {
                if (propagateError)
                    throw;
            }
        }

        /// <summary>
        /// This method is expected to run for a long time (lifetime of the connection)
        /// and can never throw. We expect this to be on a separate thread
        /// </summary>
        public void AcceptNewConnection(Stream stream, Action disconnect, EndPoint remoteEndpoint, Action<RachisHello> sayHello = null)
        {
            RemoteConnection remoteConnection = null;
            try
            {
                remoteConnection = new RemoteConnection(_tag, CurrentTerm, stream, disconnect);
                try
                {
                    RachisHello initialMessage;
                    ClusterTopology clusterTopology;
                    using (ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                    {
                        initialMessage = remoteConnection.InitFollower(context);

                        using (context.OpenReadTransaction())
                        {
                            clusterTopology = GetTopology(context);
                        }

                        ValidateCompatibility(initialMessage, clusterTopology);
                        ValidateElectionTimeout(initialMessage);

                        if (clusterTopology.TopologyId == initialMessage.TopologyId && initialMessage.DebugSourceIdentifier == _tag)
                        {
                            throw new TopologyMismatchException($"Connection from ({remoteEndpoint}_ with the same topology id and tag {_tag}. " +
                                                                "It is possible that you have DNS or routing issues that cause multiple URLs to go to the same node." +
                                                                $"Connection from {initialMessage.SourceUrl} and attempted to connect to {initialMessage.DestinationUrl}");
                        }

                        sayHello?.Invoke(initialMessage);
                    }

                    if (initialMessage.TopologyId != clusterTopology.TopologyId &&
                        string.IsNullOrEmpty(clusterTopology.TopologyId) == false)
                    {
                        throw new TopologyMismatchException(
                            $"Node {initialMessage.DebugSourceIdentifier} attempted to connect to us with topology id {initialMessage.TopologyId} but our topology id is already set ({clusterTopology.TopologyId}). " +
                            "Rejecting connection from outside our cluster, this is likely an old server trying to connect to us.");
                    }
                    if (_tag == InitialTag)
                    {
                        using (ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                        using (context.OpenWriteTransaction())
                        {
                            if (_tag == InitialTag)// double checked locking under tx write lock
                            {
                                UpdateNodeTag(context, initialMessage.DebugDestinationIdentifier);
                                context.Transaction.Commit();
                            }
                        }

                        if (_tag != initialMessage.DebugDestinationIdentifier)
                        {
                            throw new TopologyMismatchException(
                                $"Node {initialMessage.DebugSourceIdentifier} attempted to connect to us with tag {initialMessage.DebugDestinationIdentifier} but our tag is already set ({_tag}). " +
                                "Rejecting connection from confused server, this is likely an old server trying to connect to us, or bad network configuration.");
                        }
                    }
                    _clusterId = initialMessage.TopologyId;
                    SetClusterBase(_clusterId);

                    switch (initialMessage.InitialMessageType)
                    {
                        case InitialMessageType.RequestVote:
                            var elector = new Elector(this, remoteConnection);
                            elector.Run();
                            break;

                        case InitialMessageType.AppendEntries:
                            if (Follower.CheckIfValidLeader(this, remoteConnection, out var negotiation))
                            {
                                var follower = new Follower(this, negotiation.Term, remoteConnection);
                                follower.AcceptConnection(negotiation);
                            }
                            break;

                        default:
                            throw new ArgumentOutOfRangeException("Unknown initial message value: " +
                                                                  initialMessage.InitialMessageType +
                                                                  ", no idea how to handle it");
                    }
                }
                catch (Exception e)
                {
                    try
                    {
                        using (ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                            remoteConnection.Send(context, e);
                    }
                    catch
                    {
                        // errors here do not matter
                    }
                    throw;
                }
            }
            catch (Exception e)
            {
                if (Log.IsInfoEnabled)
                {
                    Log.Info("Failed to process incoming connection", e);
                }

                try
                {
                    remoteConnection?.Dispose();
                }
                catch (Exception)
                {
                    // ignored
                }

                try
                {
                    stream?.Dispose();
                }
                catch (Exception)
                {
                    // ignored
                }

                throw;
            }
        }

        private void ValidateCompatibility(RachisHello initialMessage, ClusterTopology clusterTopology)
        {
            var version = initialMessage.ServerBuildVersion;
            if (ServerVersion.IsNightlyOrDev(version))
                return;

            if (clusterTopology.Promotables.ContainsKey(Tag))
            {
                if (version >= 40_000 && version < 42_000)
                    throw new NotSupportedException($"You cannot add a new node in version {ServerVersion.FullVersion} to a pre 4.2 cluster, " +
                                                    "in order to add this node you should upgrade your cluster first.");
            }
        }

        private void ValidateElectionTimeout(RachisHello initialMessage)
        {
            if (Debugger.IsAttached)
                return; // don't check here
            var max = ElectionTimeout.TotalMilliseconds * 1.1;
            var min = ElectionTimeout.TotalMilliseconds * 0.9;
            var rcvdTimeout = initialMessage.ElectionTimeout;
            if (rcvdTimeout < min || rcvdTimeout > max)
            {
                throw new InvalidOperationException(
                    $"Cannot accept the connection '{initialMessage.InitialMessageType}' from {initialMessage.DebugSourceIdentifier} because his election timeout of {rcvdTimeout} deviates more than 10% from ours {(int)ElectionTimeout.TotalMilliseconds}.");
            }
        }

        public unsafe long InsertToLeaderLog(ClusterOperationContext context, long term, BlittableJsonReaderObject cmd,
            RachisEntryFlags flags)
        {
            Debug.Assert(context.Transaction != null);

            ValidateTerm(term);

            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);

            long lastIndex;

            if (table.SeekOnePrimaryKey(Slices.AfterAllKeys, out TableValueReader reader))
            {
                lastIndex = Bits.SwapBytes(*(long*)reader.Read(0, out int size));
                Debug.Assert(size == sizeof(long));
            }
            else
            {
                GetLastTruncated(context, out lastIndex, out long _);
            }
            lastIndex += 1;
            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(Bits.SwapBytes(lastIndex));
                tvb.Add(term);
                tvb.Add(cmd.BasePointer, cmd.Size);
                tvb.Add((int)flags);
                table.Insert(tvb);
            }

            LogHistory.InsertHistoryLog(context, lastIndex, term, cmd);

            return lastIndex;
        }

        public unsafe void ClearLogEntriesAndSetLastTruncate(ClusterOperationContext context, long index, long term)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            var removed = 0;
            while (true)
            {
                if (table.SeekOnePrimaryKey(Slices.BeforeAllKeys, out TableValueReader reader) == false)
                    break;

                table.Delete(reader.Id);

                removed++;
                if (removed % 1024 == 0)
                    Timeout.Defer(LeaderTag);
            }
            var state = context.Transaction.InnerTransaction.CreateTree(GlobalStateSlice);
            using (state.DirectAdd(LastTruncatedSlice, sizeof(long) * 2, out byte* ptr))
            {
                var data = (long*)ptr;
                data[0] = index;
                data[1] = term;
            }
        }

        public unsafe void TruncateLogBefore(ClusterOperationContext context, long upto)
        {
            GetLastCommitIndex(context, out long lastIndex, out long lastTerm);

            long entryTerm;
            long entryIndex;

            if (lastIndex < upto)
            {
                upto = lastIndex; // max we can delete
                entryIndex = lastIndex;
                entryTerm = lastTerm;
            }
            else
            {
                var maybeTerm = GetTermFor(context, upto);
                if (maybeTerm == null)
                    return;
                entryIndex = upto;
                entryTerm = maybeTerm.Value;
            }

            GetLastTruncated(context, out lastIndex, out lastTerm);

            if (lastIndex >= upto)
                return;

            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);

            var truncatedIndex = entryIndex;
            var sp = Stopwatch.StartNew();
            while (true)
            {
                if (table.SeekOnePrimaryKey(Slices.BeforeAllKeys, out TableValueReader reader) == false)
                    break;

                entryIndex = Bits.SwapBytes(*(long*)reader.Read(0, out int size));
                if (entryIndex > upto)
                    break;

                Debug.Assert(size == sizeof(long));
                entryTerm = *(long*)reader.Read(1, out size);
                Debug.Assert(size == sizeof(long));

                table.Delete(reader.Id);
                truncatedIndex = entryIndex;

                if (truncatedIndex % 1024 == 0 &&
                    sp.ElapsedMilliseconds > (int)ElectionTimeout.TotalMilliseconds / 3)
                {
                    Timeout.Defer(LeaderTag);
                    break;
                }
            }

            var state = context.Transaction.InnerTransaction.CreateTree(GlobalStateSlice);
            using (state.DirectAdd(LastTruncatedSlice, sizeof(long) * 2, out byte* ptr))
            {
                var data = (long*)ptr;
                data[0] = truncatedIndex;
                data[1] = entryTerm;
            }
        }

        public unsafe (BlittableJsonReaderObject LastTopology, long LastTopologyIndex) AppendToLog(ClusterOperationContext context,
            List<RachisEntry> entries)
        {
            Debug.Assert(entries.Count > 0);
            Debug.Assert(context.Transaction != null);
            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);

            long reversedEntryIndex = -1;
            long lastTopologyIndex = -1;
            BlittableJsonReaderObject lastTopology = null;

            using (Slice.External(context.Transaction.InnerTransaction.Allocator, (byte*)&reversedEntryIndex, sizeof(long), out Slice key))
            {
                var lastEntryIndex = GetLastEntryIndex(context);
                GetLastCommitIndex(context, out var lastCommitIndex, out var lastCommitTerm);
                var firstIndexInEntriesThatWeHaveNotSeen = 0;
                foreach (var entry in entries)
                {
                    var entryTerm = GetTermFor(context, entry.Index);
                    if (entryTerm != null && entry.Term != entryTerm)
                    {
                        //rewind entries with mismatched term
                        lastEntryIndex = Math.Min(entry.Index - 1, lastEntryIndex);
                        if (Log.IsInfoEnabled)
                        {
                            Log.Info($"Got an entry with index={entry.Index:#,#;;0} and term={entry.Term:#,#;;0} while our term for that index is {entryTerm:#,#;;0}," +
                                     $"will rewind last entry index to {lastEntryIndex:#,#;;0}");
                        }
                        break;
                    }
                    if (entry.Index > lastEntryIndex)
                        break;

                    firstIndexInEntriesThatWeHaveNotSeen++;
                }

                if (firstIndexInEntriesThatWeHaveNotSeen >= entries.Count)
                    return (null, lastTopologyIndex); // we have all of those entries in our log, so we can safely ignore them

                var firstEntry = entries[firstIndexInEntriesThatWeHaveNotSeen];
                //While we do support the case where we get the same entries, we expect them to have the same index/term up to the commit index.
                if (firstEntry.Index <= lastCommitIndex)
                {
                    ThrowFatalError(firstEntry, GetTermFor(context, firstEntry.Index), lastCommitIndex, lastCommitTerm);
                }
                var prevIndex = lastEntryIndex;

                for (var index = firstIndexInEntriesThatWeHaveNotSeen; index < entries.Count; index++)
                {
                    var entry = entries[index];
                    if (entry.Index != prevIndex + 1)
                    {
                        RachisInvalidOperationException.Throw($"Gap in the entries, prev was {prevIndex:#,#;;0} but now trying {entry.Index:#,#;;0}");
                    }

                    prevIndex = entry.Index;
                    //In the case where we delete entries reversedEntryIndex will advance forward so we need to keep the origin index.
                    var originalReversedIndex = reversedEntryIndex = Bits.SwapBytes(entry.Index);
                    if (table.ReadByKey(key, out TableValueReader reader)) // already exists
                    {
                        var term = *(long*)reader.Read(1, out int size);
                        Debug.Assert(size == sizeof(long));
                        if (term == entry.Term)
                            continue; // same, can skip

                        // we have found a divergence in the log, and we now need to truncate it from this
                        // location forward
                        do
                        {
                            table.Delete(reader.Id);
                            // move to the next id, have to swap to little endian & back to get proper
                            // behavior
                            reversedEntryIndex = Bits.SwapBytes(Bits.SwapBytes(reversedEntryIndex) + 1);
                            // now we'll find the next item to delete, and do so until we run out of items
                            // to write
                        } while (table.ReadByKey(key, out reader));

                        LogHistory.CancelHistoryEntriesFrom(context, entry.Index, term, "We have found a divergence in the log, and we now need to cancel from this location forward.");
                    }

                    var nested = context.ReadObject(entry.Entry, "entry");
                    using (table.Allocate(out TableValueBuilder tableValueBuilder))
                    {
                        tableValueBuilder.Add(originalReversedIndex);
                        tableValueBuilder.Add(entry.Term);
                        tableValueBuilder.Add(nested.BasePointer, nested.Size);
                        tableValueBuilder.Add((int)entry.Flags);
                        table.Insert(tableValueBuilder);
                    }
                    if (entry.Flags == RachisEntryFlags.Topology)
                    {
                        lastTopology?.Dispose();
                        lastTopology = nested;
                        lastTopologyIndex = entry.Index;
                    }
                    else
                    {
                        nested.Dispose();
                    }

                    LogHistory.InsertHistoryLog(context, entry.Index, entry.Term, entry.Entry);
                }
            }

            Transaction.DebugDisposeReaderAfterTransaction(context.Transaction.InnerTransaction, lastTopology);
            return (lastTopology, lastTopologyIndex);
        }

        private void ThrowFatalError(RachisEntry firstEntry, long? myTermForTheIndex, long lastCommitIndex, long lastCommitTerm)
        {
            var message =
                $"FATAL ERROR: got an append entries request with index={firstEntry.Index:#,#;;0} term={firstEntry.Term:#,#;;0} " +
                $"while my term for this index is {myTermForTheIndex:#,#;;0}. " +
                $"(last commit index={lastCommitIndex:#,#;;0} with term={lastCommitTerm:#,#;;0}), this means something went wrong badly.";
            if (Log.IsOperationsEnabled)
            {
                Log.Operations(message);
            }
            RachisInvalidOperationException.Throw(message);
        }

        internal static void GetLastTruncated<TTransaction>(TransactionOperationContext<TTransaction> context, out long lastTruncatedIndex, out long lastTruncatedTerm)
            where TTransaction : RavenTransaction
        {
            var state = context.Transaction.InnerTransaction.ReadTree(GlobalStateSlice);
            var read = state.Read(LastTruncatedSlice);
            if (read == null)
            {
                lastTruncatedIndex = 0;
                lastTruncatedTerm = 0;
                return;
            }
            var reader = read.Reader;
            lastTruncatedIndex = reader.ReadLittleEndianInt64();
            lastTruncatedTerm = reader.ReadLittleEndianInt64();
        }

        public unsafe BlittableJsonReaderObject GetEntry(ClusterOperationContext context, long index,
            out RachisEntryFlags flags)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            var reversedIndex = Bits.SwapBytes(index);
            using (Slice.External(context.Allocator, (byte*)&reversedIndex, sizeof(long), out Slice key))
            {
                if (table.ReadByKey(key, out TableValueReader reader) == false)
                {
                    flags = RachisEntryFlags.Invalid;
                    return null;
                }
                flags = *(RachisEntryFlags*)reader.Read(3, out int size);
                Debug.Assert(size == sizeof(RachisEntryFlags));
                var ptr = reader.Read(2, out size);
                BlittableJsonReaderObject entry = new BlittableJsonReaderObject(ptr, size, context);

                Transaction.DebugDisposeReaderAfterTransaction(context.Transaction.InnerTransaction, entry);
                return entry;
            }
        }

        public long GetLastCommitIndex<TTransaction>(TransactionOperationContext<TTransaction> context)
            where TTransaction : RavenTransaction
        {
            Debug.Assert(context.Transaction != null);

            var state = context.Transaction.InnerTransaction.ReadTree(GlobalStateSlice);
            var read = state.Read(LastCommitSlice);
            if (read == null)
                return 0;
            return read.Reader.ReadLittleEndianInt64();
        }

        public void GetLastCommitIndex(ClusterOperationContext context, out long index, out long term)
        {
            Debug.Assert(context.Transaction != null);

            var state = context.Transaction.InnerTransaction.ReadTree(GlobalStateSlice);
            var read = state.Read(LastCommitSlice);
            if (read == null)
            {
                index = 0;
                term = 0;
                return;
            }
            var reader = read.Reader;
            index = reader.ReadLittleEndianInt64();
            term = reader.ReadLittleEndianInt64();
        }

        public unsafe void SetLastCommitIndex(ClusterOperationContext context, long index, long term)
        {
            Debug.Assert(context.Transaction != null);
            Debug.Assert(index != 0);
            Debug.Assert(term != 0);

            var state = context.Transaction.InnerTransaction.ReadTree(GlobalStateSlice);
            var read = state.Read(LastCommitSlice);
            if (read != null)
            {
                var reader = read.Reader;
                var oldIndex = reader.ReadLittleEndianInt64();
                if (oldIndex > index)
                    throw new InvalidOperationException(
                        $"Cannot reduce the last commit index (is {oldIndex:#,#;;0} but was requested to reduce to {index:#,#;;0})");
                if (oldIndex == index)
                {
                    var oldTerm = reader.ReadLittleEndianInt64();
                    if (oldTerm != term)
                        throw new InvalidOperationException(
                            $"Cannot change just the last commit index (at {oldIndex:#,#;;0} index, was {oldTerm:#,#;;0} but was requested to change it to {term:#,#;;0})");
                }
            }

            using (state.DirectAdd(LastCommitSlice, sizeof(long) * 2, out byte* ptr))
            {
                var data = (long*)ptr;
                data[0] = index;
                data[1] = term;
            }

            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += _ => TaskExecutor.CompleteAndReplace(ref _commitIndexChanged);
        }

        public async Task WaitForCommitIndexChange(CommitIndexModification modification, long value)
        {
            var timeoutTask = TimeoutManager.WaitFor(OperationTimeout);
            while (timeoutTask.IsCompleted == false)
            {
                var task = _commitIndexChanged.Task;
                using (ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var commitIndex = GetLastCommitIndex(context);
                    switch (modification)
                    {
                        case CommitIndexModification.GreaterOrEqual:
                            if (value <= commitIndex)
                                return;
                            break;

                        case CommitIndexModification.AnyChange:
                            await WaitForCommitChangeOrThrowTimeoutException(timeoutTask, task);
                            return;

                        default:
                            throw new ArgumentOutOfRangeException(nameof(modification), modification, null);
                    }
                }

                await WaitForCommitChangeOrThrowTimeoutException(timeoutTask, task);
            }

            ThrowTimeoutException();
        }

        private static async Task WaitForCommitChangeOrThrowTimeoutException(Task timeoutTask, Task task)
        {
            if (timeoutTask == await Task.WhenAny(task, timeoutTask))
                ThrowTimeoutException();

            await task; // propagate cancellation/exception
        }

        private static void ThrowTimeoutException()
        {
            throw new TimeoutException();
        }

        public unsafe (long Min, long Max) GetLogEntriesRange(TransactionOperationContext context)
        {
            Debug.Assert(context.Transaction != null);

            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            if (table.SeekOnePrimaryKey(Slices.AfterAllKeys, out TableValueReader reader) == false)
                return (0L, 0L);
            var max = Bits.SwapBytes(*(long*)reader.Read(0, out int size));
            Debug.Assert(size == sizeof(long));
            if (table.SeekOnePrimaryKey(Slices.BeforeAllKeys, out reader) == false)
                return (0L, 0L);
            var min = Bits.SwapBytes(*(long*)reader.Read(0, out size));
            Debug.Assert(size == sizeof(long));

            return (min, max);
        }

        public unsafe long GetFirstEntryIndex(ClusterOperationContext context)
        {
            Debug.Assert(context.Transaction != null);

            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            if (table.SeekOnePrimaryKey(Slices.BeforeAllKeys, out TableValueReader reader) == false)
            {
                GetLastTruncated(context, out long lastTruncatedIndex, out long _);
                return lastTruncatedIndex;
            }
            var max = Bits.SwapBytes(*(long*)reader.Read(0, out int size));
            Debug.Assert(size == sizeof(long));
            return max;
        }

        public unsafe long GetLastEntryIndex(ClusterOperationContext context)
        {
            Debug.Assert(context.Transaction != null);

            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            if (table.SeekOnePrimaryKey(Slices.AfterAllKeys, out TableValueReader reader) == false)
            {
                GetLastTruncated(context, out long lastTruncatedIndex, out long _);
                return lastTruncatedIndex;
            }

            var max = Bits.SwapBytes(*(long*)reader.Read(0, out int size));
            Debug.Assert(size == sizeof(long));
            return max;
        }

        public long GetTermForKnownExisting(ClusterOperationContext context, long index)
        {
            var termFor = GetTermFor(context, index);
            if (termFor == null)
                throw new InvalidOperationException("Expected the index " + index +
                                                    " to have a term in the entries, but got null");
            return termFor.Value;
        }

        public unsafe long? GetTermFor(ClusterOperationContext context, long index)
        {
            Debug.Assert(context.Transaction != null);
            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            var reversedIndex = Bits.SwapBytes(index);
            using (
    Slice.External(context.Transaction.InnerTransaction.Allocator, (byte*)&reversedIndex, sizeof(long),
        out Slice key))
            {
                if (table.ReadByKey(key, out TableValueReader reader) == false)
                {
                    GetLastCommitIndex(context, out long lastIndex, out long lastTerm);
                    if (lastIndex == index)
                        return lastTerm;
                    GetLastTruncated(context, out lastIndex, out lastTerm);
                    if (lastIndex == index)
                        return lastTerm;
                    return null;
                }
                var term = *(long*)reader.Read(1, out int size);
                Debug.Assert(size == sizeof(long));
                Debug.Assert(term != 0);
                return term;
            }
        }

        public void FoundAboutHigherTerm(long term, string reason)
        {
            if (term <= CurrentTerm)
                return;

            using (ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    // we check it here again because now we are under the tx lock, so we can't get into concurrency issues
                    if (term <= CurrentTerm)
                        return;

                    CastVoteInTerm(context, term, votedFor: null, reason: reason);

                    tx.Commit();
                }
            }
        }

        public void ValidateTerm(long term)
        {
            if (term != CurrentTerm)
            {
                throw new ConcurrencyException($"The term was changed from {term:#,#;;0} to {CurrentTerm:#,#;;0}");
            }
        }

        public unsafe void CastVoteInTerm(ClusterOperationContext context, long term, string votedFor, string reason)
        {
            Debug.Assert(context.Transaction != null);
            if (term <= CurrentTerm)
                throw new ConcurrencyException($"The current term {CurrentTerm:#,#;;0} is larger or equal to {term:#,#;;0}, aborting change");

            var state = context.Transaction.InnerTransaction.CreateTree(GlobalStateSlice);
            using (state.DirectAdd(CurrentTermSlice, sizeof(long), out byte* ptr))
            {
                *(long*)ptr = term;
            }

            if (Log.IsInfoEnabled)
                Log.Info($"Casting vote for {votedFor ?? "<???>"} in {term:#,#;;0} because: {reason}");

            votedFor = votedFor ?? string.Empty;

            var size = Encoding.UTF8.GetByteCount(votedFor);

            using (state.DirectAdd(VotedForSlice, size, out var ptr))
            {
                fixed (char* pVotedFor = votedFor)
                {
                    Encoding.UTF8.GetBytes(pVotedFor, votedFor.Length, ptr, size);
                }
            }

            CurrentTerm = term;

            // give the other side enough time to become the leader before challenging them
            Timeout.Defer(votedFor);

            var currentlyTheLeader = _currentLeader;
            if (currentlyTheLeader == null)
                return;

            ThreadPool.QueueUserWorkItem(_ =>
            {
                try
                {
                    if (Log.IsInfoEnabled)
                    {
                        Log.Info($"Disposing the leader because we casted a vote for {votedFor} in {term:#,#;;0}");
                    }
                    currentlyTheLeader.Dispose();
                }
                catch (Exception e)
                {
                    if (Log.IsInfoEnabled)
                    {
                        Log.Info($"Failed to shut down leader after voting in term {term:#,#;;0} for {votedFor}", e);
                    }
                }
            }, null);
        }

        public (string VotedFor, long LastVotedTerm) GetWhoGotMyVoteIn(ClusterOperationContext context, long term)
        {
            Debug.Assert(context.Transaction != null);

            var state = context.Transaction.InnerTransaction.CreateTree(GlobalStateSlice);
            var read = state.Read(CurrentTermSlice);

            var votedTerm = read?.Reader.ReadLittleEndianInt64();

            if (votedTerm != term && votedTerm.HasValue)
                return (null, votedTerm.Value);

            read = state.Read(VotedForSlice);

            return (read?.Reader.ReadString(read.Reader.Length), votedTerm ?? 0);
        }

        public event EventHandler OnDispose;

        public bool IsDisposed => _disposeEvent.IsSet;

        public virtual void Dispose()
        {
            _disposeEvent.Set();
            Timeout?.Dispose();
            OnDispose?.Invoke(this, EventArgs.Empty);
            _topologyChanged.TrySetCanceled();
            _stateChanged.TrySetCanceled();
            _commitIndexChanged.TrySetCanceled();
            ContextPool?.Dispose();
        }

        public abstract Task<RachisConnection> ConnectToPeer(string url, string tag, X509Certificate2 certificate);

        public class BootstrapOptions
        {
            public string NewNodeTag;
            public Guid? TopologyId;
        }

        public void Bootstrap(string selfUrl, string nodeTag)
        {
            if (selfUrl == null)
                throw new ArgumentNullException(nameof(selfUrl));

            using (ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
            using (var tx = ctx.OpenWriteTransaction())
            {
                if (CurrentState != RachisState.Passive)
                    return;

                var newTag = _tag;
                if (_tag == InitialTag)
                {
                    newTag = nodeTag;
                    UpdateNodeTag(ctx, nodeTag);
                }

                var topologyId = Guid.NewGuid().ToString();

                var topology = new ClusterTopology(
                    topologyId,
                    new Dictionary<string, string>
                    {
                        [newTag] = selfUrl
                    },
                    new Dictionary<string, string>(),
                    new Dictionary<string, string>(),
                    newTag,
                    GetLastEntryIndex(ctx)
                );

                SetTopology(this, ctx, topology);
                _clusterId = topologyId;
                SetClusterBase(topologyId);

                SetSnapshotRequest(ctx, false);

                SwitchToSingleLeader(ctx);

                tx.Commit();
            }
        }

        public string HardResetToNewCluster(string nodeTag = "A")
        {
            using (ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
            using (var tx = ctx.OpenWriteTransaction())
            {
                var topologyId = Guid.NewGuid().ToString();
                var topology = new ClusterTopology(
                    topologyId,
                    new Dictionary<string, string>
                    {
                        [nodeTag] = Url
                    },
                    new Dictionary<string, string>(),
                    new Dictionary<string, string>(),
                    _tag,
                    GetLastEntryIndex(ctx) + 1
                );

                UpdateNodeTag(ctx, nodeTag);

                SetTopology(this, ctx, topology);

                SetSnapshotRequest(ctx, false);

                SwitchToSingleLeader(ctx);

                tx.Commit();

                return topologyId;
            }
        }

        public void HardResetToPassive(string topologyId = null)
        {
            using (ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
            using (var tx = ctx.OpenWriteTransaction())
            {
                UpdateNodeTag(ctx, InitialTag);
                var oldTopology = GetTopology(ctx);

                var topology = new ClusterTopology(
                    topologyId ?? oldTopology.TopologyId,
                    new Dictionary<string, string>
                    {
                        [_tag] = Url
                    },
                    new Dictionary<string, string>(),
                    new Dictionary<string, string>(),
                    _tag,
                    GetLastEntryIndex(ctx) + 1
                );

                if (topologyId != oldTopology.TopologyId)
                    // if we are going to add this to a different cluster we must get a snapshot
                    SetSnapshotRequest(ctx, true);

                SetTopology(this, ctx, topology);

                SetNewStateInTx(ctx, RachisState.Passive, null, CurrentTerm, "Hard reset to passive by admin");

                tx.Commit();
            }
        }

        public static void ValidateNodeTag(string nodeTag)
        {
            if (nodeTag == InitialTag)
                return;

            if (nodeTag.Equals("RAFT"))
                ThrowInvalidNodeTag(nodeTag, "It is a reserved tag.");
            if (nodeTag.Length > 4)
                ThrowInvalidNodeTag(nodeTag, "Max node tag length is 4.");
            // Node tag must not contain ':' or '-' chars as they are in use in change vector.
            // The following check covers that as well.
            if (nodeTag.IsUpperLettersOnly() == false)
                ThrowInvalidNodeTag(nodeTag, "Node tag must contain only upper case letters.");
        }

        public static void ThrowInvalidNodeTag(string nodeTag, string reason)
        {
            throw new ArgumentException($"Can't set the node tag to '{nodeTag}'. {reason}");
        }

        public Task AddToClusterAsync(string url, string nodeTag = null, bool validateNotInTopology = true, bool asWatcher = false)
        {
            return ModifyTopologyAsync(nodeTag, url, asWatcher ? Leader.TopologyModification.NonVoter : Leader.TopologyModification.Promotable, validateNotInTopology);
        }

        public Task RemoveFromClusterAsync(string nodeTag)
        {
            return ModifyTopologyAsync(nodeTag, null, Leader.TopologyModification.Remove);
        }

        public async Task ModifyTopologyAsync(string nodeTag, string nodeUrl, Leader.TopologyModification modification, bool validateNotInTopology = false)
        {
            var leader = _currentLeader;
            if (leader == null)
                throw new NotLeadingException("I am not the leader, cannot accept commands. " + _lastStateChangeReason);

            Task task;
            while (leader.TryModifyTopology(nodeTag, nodeUrl, modification, out task, validateNotInTopology) == false)
                await task;

            await task;
        }

        private string _leaderTag;

        public string LeaderTag
        {
            get => GetLeaderTag();
            internal set => _leaderTag = value;
        }

        public string GetLeaderTag(bool safe = false)
        {
            switch (CurrentState)
            {
                case RachisState.Passive:
                case RachisState.Candidate:
                    return null;

                case RachisState.Follower:
                    return safe ? Volatile.Read(ref _leaderTag) : _leaderTag;

                case RachisState.LeaderElect:
                case RachisState.Leader:
                    if (CurrentLeader?.Running != true)
                        return null;
                    return safe ? Volatile.Read(ref _tag) : _tag;

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public bool IsEncrypted => _persistentState.Options.Encryption.IsEnabled;

        public abstract X509Certificate2 ClusterCertificate { get; }

        public abstract bool ShouldSnapshot(Slice slice, RootObjectType type);

        public abstract long Apply(ClusterOperationContext context, long uptoInclusive, Leader leader, Stopwatch duration);

        public abstract void SnapshotInstalled(long lastIncludedIndex, bool fullSnapshot, CancellationToken token);

        private readonly AsyncManualResetEvent _leadershipTimeChanged = new AsyncManualResetEvent();
        private int _heartbeatWaitersCounter;

        public void InvokeBeforeAppendToRaftLog(ClusterOperationContext context, CommandBase cmd)
        {
            BeforeAppendToRaftLog?.Invoke(context, cmd);
        }

        public async Task WaitForHeartbeat()
        {
            Interlocked.Increment(ref _heartbeatWaitersCounter);
            try
            {
                await _leadershipTimeChanged.WaitAsync();
            }
            finally
            {
                Interlocked.Decrement(ref _heartbeatWaitersCounter);
            }
        }

        private long _leaderTime;
        private TimeSpan _operationTimeout;
        private TimeSpan _electionTimeout;
        private TimeSpan _tcpConnectionTimeout;
        private DateTime _lastStateChangeTime;
        private readonly string _clusterIdBase64Id = new string(' ', 22);
        public readonly CipherSuitesPolicy CipherSuitesPolicy;
        private volatile RachisState _currentState;

        private unsafe void SetClusterBase(string str)
        {
            if (str == null)
                return;

            var guid = new Guid(str);
            fixed (char* pChars = _clusterIdBase64Id)
            {
                var result = Base64.ConvertToBase64ArrayUnpadded(pChars, (byte*)&guid, 0, 16);
                Debug.Assert(result == 22);
            }
        }

        public void ReportLeaderTime(long leaderTime)
        {
            Interlocked.Exchange(ref _leaderTime, leaderTime);

            if (_heartbeatWaitersCounter == 0)
                return;

            _leadershipTimeChanged.SetAndResetAtomically();
        }

        public DynamicJsonArray GetClusterErrorsFromLeader()
        {
            if (_currentLeader == null)
                return new DynamicJsonArray();

            var dja = new DynamicJsonArray();
            foreach (var entry in _currentLeader.ErrorsList)
            {
                var djv = new DynamicJsonValue
                {
                    ["Node"] = entry.node,
                    ["Error"] = entry.error.ToJson()
                };
                dja.Add(djv);
            }
            return dja;
        }

        public void UpdateNodeTag(ClusterOperationContext context, string newTag)
        {
            ValidateNodeTag(newTag);

            using (Slice.From(context.Transaction.InnerTransaction.Allocator, newTag, out Slice str))
            using (Slice.From(context.Transaction.InnerTransaction.Allocator, _tag, out Slice oldTag))
            {
                var state = context.Transaction.InnerTransaction.CreateTree(GlobalStateSlice);
                state.Add(TagSlice, str);

                if (_tag != InitialTag)
                    state.Add(PreviousTagSlice, oldTag);
            }

            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += tx =>
            {
                if (tx is LowLevelTransaction llt && llt.Committed)
                {
                    _tag = newTag;
                }
            };
        }

        public bool RequestSnapshot { get; private set; }

        public void SetSnapshotRequest<TTransaction>(TransactionOperationContext<TTransaction> context, bool request)
            where TTransaction : RavenTransaction
        {
            var oldValue = GetSnapshotRequest(context);

            var state = context.Transaction.InnerTransaction.CreateTree(GlobalStateSlice);
            state.Add(SnapshotRequestSlice, Convert.ToByte(request));
            RequestSnapshot = request;

            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += tx =>
            {
                if (tx is LowLevelTransaction llt && llt.Committed == false)
                {
                    RequestSnapshot = oldValue;
                }
            };
        }

        public bool GetSnapshotRequest<TTransaction>(TransactionOperationContext<TTransaction> context)
            where TTransaction : RavenTransaction
        {
            var state = context.Transaction.InnerTransaction.CreateTree(GlobalStateSlice);
            var reader = state.Read(SnapshotRequestSlice);

            if (reader == null)
                return false;

            return Convert.ToBoolean(reader.Reader.ReadByte());
        }

        public void LeaderElectToLeaderChanged()
        {
            LeaderElected?.Invoke(null, null);
        }

        public unsafe void ClearAppendedEntriesAfter(ClusterOperationContext context, long index)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            var reversedEntryIndex = Bits.SwapBytes(index);
            using (Slice.External(context.Transaction.InnerTransaction.Allocator, (byte*)&reversedEntryIndex, sizeof(long), out Slice key))
            {
                table.DeleteByPrimaryKey(key, _ => true);
            }
        }

        public static bool IsExpectedException(Exception e)
        {
            if (e is AggregateException)
                return IsExpectedException(e.InnerException);

            return e is OperationCanceledException ||
                   e is ObjectDisposedException;
        }

        internal static void DisconnectAction(Task<RachisConnection> connectionTask)
        {
            Debug.Assert(connectionTask.IsCompleted);
            try
            {
                connectionTask.Result.Disconnect();
            }
            catch
            {
                // we don't care about it, since this is part of a continuation task.
            }
        }
    }

    public class TopologyMismatchException : Exception
    {
        public TopologyMismatchException()
        {
        }

        public TopologyMismatchException(string message) : base(message)
        {
        }

        public TopologyMismatchException(string message, Exception inner) : base(message, inner)
        {
        }
    }

    public class NotLeadingException : Exception
    {
        public NotLeadingException()
        {
        }

        public NotLeadingException(string message) : base(message)
        {
        }

        public NotLeadingException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
