using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Utils;
using Voron;
using Voron.Data;
using Voron.Data.Tables;
using Voron.Impl;
using Raven.Client.Http;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;

namespace Raven.Server.Rachis
{
    public class RachisConsensus<TStateMachine> : RachisConsensus
        where TStateMachine : RachisStateMachine, new()
    {
        private readonly ServerStore _serverStore;

        public RachisConsensus(ServerStore serverStore, int? seed = null) : base(seed)
        {
            _serverStore = serverStore;
        }

        public TStateMachine StateMachine;

        internal override RachisStateMachine GetStateMachine()
        {
            return StateMachine;
        }

        protected override void InitializeState(TransactionOperationContext context)
        {
            StateMachine = new TStateMachine();
            StateMachine.Initialize(this, context);
        }

        public override void Dispose()
        {
            SetNewState(State.Follower, new NullDisposable(), -1, "Disposing Rachis");
            StateMachine?.Dispose();
            base.Dispose();
        }

        public override void Apply(TransactionOperationContext context, long uptoInclusive, Leader leader)
        {
            StateMachine.Apply(context, uptoInclusive, leader, _serverStore);
        }

        public void EnsureNodeRemovalOnDeletion(TransactionOperationContext context, string nodeTag)
        {
            StateMachine.EnsureNodeRemovalOnDeletion(context, nodeTag);
        }

        public override X509Certificate2 ClusterCertificate => _serverStore.RavenServer.ClusterCertificateHolder?.Certificate;

        public override bool ShouldSnapshot(Slice slice, RootObjectType type)
        {
            return StateMachine.ShouldSnapshot(slice, type);
        }

        public override void SnapshotInstalled(TransactionOperationContext context, long lastIncludedIndex)
        {
            StateMachine.OnSnapshotInstalled(context, lastIncludedIndex, _serverStore);
        }

        public override Task<Stream> ConnectToPeer(string url, X509Certificate2 certificate, TransactionOperationContext context = null)
        {
            return StateMachine.ConnectToPeer(url, certificate);
        }

        private class NullDisposable : IDisposable
        {
            public void Dispose()
            {

            }
        }

        public unsafe List<BlittableJsonReaderObject> GetLogEntries(long first, TransactionOperationContext context, int max)
        {
            var entries = new List<BlittableJsonReaderObject>();
            var reveredNextIndex = Bits.SwapBytes(first);
            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            using (Slice.External(context.Allocator, (byte*)&reveredNextIndex, sizeof(long), out Slice key))
            {
                foreach (var value in table.SeekByPrimaryKey(key, 0))
                {
                    var entry = FollowerAmbassador.BuildRachisEntryToSend(context, value);
                    entries.Add(entry);
                    if (entries.Count >= max)
                        break;
                }
            }
            return entries;
        }
    }

    public abstract class RachisConsensus : IDisposable
    {
        public enum State
        {
            Passive,
            Candidate,
            Follower,
            LeaderElect,
            Leader
        }

        internal abstract RachisStateMachine GetStateMachine();

        public const string InitialTag = "?";

        public State CurrentState { get; private set; }

        public string LastStateChangeReason => _lastStateChangeReason;

        public event EventHandler<ClusterTopology> TopologyChanged;

        public event EventHandler<StateTransition> StateChanged;

        public event EventHandler LeaderElected;

        private string _tag;
        private string _clusterId;

        public TransactionContextPool ContextPool { get; private set; }
        private StorageEnvironment _persistentState;
        internal Logger Log;

        private readonly List<IDisposable> _disposables = new List<IDisposable>();

        public long CurrentTerm { get; private set; }
        public string Tag => _tag;
        public string ClusterId => _clusterId;
        public string Url;

        private static readonly Slice GlobalStateSlice;
        private static readonly Slice CurrentTermSlice;
        private static readonly Slice VotedForSlice;
        private static readonly Slice LastCommitSlice;
        private static readonly Slice LastTruncatedSlice;
        private static readonly Slice TopologySlice;
        private static readonly Slice TagSlice;

        internal static readonly Slice EntriesSlice;
        internal static readonly TableSchema LogsTable;

        static RachisConsensus()
        {
            Slice.From(StorageEnvironment.LabelsContext, "GlobalState", out GlobalStateSlice);
            Slice.From(StorageEnvironment.LabelsContext, "Tag", out TagSlice);

            Slice.From(StorageEnvironment.LabelsContext, "CurrentTerm", out CurrentTermSlice);
            Slice.From(StorageEnvironment.LabelsContext, "VotedFor", out VotedForSlice);
            Slice.From(StorageEnvironment.LabelsContext, "LastCommit", out LastCommitSlice);
            Slice.From(StorageEnvironment.LabelsContext, "Topology", out TopologySlice);
            Slice.From(StorageEnvironment.LabelsContext, "LastTruncated", out LastTruncatedSlice);


            Slice.From(StorageEnvironment.LabelsContext, "Entries", out EntriesSlice);

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

        private Leader _currentLeader;
        public Leader CurrentLeader => _currentLeader;
        private TaskCompletionSource<object> _topologyChanged = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        private TaskCompletionSource<object> _stateChanged = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        private TaskCompletionSource<object> _commitIndexChanged = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ManualResetEventSlim _disposeEvent = new ManualResetEventSlim();
        private readonly Random _rand;
        private string _lastStateChangeReason;
        public Candidate Candidate { get; private set; }

        protected RachisConsensus(int? seed = null)
        {
            _rand = seed.HasValue ? new Random(seed.Value) : new Random();
        }

        public void RandomizeTimeout(bool extend = false)
        {
            //We want to be able to reproduce rare issues that are related to timing
            var timeout = (int)ElectionTimeout.TotalMilliseconds;
            if (extend)
                timeout = Math.Max(timeout, timeout * 2); // avoid overflow

            Timeout.TimeoutPeriod = _rand.Next(timeout / 3 * 2, timeout);
        }

        public unsafe void Initialize(StorageEnvironment env, RavenConfiguration configuration, string myUrl)
        {
            try
            {
                _persistentState = env;

                OperationTimeout = configuration.Cluster.OperationTimeout.AsTimeSpan;
                ElectionTimeout = configuration.Cluster.ElectionTimeout.AsTimeSpan;
                TcpConnectionTimeout = configuration.Cluster.TcpConnectionTimeout.AsTimeSpan;
                
                DebuggerAttachedTimeout.LongTimespanIfDebugging(ref _operationTimeout);
                DebuggerAttachedTimeout.LongTimespanIfDebugging(ref _electionTimeout);

                ContextPool = new TransactionContextPool(_persistentState);

                ClusterTopology topology;
                using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var state = tx.InnerTransaction.CreateTree(GlobalStateSlice);

                    var readResult = state.Read(TagSlice);
                    _tag = readResult == null ? InitialTag : readResult.Reader.ToStringValue();

                    Log = LoggingSource.Instance.GetLogger<RachisConsensus>(_tag);
                    LogsTable.Create(tx.InnerTransaction, EntriesSlice, 16);

                    var read = state.Read(CurrentTermSlice);
                    if (read == null || read.Reader.Length != sizeof(long))
                    {
                        using (state.DirectAdd(CurrentTermSlice, sizeof(long), out byte* ptr))
                            *(long*)ptr = CurrentTerm = 0;
                    }
                    else
                        CurrentTerm = read.Reader.ReadLittleEndianInt64();

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
                    _clusterId = topology.TopologyId;
                    InitializeState(context);

                    tx.Commit();
                }

                Timeout = new TimeoutEvent(0);
                RandomizeTimeout();

                // if we don't have a topology id, then we are passive
                // an admin needs to let us know that it is fine, either
                // by explicit bootstraping or by connecting us to a cluster
                if (topology.TopologyId == null ||
                    topology.Members.ContainsKey(_tag) == false)
                {
                    CurrentState = State.Passive;
                    return;
                }

                CurrentState = State.Follower;
                if (topology.Members.Count == 1)
                {
                    using (ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    {
                        using (ctx.OpenWriteTransaction())
                        {
                            SwitchToSingleLeader(ctx);
                            ctx.Transaction.Commit();
                        }

                    }
                }
                else
                    Timeout.Start(SwitchToCandidateStateOnTimeout);
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        private void SwitchToSingleLeader(TransactionOperationContext context)
        {
            var electionTerm = CurrentTerm + 1;
            CastVoteInTerm(context, electionTerm, Tag);

            if (Log.IsInfoEnabled)
            {
                Log.Info("Switching to leader state");
            }
            var leader = new Leader(this);
            SetNewStateInTx(context, State.LeaderElect, leader, electionTerm, "I'm the only one in the cluster, so I'm the leader" , () => _currentLeader = leader);
            Candidate = null;
            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += tx =>
            {
                if (tx is LowLevelTransaction llt && llt.Committed)
                {
                    leader.Start();
                }
            };
        }

        protected abstract void InitializeState(TransactionOperationContext context);

        public async Task WaitForState(State state)
        {
            while (true)
            {
                // we setup the wait _before_ checking the state
                var task = _stateChanged.Task;

                if (CurrentState == state)
                    return;

                await task;
            }
        }

        public async Task WaitForLeaveState(State state)
        {
            while (true)
            {
                // we setup the wait _before_ checking the state
                var task = _stateChanged.Task;

                if (CurrentState != state)
                    return;

                await task;
            }
        }

        public Task GetTopologyChanged()
        {
            return _topologyChanged.Task;
        }

        public async Task WaitForTopology(Leader.TopologyModification modification, string nodeTag = null)
        {
            while (true)
            {
                var task = _topologyChanged.Task;
                var tag = nodeTag ?? _tag;
                using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
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

                await task;
            }
        }

        public enum CommitIndexModification
        {
            GreaterOrEqual,
            AnyChange
        }

        public void SetNewState(State state, IDisposable disposable, long expectedTerm, string stateChangedReason, Action beforeStateChangedEvent = null)
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenWriteTransaction()) // we use the write transaction lock here
            {
                SetNewStateInTx(context, state, disposable, expectedTerm, stateChangedReason , beforeStateChangedEvent);
                context.Transaction.Commit();
            }
        }

        public class StateTransition
        {
            public State From;
            public State To;
            public string Reason;
            public long CurrentTerm;
            public DateTime When;
        }

        private void SetNewStateInTx(TransactionOperationContext context,
            State state,
            IDisposable disposable,
            long expectedTerm,
            string stateChangedReason,
            Action beforeStateChangedEvent = null)
        {
            if (expectedTerm != CurrentTerm && expectedTerm != -1)
                throw new ConcurrencyException(
                    $"Attempted to switch state to {state} on expected term {expectedTerm} but the real term is {CurrentTerm}");

            _currentLeader = null;
            _lastStateChangeReason = stateChangedReason;
            var toDispose = new List<IDisposable>(_disposables);

            _disposables.Clear();

            if (disposable != null)
                _disposables.Add(disposable);
            else if (state != State.Passive)
            {
                // if we are back to null state, wait to become candidate if no one talks to us
                Timeout.Start(SwitchToCandidateStateOnTimeout);
            } 

            if (state == State.Passive)
            {
                DeleteTopology(context);
            }

            var transition = new StateTransition
            {
                CurrentTerm = expectedTerm,
                From = CurrentState,
                To = state,
                Reason = stateChangedReason,
                When = DateTime.UtcNow
            };

            PrevStates.LimitedSizeEnqueue(transition, 5);

            CurrentState = state;

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

                    TaskExecutor.CompleteReplaceAndExecute(ref _stateChanged, () =>
                    {
                        foreach (var d in toDispose)
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
                        }
                    });
                }
            };
        }

        public ConcurrentQueue<StateTransition> PrevStates { get; set; } = new ConcurrentQueue<StateTransition>();

        public bool TakeOffice()
        {
            if (CurrentState != State.LeaderElect)
                return false;

            CurrentState = State.Leader;
            TaskExecutor.CompleteAndReplace(ref _stateChanged);
            return true;
        }

        public void AppendStateDisposable(IDisposable parentState, IDisposable disposeOnStateChange)
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenWriteTransaction()) // using write tx just for the lock here
            {
                if (_disposables.Count == 0 || ReferenceEquals(_disposables[0], parentState) == false)
                    throw new ConcurrencyException(
                        "Could not set the disposeOnStateChange because by the time we did it the parent state has changed");
                _disposables.Add(disposeOnStateChange);
            }
        }

        public void SwitchToLeaderState(long electionTerm, string reason)
        {
            if (Log.IsInfoEnabled)
            {
                Log.Info("Switching to leader state");
            }
            var leader = new Leader(this);
            SetNewState(State.LeaderElect, leader, electionTerm, reason, () => _currentLeader = leader);
            leader.Start();
        }

        public async Task<(long Index, object Result)> PutAsync(CommandBase cmd)
        {
            var leader = _currentLeader;
            if (leader == null)
                throw new NotLeadingException("Not a leader, cannot accept commands. " + _lastStateChangeReason);

            var putTask = leader.PutAsync(cmd);
            if (await putTask.WaitWithTimeout(OperationTimeout) == false)
                throw new TimeoutException($"Waited for {OperationTimeout} but the command was not applied in this time.");

            return await putTask;
        }

        public void Put(CommandBase cmd)
        {
            var leader = _currentLeader;
            if (leader == null)
                throw new NotLeadingException("Not a leader, cannot accept commands. " + _lastStateChangeReason);

            leader.PutAsync(cmd);
        }

        public void SwitchToCandidateStateOnTimeout()
        {
            SwitchToCandidateState("Election timeout");
        }

        public void SwitchToCandidateState(string reason, bool forced = false)
        {
            Timeout.DisableTimeout();
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var clusterTopology = GetTopology(context);
                if (clusterTopology.TopologyId == null ||
                    clusterTopology.Members.ContainsKey(_tag) == false)
                {
                    if (Log.IsInfoEnabled)
                    {
                        Log.Info("Can't switch to candidate mode when not initialized with topology / not a voter");
                    }
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
            SetNewState(State.Candidate, candidate, CurrentTerm, reason);
            candidate.Start();
        }

        public void DeleteTopology(TransactionOperationContext context)
        {
            var topology = GetTopology(context);
            var newTopology = new ClusterTopology(
                topology.TopologyId,
                new Dictionary<string, string>(),
                new Dictionary<string, string>(),
                new Dictionary<string, string>(),
                topology.LastNodeId
            );
            SetTopology(context, newTopology);
        }

        public unsafe ClusterTopology GetTopology(TransactionOperationContext context)
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
                    ""
                );
            }

            var json = new BlittableJsonReaderObject(read.Reader.Base, read.Reader.Length, context);
            return JsonDeserializationRachis<ClusterTopology>.Deserialize(json);
        }

        public unsafe BlittableJsonReaderObject GetTopologyRaw(TransactionOperationContext context)
        {
            Debug.Assert(context.Transaction != null);
            var state = context.Transaction.InnerTransaction.ReadTree(GlobalStateSlice);
            var read = state.Read(TopologySlice);
            if (read == null)
                return null;

            return new BlittableJsonReaderObject(read.Reader.Base, read.Reader.Length, context);
        }

        public BlittableJsonReaderObject SetTopology(TransactionOperationContext context, ClusterTopology topology)
        {
            Debug.Assert(context.Transaction != null);
            var topologyJson = SetTopology(this, context, topology);

            return topologyJson;
        }
        public static unsafe BlittableJsonReaderObject SetTopology(RachisConsensus engine, TransactionOperationContext context,
            ClusterTopology clusterTopology)
        {
            var topologyJson = context.ReadObject(clusterTopology.ToJson(), "topology");
            var state = context.Transaction.InnerTransaction.CreateTree(GlobalStateSlice);
            using (state.DirectAdd(TopologySlice, topologyJson.Size, out byte* ptr))
            {
                topologyJson.CopyTo(ptr);
            }

            if (engine == null)
                return null;

            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += _ =>
            {
                TaskExecutor.CompleteAndReplace(ref engine._topologyChanged);
                engine.TopologyChanged?.Invoke(engine, clusterTopology);
            };

            return topologyJson;
        }

        /// <summary>
        /// This method is expected to run for a long time (lifetime of the connection)
        /// and can never throw. We expect this to be on a separate thread
        /// </summary>
        public void AcceptNewConnection(Stream stream, Action<RachisHello> sayHello = null)
        {
            RemoteConnection remoteConnection = null;
            try
            {
                remoteConnection = new RemoteConnection(_tag, stream);
                try
                {
                    RachisHello initialMessage;
                    ClusterTopology clusterTopology;
                    using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    {
                        initialMessage = remoteConnection.InitFollower(context);
                        sayHello?.Invoke(initialMessage);
                        using (context.OpenReadTransaction())
                        {
                            clusterTopology = GetTopology(context);
                        }
                    }

                    if (initialMessage.TopologyId != clusterTopology.TopologyId &&
                        string.IsNullOrEmpty(clusterTopology.TopologyId) == false)
                    {
                        throw new TopologyMismatchException(
                            $"{initialMessage.DebugSourceIdentifier} attempted to connect to us with topology id {initialMessage.TopologyId} but our topology id is already set ({clusterTopology.TopologyId}). " +
                            "Rejecting connection from outside our cluster, this is likely an old server trying to connect to us.");
                    }
                    if (_tag == InitialTag)
                    {
                        using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
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
                                $"{initialMessage.DebugSourceIdentifier} attempted to connect to us with tag {initialMessage.DebugDestinationIdentifier} but our tag is already set ({_tag}). " +
                                "Rejecting connection from confused server, this is likely an old server trying to connect to us, or bad network configuration.");
                        }
                    }
                    _clusterId = initialMessage.TopologyId;

                    switch (initialMessage.InitialMessageType)
                    {
                        case InitialMessageType.RequestVote:
                            var elector = new Elector(this, remoteConnection);
                            elector.HandleVoteRequest();
                            break;
                        case InitialMessageType.AppendEntries:
                            var follower = new Follower(this, remoteConnection);
                            follower.TryAcceptConnection();
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
                        using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
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
                }

                try
                {
                    stream?.Dispose();
                }
                catch (Exception)
                {
                }
            }
        }

        public unsafe long InsertToLeaderLog(TransactionOperationContext context, BlittableJsonReaderObject cmd,
            RachisEntryFlags flags)
        {
            Debug.Assert(context.Transaction != null);
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
                tvb.Add(CurrentTerm);
                tvb.Add(cmd.BasePointer, cmd.Size);
                tvb.Add((int)flags);
                table.Insert(tvb);
            }
            return lastIndex;
        }

        public unsafe void ClearLogEntriesAndSetLastTruncate(TransactionOperationContext context, long index, long term)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            while (true)
            {
                if (table.SeekOnePrimaryKey(Slices.BeforeAllKeys, out TableValueReader reader) == false)
                    break;

                table.Delete(reader.Id);
            }
            var state = context.Transaction.InnerTransaction.CreateTree(GlobalStateSlice);
            using (state.DirectAdd(LastTruncatedSlice, sizeof(long) * 2, out byte* ptr))
            {
                var data = (long*)ptr;
                data[0] = index;
                data[1] = term;
            }
        }
        public unsafe void TruncateLogBefore(TransactionOperationContext context, long upto)
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
            }
            var state = context.Transaction.InnerTransaction.CreateTree(GlobalStateSlice);
            using (state.DirectAdd(LastTruncatedSlice, sizeof(long) * 2, out byte* ptr))
            {
                var data = (long*)ptr;
                data[0] = entryIndex;
                data[1] = entryTerm;
            }
        }

        public unsafe BlittableJsonReaderObject AppendToLog(TransactionOperationContext context,
            List<RachisEntry> entries)
        {
            Debug.Assert(entries.Count > 0);
            Debug.Assert(context.Transaction != null);
            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);

            long reversedEntryIndex = -1;

            BlittableJsonReaderObject lastTopology = null;

            using (
    Slice.External(context.Transaction.InnerTransaction.Allocator, (byte*)&reversedEntryIndex, sizeof(long),
        out Slice key))
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
                            Log.Info($"Got an entry with index={entry.Index} and term={entry.Term} while our term for that index is {entryTerm}," +
                                     $"will rewind last entry index to {lastEntryIndex}");
                        }
                        break;
                    }
                    if (entry.Index > lastEntryIndex)
                        break;

                    firstIndexInEntriesThatWeHaveNotSeen++;
                }
                if (firstIndexInEntriesThatWeHaveNotSeen >= entries.Count)
                    return null; // we have all of those entries in our log, so we can safely ignore them

                var firstEntry = entries[firstIndexInEntriesThatWeHaveNotSeen];
                //While we do support the case where we get the same entries, we expect them to have the same index/term up to the commit index.
                if (firstEntry.Index < lastCommitIndex)
                {
                    ThrowFatalError(firstEntry, lastCommitIndex, lastCommitTerm);
                }
                var prevIndex = lastEntryIndex;

                for (var index = firstIndexInEntriesThatWeHaveNotSeen; index < entries.Count; index++)
                {
                    var entry = entries[index];
                    if (entry.Index != prevIndex + 1)
                    {
                        throw new InvalidOperationException(
                            $"Gap in the entries, prev was {prevIndex} but now trying {entry.Index}");
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
                    }
                    else
                    {
                        nested.Dispose();
                    }
                }
            }
            return lastTopology;
        }

        private void ThrowFatalError(RachisEntry firstEntry, long lastCommitIndex, long lastCommitTerm)
        {
            var message =
                $"FATAL ERROR: got an append entries request with index={firstEntry.Index} term={firstEntry.Term} " +
                $"while my commit index={lastCommitIndex} with term={lastCommitTerm}, this means something went wrong badly.";
            if (Log.IsInfoEnabled)
            {
                Log.Info(message);
            }
            throw new InvalidOperationException(message);
        }

        internal static void GetLastTruncated(TransactionOperationContext context, out long lastTruncatedIndex,
            out long lastTruncatedTerm)
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


        public unsafe BlittableJsonReaderObject GetEntry(TransactionOperationContext context, long index,
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
                return new BlittableJsonReaderObject(ptr, size, context);
            }
        }

        public long GetLastCommitIndex(TransactionOperationContext context)
        {
            Debug.Assert(context.Transaction != null);

            var state = context.Transaction.InnerTransaction.ReadTree(GlobalStateSlice);
            var read = state.Read(LastCommitSlice);
            if (read == null)
                return 0;
            return read.Reader.ReadLittleEndianInt64();
        }

        public void GetLastCommitIndex(TransactionOperationContext context, out long index, out long term)
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



        public unsafe void SetLastCommitIndex(TransactionOperationContext context, long index, long term)
        {
            Debug.Assert(context.Transaction != null);

            var state = context.Transaction.InnerTransaction.ReadTree(GlobalStateSlice);
            var read = state.Read(LastCommitSlice);
            if (read != null)
            {
                var reader = read.Reader;
                var oldIndex = reader.ReadLittleEndianInt64();
                if (oldIndex > index)
                    throw new InvalidOperationException(
                        $"Cannot reduce the last commit index (is {oldIndex} but was requested to reduce to {index})");
                if (oldIndex == index)
                {
                    var oldTerm = reader.ReadLittleEndianInt64();
                    if (oldTerm != term)
                        throw new InvalidOperationException(
                            $"Cannot change just the last commit index (is {oldIndex} term, was {oldTerm} but was requested to change it to {term})");
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
                using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
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
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(nameof(modification), modification, null);
                    }
                }

                if (timeoutTask == await Task.WhenAny(task, timeoutTask))
                    ThrowTimeoutException();
            }
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

        public unsafe long GetFirstEntryIndex(TransactionOperationContext context)
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

        public unsafe long GetLastEntryIndex(TransactionOperationContext context)
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

        public long GetTermForKnownExisting(TransactionOperationContext context, long index)
        {
            var termFor = GetTermFor(context, index);
            if (termFor == null)
                throw new InvalidOperationException("Expected the index " + index +
                                                    " to have a term in the entries, but got null");
            return termFor.Value;
        }

        public unsafe long? GetTermFor(TransactionOperationContext context, long index)
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

        public void FoundAboutHigherTerm(long term)
        {
            if (term == CurrentTerm)
                return;

            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    // we check it here again because now we are under the tx lock, so we can't get into concurrency issues
                    if (term == CurrentTerm)
                        return;

                    CastVoteInTerm(context, term, votedFor: null);

                    tx.Commit();
                }
            }
        }

        public unsafe void CastVoteInTerm(TransactionOperationContext context, long term, string votedFor)
        {
            Debug.Assert(context.Transaction != null);
            if (term <= CurrentTerm)
                throw new ConcurrencyException($"The current term {CurrentTerm} is larger than {term}, aborting change " + Environment.StackTrace);

            var state = context.Transaction.InnerTransaction.CreateTree(GlobalStateSlice);
            using (state.DirectAdd(CurrentTermSlice, sizeof(long), out byte* ptr))
            {
                *(long*)ptr = term;
            }

            votedFor = votedFor ?? String.Empty;

            var size = Encoding.UTF8.GetByteCount(votedFor);

            using (state.DirectAdd(VotedForSlice, size, out var ptr))
            {
                fixed (char* pVotedFor = votedFor)
                {
                    Encoding.UTF8.GetBytes(pVotedFor, votedFor.Length, ptr, size);
                }
            }

            CurrentTerm = term;
        }

        public string GetWhoGotMyVoteIn(TransactionOperationContext context, long term)
        {
            Debug.Assert(context.Transaction != null);

            var state = context.Transaction.InnerTransaction.CreateTree(GlobalStateSlice);
            var read = state.Read(CurrentTermSlice);

            var votedTerm = read?.Reader.ReadLittleEndianInt64();

            if (votedTerm != term)
                return null;

            read = state.Read(VotedForSlice);

            return read?.Reader.ReadString(read.Reader.Length);
        }

        public event EventHandler OnDispose;

        public bool IsDisposed => _disposeEvent.IsSet;

        public virtual void Dispose()
        {
            _disposeEvent.Set();
            OnDispose?.Invoke(this, EventArgs.Empty);
            _topologyChanged.TrySetCanceled();
            _stateChanged.TrySetCanceled();
            _commitIndexChanged.TrySetCanceled();
            ContextPool?.Dispose();
        }

        public abstract Task<Stream> ConnectToPeer(string url, X509Certificate2 certificate, TransactionOperationContext context = null);

        public void Bootstrap(string selfUrl, bool forNewCluster = false)
        {
            if (selfUrl == null)
                throw new ArgumentNullException(nameof(selfUrl));

            using (ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (var tx = ctx.OpenWriteTransaction())
            {
                if (CurrentState != State.Passive && forNewCluster == false)
                    return;

                var lastNode = _tag == InitialTag ? "A" : GetTopology(ctx).LastNodeId;
                // If we were kicked out form a cluster we want to keep the old cluster's tag and lastNode
                // but if we are new born we will set our tag to A. 
                if (forNewCluster == false && _tag == InitialTag)
                {
                    UpdateNodeTag(ctx, "A");
                }

                var topology = new ClusterTopology(
                    Guid.NewGuid().ToString(),
                    new Dictionary<string, string>
                    {
                        [_tag] = selfUrl
                    },
                    new Dictionary<string, string>(),
                    new Dictionary<string, string>(),
                    lastNode
                );

                SetTopology(null, ctx, topology);

                SwitchToSingleLeader(ctx);

                tx.Commit();
            }
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
                throw new NotLeadingException("There is no leader, cannot accept commands. " + _lastStateChangeReason);

            Task task;
            while (leader.TryModifyTopology(nodeTag, nodeUrl, modification, out task, validateNotInTopology) == false)
                await task;

            await task;
          
        }

        private volatile string _leaderTag;
        public string LeaderTag
        {
            get
            {
                switch (CurrentState)
                {
                    case State.Passive:
                        return null;
                    case State.Candidate:
                        return null;
                    case State.Follower:
                        return _leaderTag;
                    case State.LeaderElect:
                    case State.Leader:
                        return _tag;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            internal set
            {
                _leaderTag = value;
            }
        }

        public bool IsEncrypted => _persistentState.Options.EncryptionEnabled;

        public abstract X509Certificate2 ClusterCertificate { get; }

        public abstract bool ShouldSnapshot(Slice slice, RootObjectType type);

        public abstract void Apply(TransactionOperationContext context, long uptoInclusive, Leader leader);

        public abstract void SnapshotInstalled(TransactionOperationContext context, long lastIncludedIndex);

        private readonly AsyncManualResetEvent _leadershipTimeChanged = new AsyncManualResetEvent();
        private int _heartbeatWaitersCounter;

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

        public void ReportLeaderTime(long leaderTime)
        {
            Interlocked.Exchange(ref _leaderTime, leaderTime);

            if (_heartbeatWaitersCounter == 0)
                return;

            _leadershipTimeChanged.Set();
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

        public void UpdateNodeTag(TransactionOperationContext context, string newTag)
        {
            _tag = newTag;

            using (Slice.From(context.Transaction.InnerTransaction.Allocator, _tag, out Slice str))
            {
                var state = context.Transaction.InnerTransaction.CreateTree(GlobalStateSlice);
                state.Add(TagSlice, str);
            }

        }

        public void LeaderElectToLeaderChanged()
        {
            LeaderElected?.Invoke(null, null);
        }
    }

    public class TopologyMismatchException : Exception
    {
        public TopologyMismatchException() { }
        public TopologyMismatchException(string message) : base(message) { }
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
