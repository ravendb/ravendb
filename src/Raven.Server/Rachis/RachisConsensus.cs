using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance;
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

namespace Raven.Server.Rachis
{
    public class RachisConsensus<TStateMachine> : RachisConsensus
        where TStateMachine : RachisStateMachine, new()
    {
        public RachisConsensus(int? seed = null) : base(seed)
        {
        }

        public TStateMachine StateMachine;

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
            StateMachine.Apply(context, uptoInclusive, leader);
        }

        public override bool ShouldSnapshot(Slice slice, RootObjectType type)
        {
            return StateMachine.ShouldSnapshot(slice, type);
        }

        public override void SnapshotInstalled(TransactionOperationContext context, long lastIncludedIndex)
        {
            StateMachine.OnSnapshotInstalled(context, lastIncludedIndex);
        }

        public override async Task<Stream> ConenctToPeer(string url, string apiKey, TransactionOperationContext context = null)
        {
            return await StateMachine.ConnectToPeer(url, apiKey);
        }

        private class NullDisposable : IDisposable
        {
            public void Dispose()
            {

            }
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

        public State CurrentState { get; private set; }
        public TimeoutEvent Timeout { get; private set; }
        public TimeSpan RemoteOperationTimeout { get; private set; }

        public string LastStateChangeReason => _lastStateChangeReason;

        public event EventHandler<ClusterTopology> TopologyChanged;

        private string _tag;
        public TransactionContextPool ContextPool { get; private set; }
        private StorageEnvironment _persistentState;
        internal Logger Log;

        private readonly List<IDisposable> _disposables = new List<IDisposable>();


        public long CurrentTerm { get; private set; }
        public string Tag => _tag;

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
                StartIndex = 0,
            });
        }

        public TimeSpan ElectionTimeout;

        private Leader _currentLeader;
        private TaskCompletionSource<object> _topologyChanged = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        private TaskCompletionSource<object> _stateChanged = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        private TaskCompletionSource<object> _commitIndexChanged = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ManualResetEventSlim _disposeEvent = new ManualResetEventSlim();
        private int? _seed;
        private string _lastStateChangeReason;

        protected RachisConsensus(int? seed = null)
        {
            _seed = seed;
        }

        public unsafe void Initialize(StorageEnvironment env, ClusterConfiguration configuration)
        {
            try
            {
                _persistentState = env;
                RemoteOperationTimeout = configuration.ClusterOperationTimeout.AsTimeSpan;
                ElectionTimeout = configuration.ElectionTimeout.AsTimeSpan;
                if (Debugger.IsAttached)
                {
                    ElectionTimeout = TimeSpan.FromMilliseconds(ElectionTimeout.TotalMilliseconds * 10);
                }
                ContextPool = new TransactionContextPool(_persistentState);

                ClusterTopology topology;
                using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var state = tx.InnerTransaction.CreateTree(GlobalStateSlice);

                    var readResult = state.Read(TagSlice);
                    _tag = readResult == null ? "?" : readResult.Reader.ToStringValue();

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

                    InitializeState(context);

                    tx.Commit();
                }
                //We want to be able to reproduce rare issues that are related to timing
                var rand = _seed.HasValue ? new Random(_seed.Value) : new Random();
                Timeout = new TimeoutEvent(rand.Next((int)Math.Max(int.MaxValue, ElectionTimeout.TotalMilliseconds / 3 * 2),
                    (int)Math.Max(int.MaxValue, ElectionTimeout.TotalMilliseconds)));

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
                    using (ctx.OpenWriteTransaction())
                    {
                        SwitchToSingleLeader(ctx);
                        ctx.Transaction.Commit();
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
            SetNewStateInTx(context, State.LeaderElect, leader, electionTerm, "I'm the only one in the cluster, so I'm the leader");
            _currentLeader = leader;
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

        public void SetNewState(State state, IDisposable disposable, long expectedTerm, string stateChangedReason)
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenWriteTransaction()) // we use the write transaction lock here
            {
                SetNewStateInTx(context, state, disposable, expectedTerm, stateChangedReason);

                context.Transaction.Commit();
            }
        }

        private void SetNewStateInTx(TransactionOperationContext context, State state, IDisposable disposable, long expectedTerm, string stateChangedReason)
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
            else if (state != State.Passive) // if we are back to null state, wait to become candidate if no one talks to us
                Timeout.Start(SwitchToCandidateStateOnTimeout);

            if (state == State.Passive)
            {
                DeleteTopology(context);
            }

            CurrentState = state;

            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += tx =>
            {
                if (tx is LowLevelTransaction llt && llt.Committed)
                {
                    TaskExecutor.CompleteReplaceAndExecute(ref _stateChanged, () =>
                    {
                        foreach (var d in toDispose)
                        {
                            d.Dispose();
                        }
                    });
                }
            };
        }

        public void TakeOffice()
        {
            if (CurrentState != State.LeaderElect)
                return;

            CurrentState = State.Leader;
            TaskExecutor.CompleteAndReplace(ref _stateChanged);
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
            SetNewState(State.LeaderElect, leader, electionTerm, reason);
            _currentLeader = leader;
            leader.Start();
        }

        public Task<(long, BlittableJsonReaderObject)> PutAsync(BlittableJsonReaderObject cmd)
        {
            var leader = _currentLeader;
            if (leader == null)
                throw new NotLeadingException("Not a leader, cannot accept commands. " + _lastStateChangeReason);

            return leader.PutAsync(cmd);
        }

        public void SwitchToCandidateStateOnTimeout()
        {
            SwitchToCandidateState("Election timeout");
        }

        public void SwitchToCandidateState(string reason, bool forced = false)
        {
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
                Log.Info("Switching to candidate state");
            }

            var candidate = new Candidate(this)
            {
                IsForcedElection = forced
            };

            SetNewState(State.Candidate, candidate, CurrentTerm, reason);
            candidate.Start();
        }

        public void DeleteTopology(TransactionOperationContext context)
        {
            var topology = GetTopology(context);
            var newTopology = new ClusterTopology(
                topology.TopologyId,
                null,
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
            var tx = context.Transaction.InnerTransaction;
            var topologyJson = SetTopology(this, tx, context, topology);

            return topologyJson;
        }
        public static unsafe BlittableJsonReaderObject SetTopology(RachisConsensus engine, Transaction tx, JsonOperationContext context,
            ClusterTopology clusterTopology)
        {
            var topologyJson = context.ReadObject(clusterTopology.ToJson(), "topology");
            var state = tx.CreateTree(GlobalStateSlice);
            using (state.DirectAdd(TopologySlice, topologyJson.Size, out byte* ptr))
            {
                topologyJson.CopyTo(ptr);
            }

            if (engine == null)
                return null;

            tx.LowLevelTransaction.OnDispose += _ =>
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
                            $"Rejecting connection from outside our cluster, this is likely an old server trying to connect to us.");
                    }

                    if (_tag == "?")
                    {
                        using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                        using (context.OpenWriteTransaction())
                        {
                            if (_tag == "?")// double checked locking under tx write lock
                            {
                                UpdateNodeTag(context, initialMessage.DebugDestinationIdentifier);
                                context.Transaction.Commit();
                            }
                        }

                        if (_tag != initialMessage.DebugDestinationIdentifier)
                        {
                            throw new TopologyMismatchException(
                                $"{initialMessage.DebugSourceIdentifier} attempted to connect to us with tag {initialMessage.DebugDestinationIdentifier} but our tag is already set ({_tag}). " +
                                $"Rejecting connection from confused server, this is likely an old server trying to connect to us, or bad network configuration.");
                        }
                    }

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
                            throw new ArgumentOutOfRangeException("Uknown initial message value: " +
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
                GetLastTruncated(context, out lastIndex, out long lastIndexTerm);
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

        public unsafe void ClearLogEntriesAndSetLastTrancate(TransactionOperationContext context, long index, long term)
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
                    return null; // we have all of those entires in our log, so we can safely ignore them

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

                        // we have found a divergence in the log, and we now need to trucate it from this
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

        private static void GetLastTruncated(TransactionOperationContext context, out long lastTruncatedIndex,
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
            var timeoutTask = TimeoutManager.WaitFor(RemoteOperationTimeout);
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

        public unsafe Tuple<long, long> GetLogEntriesRange(TransactionOperationContext context)
        {
            Debug.Assert(context.Transaction != null);

            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            if (table.SeekOnePrimaryKey(Slices.AfterAllKeys, out TableValueReader reader) == false)
                return Tuple.Create(0L, 0L);
            var max = Bits.SwapBytes(*(long*)reader.Read(0, out int size));
            Debug.Assert(size == sizeof(long));
            if (table.SeekOnePrimaryKey(Slices.BeforeAllKeys, out reader) == false)
                return Tuple.Create(0L, 0L);
            var min = Bits.SwapBytes(*(long*)reader.Read(0, out size));
            Debug.Assert(size == sizeof(long));

            return Tuple.Create(min, max);
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

        public abstract Task<Stream> ConenctToPeer(string url, string apiKey, TransactionOperationContext context = null);

        public void Bootstarp(string selfUrl)
        {
            if (selfUrl == null)
                throw new ArgumentNullException(nameof(selfUrl));

            using (ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (var tx = ctx.OpenWriteTransaction())
            {
                if (CurrentState != State.Passive)
                    return;

                UpdateNodeTag(ctx, "A");

                var topology = new ClusterTopology(
                    Guid.NewGuid().ToString(),
                    null,
                    new Dictionary<string, string>
                    {
                        [_tag] = selfUrl
                    },
                    new Dictionary<string, string>(),
                    new Dictionary<string, string>(),
                    "A"
                );

                SetTopology(null, tx.InnerTransaction, ctx, topology);

                SwitchToSingleLeader(ctx);

                tx.Commit();
            }
        }

        public Task AddToClusterAsync(string url)
        {
            return ModifyTopologyAsync(null, url, Leader.TopologyModification.Promotable, true);
        }

        public Task RemoveFromClusterAsync(string nodeTag)
        {
            return ModifyTopologyAsync(nodeTag, null, Leader.TopologyModification.Remove);
        }

        private async Task ModifyTopologyAsync(string nodeTag, string nodeUrl, Leader.TopologyModification modification, bool validateNotInTopology = false)
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
        public abstract bool ShouldSnapshot(Slice slice, RootObjectType type);

        public abstract void Apply(TransactionOperationContext context, long uptoInclusive, Leader leader);

        public abstract void SnapshotInstalled(TransactionOperationContext context, long lastIncludedIndex);

        private readonly AsyncManualResetEvent _leadershipTimeChanged = new AsyncManualResetEvent();
        private int _hasTimers;

        public Task WaitForHeartbeat()
        {
            Interlocked.Increment(ref _hasTimers);
            try
            {
                return _leadershipTimeChanged.WaitAsync();
            }
            finally
            {
                Interlocked.Decrement(ref _hasTimers);
            }
        }

        public async Task WaitForTimeout(long knownLeaderTime, TimeSpan timeout)
        {
            Interlocked.Increment(ref _hasTimers);
            try
            {
                var term = CurrentTerm;
                var task = _leadershipTimeChanged.WaitAsync(timeout);

                while (true)
                {
                    if (term != CurrentTerm)
                        knownLeaderTime = 0;

                    var timePassed = TimeSpan.FromMilliseconds(_leaderTime - knownLeaderTime);
                    if (timePassed > timeout)
                        return;

                    if (await task == false)
                        return;

                    var remaining = timeout.Subtract(timePassed);
                    task = _leadershipTimeChanged.WaitAsync(remaining);
                }
            }
            finally
            {
                Interlocked.Decrement(ref _hasTimers);
            }
        }
        public Task WaitForTimeout(TimeSpan timeout)
        {
            return WaitForTimeout(_leaderTime, timeout);
        }

        private long _leaderTime;

        public void ReportLeaderTime(long leaderTime)
        {
            Interlocked.Exchange(ref _leaderTime, leaderTime);

            if (_hasTimers == 0)
                return;

            _leadershipTimeChanged.SetInAsyncMannerFireAndForget();
        }

        public DynamicJsonArray GetClusterErrorsFromLeader()
        {
            if (_currentLeader == null)
                return new DynamicJsonArray();

            var dja = new DynamicJsonArray();
            while (_currentLeader.ErrorsList.TryDequeue(out var entry))
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