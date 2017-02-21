using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Server.Kestrel.Internal;
using Org.BouncyCastle.Crypto;
using Raven.Client.Exceptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Data;
using Voron.Data.Tables;
using Voron.Impl;
using Voron.Util;

namespace Raven.Server.Rachis
{
    public class RachisConsensus<TStateMachine> : RachisConsensus
        where TStateMachine : RachisStateMachine, new()
    {
        public RachisConsensus(StorageEnvironmentOptions options, string url) : base(options, url)
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
            SetNewState(State.Follower, new NullDisposable());
            StateMachine?.Dispose();
            base.Dispose();
        }

        public override void Apply(TransactionOperationContext context, long uptoInclusive)
        {
            StateMachine.Apply(context, uptoInclusive);
        }

        public override bool ShouldSnapshot(Slice slice, RootObjectType type)
        {
            return StateMachine.ShouldSnapshot(slice, type);
        }

        public override void SnapshotInstalled(TransactionOperationContext context)
        {
            StateMachine.OnSnapshotInstalled(context);
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

        private readonly StorageEnvironmentOptions _options;
        private readonly string _url;
        public TransactionContextPool ContextPool { get; private set; }
        private StorageEnvironment _persistentState;
        internal readonly Logger Log;

        private readonly List<IDisposable> _disposables = new List<IDisposable>();


        public long CurrentTerm { get; private set; }
        public string Url => _url;
        public string TempPath => _options.TempPath;

        private static readonly Slice GlobalStateSlice;
        private static readonly Slice CurrentTermSlice;
        private static readonly Slice VotedForSlice;
        private static readonly Slice LastCommitSlice;
        private static readonly Slice LastTruncatedSlice;
        private static readonly Slice TopologySlice;


        internal static readonly Slice EntriesSlice;
        internal static readonly TableSchema LogsTable;

        static RachisConsensus()
        {
            Slice.From(StorageEnvironment.LabelsContext, "GlobalState", out GlobalStateSlice);

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

        public int ElectionTimeoutMs = Debugger.IsAttached ? 3000 : 300;

        private Leader _currentLeader;
        private TaskCompletionSource<object> _topologyChanged = new TaskCompletionSource<object>();
        private TaskCompletionSource<object> _stateChanged = new TaskCompletionSource<object>();

        protected RachisConsensus(StorageEnvironmentOptions options, string url)
        {
            _options = options;
            _url = url;
            Log = LoggingSource.Instance.GetLogger<RachisConsensus>(options.BasePath);
        }

        public unsafe void Initialize()
        {
            try
            {
                _persistentState = new StorageEnvironment(_options);
                ContextPool = new TransactionContextPool(_persistentState);

                ClusterTopology topology;
                TransactionOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenWriteTransaction())
                {
                    LogsTable.Create(tx.InnerTransaction, EntriesSlice, 16);

                    var state = tx.InnerTransaction.CreateTree(GlobalStateSlice);
                    var read = state.Read(CurrentTermSlice);
                    if (read == null || read.Reader.Length != sizeof(long))
                        *(long*)state.DirectAdd(CurrentTermSlice, sizeof(long)) = CurrentTerm = 0;
                    else
                        CurrentTerm = read.Reader.ReadLittleEndianInt64();

                    topology = GetTopology(context);

                    InitializeState(context);

                    tx.Commit();
                }

                Timeout = new TimeoutEvent(new Random().Next((ElectionTimeoutMs / 3) * 2, ElectionTimeoutMs));

                // if we don't have a topology id, then we are passive
                // an admin needs to let us know that it is fine, either
                // by explicit bootstraping or by connecting us to a cluster
                if (topology.TopologyId == null ||
                    topology.Voters.Contains(_url) == false)
                {
                    CurrentState = State.Passive;
                    return;
                }

                CurrentState = State.Follower;
                if (topology.Voters.Length == 1)
                {
                    using (ContextPool.AllocateOperationContext(out context))
                    using (var tx = context.OpenWriteTransaction())
                    {

                        CastVoteInTerm(context, CurrentTerm + 1, Url);

                        tx.Commit();
                    }
                    SwitchToLeaderState();
                }
                else
                    Timeout.Start(SwitchToCandidateState);
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        protected abstract void InitializeState(TransactionOperationContext context);


        public async Task WaitForState(State state)
        {
            while (true)
            {
                var task = _stateChanged.Task;
                if (CurrentState == state)
                    return;
                await task;
            }
        }


        public async Task WaitForTopology(Leader.TopologyModification modification)
        {
            while (true)
            {
                var task = _topologyChanged.Task;
                TransactionOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                using (context.OpenReadTransaction())
                {
                    var clusterTopology = GetTopology(context);
                    switch (modification)
                    {
                        case Leader.TopologyModification.Voter:
                            if (clusterTopology.Voters.Contains(_url))
                                return;
                            break;
                        case Leader.TopologyModification.Promotable:
                            if (clusterTopology.Promotables.Contains(_url))
                                return;
                            break;
                        case Leader.TopologyModification.NonVoter:
                            if (clusterTopology.NonVotingMembers.Contains(_url))
                                return;
                            break;
                        case Leader.TopologyModification.Remove:
                            if (clusterTopology.Voters.Contains(_url) == false &&
                                clusterTopology.Promotables.Contains(_url) == false &&
                                clusterTopology.NonVotingMembers.Contains(_url) == false)
                                return;
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(nameof(modification), modification, null);
                    }
                }

                await task;
            }
        }

        public void SetNewState(State state, IDisposable disposable)
        {
            List<IDisposable> toDispose;

            lock (_disposables)
            {
                _currentLeader = null;
                toDispose = new List<IDisposable>(_disposables);

                _disposables.Clear();

                if (disposable != null)
                    _disposables.Add(disposable);
                else // if we are back to null state, wait to become candidate if no one talks to us
                    Timeout.Start(SwitchToCandidateState);
            }

            UpdateState(state);

            foreach (var t in toDispose)
            {
                t.Dispose();
            }
        }

        public void TakeOffice()
        {
            if (CurrentState != State.LeaderElect)
                return;

            UpdateState(State.Leader);
        }


        private void UpdateState(State state)
        {
            CurrentState = state;
            ThreadPool.QueueUserWorkItem(
                _ => { Interlocked.Exchange(ref _stateChanged, new TaskCompletionSource<object>()).TrySetResult(null); });
        }

        public void AppendStateDisposable(IDisposable parentState, IDisposable disposeOnStateChange)
        {
            lock (_disposables)
            {
                if (ReferenceEquals(_disposables[0], parentState) == false)
                    throw new ConcurrencyException(
                        "Could not set the disposeOnStateChange because by the time we did it the parent state has changed");
                _disposables.Add(disposeOnStateChange);
            }
        }

        public void SwitchToLeaderState()
        {
            if (Log.IsInfoEnabled)
            {
                Log.Info("Switching to leader state");
            }
            var leader = new Leader(this);
            SetNewState(State.LeaderElect, leader);
            _currentLeader = leader;
            leader.Start();
        }

        public Task PutAsync(BlittableJsonReaderObject cmd)
        {
            var leader = _currentLeader;
            if (leader == null)
                throw new InvalidOperationException("Not a leader, cannot accept commands");

            return leader.PutAsync(cmd);
        }

        public void SwitchToCandidateState()
        {
            TransactionOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var clusterTopology = GetTopology(context);
                if (clusterTopology.TopologyId == null ||
                    clusterTopology.Voters.Contains(_url) == false)
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
            var candidate = new Candidate(this);
            SetNewState(State.Candidate, candidate);
            candidate.Start();
        }

        public unsafe ClusterTopology GetTopology(TransactionOperationContext context)
        {
            Debug.Assert(context.Transaction != null);
            var state = context.Transaction.InnerTransaction.ReadTree(GlobalStateSlice);
            var read = state.Read(TopologySlice);
            if (read == null)
                return new ClusterTopology(null, null, new string[0], new string[0], new string[0]);

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

        private static BlittableJsonReaderObject SetTopology(RachisConsensus engine, Transaction tx, JsonOperationContext context, ClusterTopology topology)
        {
            var djv = new DynamicJsonValue
            {
                [nameof(ClusterTopology.TopologyId)] = topology.TopologyId,
                [nameof(ClusterTopology.ApiKey)] = topology.ApiKey,
                [nameof(ClusterTopology.Voters)] = new DynamicJsonArray(topology.Voters),
                [nameof(ClusterTopology.Promotables)] = new DynamicJsonArray(topology.Promotables),
                [nameof(ClusterTopology.NonVotingMembers)] = new DynamicJsonArray(topology.NonVotingMembers),
            };

            var topologyJson = context.ReadObject(djv, "topology");

            SetTopology(engine, tx, topologyJson);

            return topologyJson;
        }

        public static unsafe void SetTopology(RachisConsensus engine, Transaction tx, BlittableJsonReaderObject topologyJson)
        {
            var state = tx.CreateTree(GlobalStateSlice);
            var ptr = state.DirectAdd(TopologySlice, topologyJson.Size);
            topologyJson.CopyTo(ptr);

            if (engine == null)
                return;


            tx.LowLevelTransaction.OnDispose += _ =>
            {
                Interlocked.Exchange(ref engine._topologyChanged, new TaskCompletionSource<object>()).TrySetResult(null);
            };


        }

        public string GetDebugInformation()
        {
            return _url;
        }

        /// <summary>
        /// This method is expected to run for a long time (lifetime of the connection)
        /// and can never throw. We expect this to be on a separate thread
        /// </summary>
        public void AcceptNewConnection(TcpClient tcpClient, Action<RachisHello> sayHello = null)
        {
            RemoteConnection remoteConnection = null;
            try
            {
                remoteConnection = new RemoteConnection(_url, tcpClient.GetStream());
                try
                {
                    RachisHello initialMessage;
                    ClusterTopology clusterTopology;
                    TransactionOperationContext context;
                    using (ContextPool.AllocateOperationContext(out context))
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
                        throw new InvalidOperationException(
                            $"{initialMessage.DebugSourceIdentifier} attempted to connect to us with topology id {initialMessage.TopologyId} but our topology id is already set ({clusterTopology.TopologyId}). " +
                            $"Rejecting connection from outside our cluster, this is likely an old server trying to connect to us.");
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

                    //initialMessage.AppendEntries
                    // validate that can handle this
                    // start listening thread
                }
                catch (Exception e)
                {
                    TransactionOperationContext context;
                    using (ContextPool.AllocateOperationContext(out context))
                        remoteConnection.Send(context, e);
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
                    tcpClient?.Dispose();
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

            TableValueReader reader;
            if (table.SeekOnePrimaryKey(Slices.AfterAllKeys, out reader))
            {
                int size;
                lastIndex = Bits.SwapBytes(*(long*)reader.Read(0, out size));
                Debug.Assert(size == sizeof(long));
            }
            else
            {
                long lastIndexTerm;
                GetLastTruncated(context, out lastIndex, out lastIndexTerm);
            }
            lastIndex += 1;
            var tvb = new TableValueBuilder
            {
                Bits.SwapBytes(lastIndex),
                CurrentTerm,
                {cmd.BasePointer, cmd.Size},
                (int) flags
            };
            table.Insert(tvb);

            return lastIndex;
        }

        public unsafe void TruncateLogBefore(TransactionOperationContext context, long upto)
        {
            long lastIndex;
            long lastTerm;
            GetLastCommitIndex(context, out lastIndex, out lastTerm);

            long entryTerm;
            long entryIndex;

            if (lastIndex < upto)
            {
                upto = lastIndex;// max we can delete
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
                TableValueReader reader;
                if (table.SeekOnePrimaryKey(Slices.BeforeAllKeys, out reader) == false)
                    break;

                int size;
                entryIndex = Bits.SwapBytes(*(long*)reader.Read(0, out size));
                if (entryIndex > upto)
                    break;
                Debug.Assert(size == sizeof(long));

                entryTerm = *(long*)reader.Read(1, out size);
                Debug.Assert(size == sizeof(long));

                table.Delete(reader.Id);
            }
            var state = context.Transaction.InnerTransaction.CreateTree(GlobalStateSlice);
            var data = (long*)state.DirectAdd(LastTruncatedSlice, sizeof(long) * 2);
            data[0] = entryIndex;
            data[1] = entryTerm;
        }

        public unsafe BlittableJsonReaderObject AppendToLog(TransactionOperationContext context, List<RachisEntry> entries)
        {
            Debug.Assert(entries.Count > 0);
            Debug.Assert(context.Transaction != null);
            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);

            long reversedEntryIndex = -1;

            BlittableJsonReaderObject lastTopology = null;

            Slice key;
            using (Slice.External(context.Transaction.InnerTransaction.Allocator, (byte*)&reversedEntryIndex, sizeof(long), out key))
            {
                var lastEntryIndex = GetLastEntryIndex(context);
                var firstIndexInEntriesThatWeHaveNotSeen = 0;
                foreach (var entry in entries)
                {
                    if (entry.Index > lastEntryIndex)
                        break;

                    firstIndexInEntriesThatWeHaveNotSeen++;
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
                    reversedEntryIndex = Bits.SwapBytes(entry.Index);
                    TableValueReader reader;
                    if (table.ReadByKey(key, out reader)) // already exists
                    {
                        int size;
                        var term = *(long*)reader.Read(1, out size);
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
                    table.Insert(new TableValueBuilder
                    {
                        reversedEntryIndex,
                        entry.Term,
                        {nested.BasePointer, nested.Size},
                        (int) entry.Flags
                    });
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

        private static void GetLastTruncated(TransactionOperationContext context, out long lastTruncatedIndex, out long lastTruncatedTerm)
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


        public unsafe BlittableJsonReaderObject GetEntry(TransactionOperationContext context, long index, out RachisEntryFlags flags)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            var reversedIndex = Bits.SwapBytes(index);
            Slice key;
            using (Slice.External(context.Allocator, (byte*)&reversedIndex, sizeof(long), out key))
            {
                TableValueReader reader;
                if (table.ReadByKey(key, out reader) == false)
                {
                    flags = RachisEntryFlags.Invalid;
                    return null;
                }
                int size;
                flags = *(RachisEntryFlags*)reader.Read(3, out size);
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
                            $"Cannot change just the last commit index (is {oldIndex} term, was {oldTerm} but was requested to change it ot {term})");
                }
            }

            var data = (long*)state.DirectAdd(LastCommitSlice, sizeof(long) * 2);
            data[0] = index;
            data[1] = term;
        }

        public unsafe Tuple<long, long> GetLogEntriesRange(TransactionOperationContext context)
        {
            Debug.Assert(context.Transaction != null);

            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            TableValueReader reader;
            if (table.SeekOnePrimaryKey(Slices.AfterAllKeys, out reader) == false)
                return Tuple.Create(0L, 0L);
            int size;
            var max = Bits.SwapBytes(*(long*)reader.Read(0, out size));
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
            TableValueReader reader;
            if (table.SeekOnePrimaryKey(Slices.BeforeAllKeys, out reader) == false)
            {
                long lastTruncatedIndex;
                long _;
                GetLastTruncated(context, out lastTruncatedIndex, out _);
                return lastTruncatedIndex;
            }
            int size;
            var max = Bits.SwapBytes(*(long*)reader.Read(0, out size));
            Debug.Assert(size == sizeof(long));
            return max;
        }

        public unsafe long GetLastEntryIndex(TransactionOperationContext context)
        {
            Debug.Assert(context.Transaction != null);

            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            TableValueReader reader;
            if (table.SeekOnePrimaryKey(Slices.AfterAllKeys, out reader) == false)
            {
                long lastTruncatedIndex;
                long _;
                GetLastTruncated(context, out lastTruncatedIndex, out _);
                return lastTruncatedIndex;
            }
            int size;
            var max = Bits.SwapBytes(*(long*)reader.Read(0, out size));
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
            Slice key;
            using (Slice.External(context.Transaction.InnerTransaction.Allocator, (byte*)&reversedIndex, sizeof(long), out key))
            {
                TableValueReader reader;
                if (table.ReadByKey(key, out reader) == false)
                {
                    long lastIndex;
                    long lastTerm;
                    GetLastCommitIndex(context, out lastIndex, out lastTerm);
                    if (lastIndex == index)
                        return lastTerm;
                    GetLastTruncated(context, out lastIndex, out lastTerm);
                    if (lastIndex == index)
                        return lastTerm;
                    return null;
                }
                int size;
                var term = *(long*)reader.Read(1, out size);
                Debug.Assert(size == sizeof(long));
                return term;
            }
        }

        public void FoundAboutHigherTerm(long term)
        {
            if (term == CurrentTerm)
                return;

            TransactionOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
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
                throw new ConcurrencyException($"The current term {CurrentTerm} is larger than {term}, aborting change");

            var state = context.Transaction.InnerTransaction.CreateTree(GlobalStateSlice);
            *(long*)state.DirectAdd(CurrentTermSlice, sizeof(long)) = term;

            votedFor = votedFor ?? string.Empty;

            var size = Encoding.UTF8.GetByteCount(votedFor);

            var ptr = state.DirectAdd(VotedForSlice, size);
            fixed (char* pVotedFor = votedFor)
            {
                Encoding.UTF8.GetBytes(pVotedFor, votedFor.Length, ptr, size);
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

        public virtual void Dispose()
        {
            ThreadPool.QueueUserWorkItem(_ => _topologyChanged.TrySetCanceled());
            ContextPool?.Dispose();
            _persistentState?.Dispose();
            _options?.Dispose();
        }

        public Stream ConenctToPeer(string url, string apiKey)
        {
            //var serverUrl = new UriBuilder(url)
            //{
            //    Fragment = null // remove debug info
            //}.Uri.ToString();

            //var tcpInfo = ReplicationUtils.GetTcpInfo(serverUrl, null, apiKey);

            var tcpInfo = new Uri(url);
            var tcpClient = new TcpClient();
            tcpClient.ConnectAsync(tcpInfo.Host, tcpInfo.Port).Wait();
            return tcpClient.GetStream();
        }

        public static void Bootstarp(StorageEnvironmentOptions options, string self)
        {
            var old = options.OwnsPagers;
            options.OwnsPagers = false;
            try
            {
                using (var env = new StorageEnvironment(options))
                {
                    using (var tx = env.WriteTransaction())
                    using (var ctx = JsonOperationContext.ShortTermSingleUse())
                    {
                        var topology = new ClusterTopology(Guid.NewGuid().ToString(),
                            null,
                            new string[] { self },
                            new string[0],
                            new string[0]);

                        SetTopology(null, tx, ctx, topology);

                        tx.Commit();
                    }
                }
            }
            finally
            {
                options.OwnsPagers = old;
            }
        }

        public Task AddToClusterAsync(string node)
        {
            return ModifyTopologyAsync(node, Leader.TopologyModification.Promotable);
        }


        public Task RemoveFromClusterAsync(string node)
        {
            return ModifyTopologyAsync(node, Leader.TopologyModification.Remove);
        }

        private Task ModifyTopologyAsync(string newNode, Leader.TopologyModification modification)
        {
            var leader = _currentLeader;
            if (leader == null)
                throw new InvalidOperationException("Not a leader, cannot accept commands");

            Task task;
            if (leader.TryModifyTopology(newNode, modification, out task) == false)
                throw new InvalidOperationException("Cannot run a modification on the topology when one is in progress");

            return task;
        }

        public abstract bool ShouldSnapshot(Slice slice, RootObjectType type);

        public abstract void Apply(TransactionOperationContext context, long uptoInclusive);

        public abstract void SnapshotInstalled(TransactionOperationContext context);
    }
}