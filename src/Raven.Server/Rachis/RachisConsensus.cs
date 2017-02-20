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
using Raven.Client.Exceptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
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

        protected override void InitializeState()
        {
            StateMachine = new TStateMachine();
            StateMachine.Initialize(this);
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

        public int ElectionTimeoutMs = 300;
        private Leader _currentLeader;

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

                    InitializeState();

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

        protected abstract void InitializeState();

        public void SetNewState(State state, IDisposable disposable)
        {
            lock (_disposables)
            {
                _currentLeader = null;
                foreach (var t in _disposables)
                {
                    t.Dispose();
                }

                _disposables.Clear();

                if (disposable != null)
                {
                    CurrentState = state;
                    _disposables.Add(disposable);
                }
                else // if we are back to null state, wait to become leader
                {
                    CurrentState = state;
                    Timeout.Start(SwitchToCandidateState);
                }
            }
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
            SetNewState(State.Leader, leader);
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
            var topologyJson = SetTopology(tx, context, topology);

            return topologyJson;
        }

        private static unsafe BlittableJsonReaderObject SetTopology(Transaction tx, JsonOperationContext context, ClusterTopology topology)
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

            SetTopology(tx, topologyJson);
            return topologyJson;
        }

        public static unsafe void SetTopology(Transaction tx, BlittableJsonReaderObject topologyJson)
        {
            var state = tx.CreateTree(GlobalStateSlice);
            var ptr = state.DirectAdd(TopologySlice, topologyJson.Size);
            topologyJson.CopyTo(ptr);
        }

        public string GetDebugInformation()
        {
            // TODO: Full debug information (maching name, port, ip, etc)
            return Environment.MachineName;
        }

        /// <summary>
        /// This method is expected to run for a long time (lifetime of the connection)
        /// and can never throw. We expect this to be on a separate thread
        /// </summary>
        public void AcceptNewConnection(TcpClient tcpClient)
        {
            RemoteConnection remoteConnection = null;
            try
            {
                remoteConnection = new RemoteConnection(tcpClient.GetStream());
                try
                {
                    RachisHello initialMessage;
                    ClusterTopology clusterTopology;
                    TransactionOperationContext context;
                    using (ContextPool.AllocateOperationContext(out context))
                    {
                        initialMessage = remoteConnection.InitFollower(context);
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
                lastIndex = 0;
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
            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            while (true)
            {
                TableValueReader reader;
                if (table.SeekOnePrimaryKey(Slices.BeforeAllKeys, out reader) == false)
                    break;

                if (table.NumberOfEntries == 1)
                    break; // we always leave at least the last entry here, so GetLastEntyIndex will work

                int size;
                var entryIndex = Bits.SwapBytes(*(long*)reader.Read(0, out size));
                if (entryIndex > upto)
                    break;

                table.Delete(reader.Id);
            }
        }

        public unsafe void AppendToLog(TransactionOperationContext context, List<RachisEntry> entries)
        {
            Debug.Assert(entries.Count > 0);
            Debug.Assert(context.Transaction != null);
            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);

            long reversedEntryIndex = Bits.SwapBytes(entries[0].Index - 1);

            Slice key;
            using (Slice.External(context.Transaction.InnerTransaction.Allocator, (byte*)&reversedEntryIndex, sizeof(long), out key))
            {
                long prevIndex;
                TableValueReader reader;
                if (reversedEntryIndex != 0)
                {
                    if (table.ReadByKey(key, out reader) == false)
                        throw new InvalidOperationException(
                            $"Was asked to append {entries[0].Index} but couldn\'t find {(entries[0].Index - 1)} in the log");

                    prevIndex = entries[0].Index - 1;
                }
                else
                {
                    prevIndex = 0;
                }
                for (var index = 0; index < entries.Count; index++)
                {
                    var entry = entries[index];
                    if (entry.Index != prevIndex + 1)
                    {
                        throw new InvalidOperationException($"Gap in the entries, prev was {prevIndex} but now trying {entry.Index}");
                    }
                    prevIndex = entry.Index;
                    reversedEntryIndex = Bits.SwapBytes(entry.Index);
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

                    using (var nested = context.ReadObject(entry.Entry, "entry"))
                    {
                        table.Insert(new TableValueBuilder
                        {
                            reversedEntryIndex,
                            entry.Term,
                            {nested.BasePointer, nested.Size},
                            (int)entry.Flags
                        });
                    }
                }
            }
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
                flags = *(RachisEntryFlags*) reader.Read(3, out size);
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
            index = read.Reader.ReadLittleEndianInt64();
            term = read.Reader.ReadLittleEndianInt64();
        }

        public unsafe void SetLastCommitIndex(TransactionOperationContext context, long index, long term)
        {
            Debug.Assert(context.Transaction != null);

            var state = context.Transaction.InnerTransaction.ReadTree(GlobalStateSlice);
            var read = state.Read(LastCommitSlice);
            if (read != null)
            {
                var oldValue = read.Reader.ReadLittleEndianInt64();
                if (oldValue >= index)
                    throw new InvalidOperationException(
                        $"Cannot reduce the last commit index (is {oldValue} but was requested to reduce to {index})");
            }

            var data = (long*)state.DirectAdd(LastCommitSlice, sizeof(long)*2);
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
                return 0;
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
                return 0;
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

        public long? GetTermFor(long index)
        {
            TransactionOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                return GetTermFor(context, index);
            }
        }

        public static unsafe long? GetTermFor(TransactionOperationContext context, long index)
        {
            Debug.Assert(context.Transaction != null);
            var table = context.Transaction.InnerTransaction.OpenTable(LogsTable, EntriesSlice);
            var reversedIndex = Bits.SwapBytes(index);
            Slice key;
            using (Slice.External(context.Transaction.InnerTransaction.Allocator, (byte*)&reversedIndex, sizeof(long), out key))
            {
                TableValueReader reader;
                if (table.ReadByKey(key, out reader) == false)
                    return null;
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

                        SetTopology(tx, ctx, topology);

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

    }
}