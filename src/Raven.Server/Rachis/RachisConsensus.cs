using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using Raven.Client.Exceptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;
using Voron.Global;

namespace Raven.Server.Rachis
{
    public class RachisConsensus : IDisposable
    {
        private readonly StorageEnvironmentOptions _options;
        public TransactionContextPool ContextPool { get; private set; }
        private StorageEnvironment _persistentState;
        private readonly Logger _log;
        private RachisStateMachine _stateMachine;

        public string DebugCurrentLeader { get; private set; }
        public long CurrentTerm { get; private set; }
        public string TopologyId { get; private set; }

        private static readonly Slice GlobalStateSlice;
        private static readonly Slice CurrentTermSlice;
        private static readonly Slice TopologyIdSlice;
        private static readonly Slice LastAppliedSlice;
        private static readonly Slice LastCommitSlice;


        private static readonly Slice EntriesSlice;
        private static readonly TableSchema _logsTable;

        static RachisConsensus()
        {
            Slice.From(StorageEnvironment.LabelsContext, "GlobalState", out GlobalStateSlice);
            Slice.From(StorageEnvironment.LabelsContext, "CurrentTerm", out CurrentTermSlice);
            Slice.From(StorageEnvironment.LabelsContext, "TopologyId", out TopologyIdSlice);
            Slice.From(StorageEnvironment.LabelsContext, "LastApplied", out LastAppliedSlice);
            Slice.From(StorageEnvironment.LabelsContext, "LastCommit", out LastCommitSlice);

            Slice.From(StorageEnvironment.LabelsContext, "Entries", out EntriesSlice);

            _logsTable = new TableSchema();
            _logsTable.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
            });
        }

        public RachisConsensus(StorageEnvironmentOptions options)
        {
            _options = options;
            _log = LoggingSource.Instance.GetLogger<RachisConsensus>(options.BasePath);
        }

        public unsafe void Initialize(RachisStateMachine stateMachine)
        {
            try
            {
                _stateMachine = stateMachine;
                _persistentState = new StorageEnvironment(_options);
                using (var tx = _persistentState.WriteTransaction())
                {
                    _logsTable.Create(tx, EntriesSlice, 16);

                    var state = tx.CreateTree(GlobalStateSlice);
                    if (state.State.NumberOfEntries == 0)
                    {
                        *(long*)state.DirectAdd(CurrentTermSlice, sizeof(long)) = CurrentTerm = 0;
                        state.Add(TopologyIdSlice, Array.Empty<byte>());
                        TopologyId = null;
                    }
                    else
                    {
                        var read = state.Read(CurrentTermSlice);
                        if (read == null || read.Reader.Length != sizeof(long))
                            throw new InvalidOperationException(
                                "Could not read the current term from persistent storage");
                        CurrentTerm = read.Reader.ReadLittleEndianInt64();
                        read = state.Read(TopologyIdSlice);
                        if (read == null)
                            throw new InvalidOperationException("Could not read the topology id from persistent storage");
                        TopologyId = read.Reader.ReadString(read.Reader.Length);
                    }
                }
                ContextPool = new TransactionContextPool(_persistentState);
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        /// <summary>
        /// This method is expected to run for a long time (as long as we are the leader)
        /// it is responsible for talking to the remote follower and maintaining its state.
        /// This can never throw, and will run on its own thread.
        /// </summary>
        public unsafe void FollowerAmbassador(string debugFollower, Func<Stream> conenctToFollower)
        {
            try
            {
                while (true)
                {
                    //TODO: need a way to shut this down when we are no longer leader / shutting down
                    Stream stream = null;
                    try
                    {
                        try
                        {
                            stream = conenctToFollower();
                        }
                        catch (Exception e)
                        {
                            if (_log.IsInfoEnabled)
                            {
                                _log.Info("Failed to connect to remote follower: " + debugFollower, e);
                            }
                            WaitHeartbeat();
                            continue; // we'll retry connecting
                        }

                        using (var connection = new RemoteConnection(stream))
                        {
                            //TODO: those should be something that is exposed and can be inspected by debug endpoint
                            long followerMatchIndex = 0;
                            long followerNextIndex = 0;

                            if (InitialNegotiationWithFollower(debugFollower, connection, out followerMatchIndex, out followerNextIndex) == false)
                                return;

                            TransactionOperationContext context;
                            using (ContextPool.AllocateOperationContext(out context))
                            {
                                //TODO: implement this
                                connection.Send(context, new InstallSnapshot
                                {
                                    LastIncludedIndex = -1,
                                    LastIncludedTerm = -1,
                                    Topology = -1,//TODO: fake, just to remember doing this
                                    SnapshotSize = 0
                                });
                            }

                            var entries = new List<BlittableJsonReaderObject>();
                            while (true)
                            {
                                // TODO: how to close
                                entries.Clear();
                                using (ContextPool.AllocateOperationContext(out context))
                                {
                                    AppendEntries appendEntries;
                                    using (context.OpenReadTransaction())
                                    {
                                        var table = context.Transaction.InnerTransaction.OpenTable(_logsTable, EntriesSlice);

                                        var reveredNextIndex = Bits.SwapBytes(followerNextIndex);
                                        Slice key;
                                        using (Slice.External(context.Allocator, (byte*)&reveredNextIndex, sizeof(long),
                                                out key))
                                        {
                                            long totalSize = 0;
                                            foreach (var value in table.SeekByPrimaryKey(key))
                                            {
                                                var entry = BuildRachisEntryToSend(context, value);
                                                entries.Add(entry);
                                                totalSize += entry.Size;
                                                if (totalSize > Constants.Size.Megabyte)
                                                    break; // TODO: Configurable?
                                            }

                                            appendEntries = new AppendEntries
                                            {
                                                HasTopologyChange = false, // TODO: figure this out
                                                EntriesCount = entries.Count,
                                                LeaderCommit = GetLastCommitIndex(context),
                                                Term = CurrentTerm,
                                                //TODO: figure out what to do if we have a snapshot that removed all 
                                                //TODO: the entries
                                                PrevLogTerm = GetTermFor(followerNextIndex - 1) ?? 0,
                                                PrevLogIndex = followerNextIndex - 1
                                            };
                                        }
                                    }

                                    // out of the tx, we can do network calls

                                    connection.Send(context, appendEntries, entries);
                                    var aer = connection.Read<AppendEntriesResponse>(context);
                                    
                                    if (aer.Success == false)
                                    {
                                        // shouldn't happen, the connection should be aborted if this is the case, but still
                                        var msg = "A negative Append Entries Response after the connection has been established shouldn't happen. Message: " +
                                                  aer.Message;
                                        if (_log.IsInfoEnabled)
                                        {
                                            _log.Info("BUG? " + msg);
                                        }
                                        throw new InvalidOperationException(msg);
                                    }
                                    Debug.Assert(aer.CurrentTerm == CurrentTerm);
                                    followerMatchIndex = aer.LastLogIndex;
                                    //TODO: publish the follower match index to the leader so it can make decisions
                                    followerNextIndex = aer.LastLogIndex + 1;
                                }

                                WaitHeartbeat();
                            }
                        }
                    }
                    finally
                    {
                        stream?.Dispose();
                    }
                }
            }
            catch (Exception e)
            {
                if (_log.IsInfoEnabled)
                {
                    _log.Info("Failed to talk to remote follower: " + debugFollower, e);
                }
            }
        }

        private static unsafe BlittableJsonReaderObject BuildRachisEntryToSend(TransactionOperationContext context,
            Table.TableValueHolder value)
        {
            BlittableJsonReaderObject entry;
            using (var writer =
                new ManualBlittalbeJsonDocumentBuilder<UnmanagedWriteBuffer>(
                    context, BlittableJsonDocumentBuilder.UsageMode.None))
            {
                writer.StartWriteObjectDocument();
                writer.StartWriteObject();

                writer.WritePropertyName(nameof(RachisEntry.Index));

                int size;
                var index = Bits.SwapBytes(*(long*)value.Reader.Read(0, out size));
                Debug.Assert(size == sizeof(long));
                writer.WriteValue(index);

                writer.WritePropertyName(nameof(RachisEntry.Term));
                var term = *(long*)value.Reader.Read(1, out size);
                Debug.Assert(size == sizeof(long));
                writer.WriteValue(term);

                writer.WritePropertyName(nameof(RachisEntry.Entry));
                writer.WriteEmbeddedBlittableDocument(value.Reader.Read(2, out size), size);


                writer.WriteObjectEnd();
                writer.FinalizeDocument();
                entry = writer.CreateReader();
            }
            return entry;
        }

        private bool InitialNegotiationWithFollower(string debugFollower, RemoteConnection connection, out long followerMatchIndex, out long followerNextIndex)
        {
            TransactionOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                AppendEntries appendEntries;
                using (context.OpenReadTransaction())
                {
                    var range = GetLogEntriesRange(context);
                    appendEntries = new AppendEntries
                    {
                        EntriesCount = 0,
                        HasTopologyChange = false,
                        Term = CurrentTerm,
                        LeaderCommit = GetLastCommitIndex(context),
                        PrevLogIndex = range.Item2,
                        PrevLogTerm = GetTermForKnownExisting(context, range.Item2),
                    };
                }

                connection.Send(context, new RachisHello
                {
                    TopologyId = TopologyId,
                    InitialMessageType = InitialMessageType.AppendEntries,
                    DebugSourceIdentifier = GetDebugInformation()
                });


                connection.Send(context, appendEntries);

                var aer = connection.Read<AppendEntriesResponse>(context);
                if (aer.CurrentTerm != CurrentTerm)
                {
                    //TODO: Step down as leader
                    Debug.Assert(false, "TODO: Step down as leader");
                    followerMatchIndex = -1;
                    followerNextIndex = -1;
                    return false;
                }

                if (aer.Success)
                {
                    followerMatchIndex = aer.LastLogIndex;
                    followerNextIndex = aer.LastLogIndex + 1;
                    return true;
                }

                if (aer.Negotiation == null)
                    throw new InvalidOperationException("BUG: We didn't get a success on first AppendEntries to peer " +
                                                        debugFollower + ", the term match but there is no negotiation");

                // need to negotiate
                do
                {
                    using (context.OpenReadTransaction())
                    {
                        if (aer.Negotiation.MidpointTerm ==
                            GetTermForKnownExisting(context, aer.Negotiation.MidpointIndex))// we know that we have longer / equal log tot the follower
                        {
                            aer.Negotiation.MinIndex = aer.Negotiation.MidpointIndex + 1;
                        }
                        else
                        {
                            aer.Negotiation.MaxIndex = aer.Negotiation.MidpointIndex - 1;
                        }
                        var midIndex = (aer.Negotiation.MinIndex + aer.Negotiation.MaxIndex) / 2;
                        appendEntries = new AppendEntries
                        {
                            EntriesCount = 0,
                            HasTopologyChange = false,
                            Term = CurrentTerm,
                            LeaderCommit = GetLastCommitIndex(context),
                            PrevLogIndex = midIndex,
                            PrevLogTerm = GetTermForKnownExisting(context, midIndex),
                        };
                    }
                    connection.Send(context, appendEntries);
                    aer = connection.Read<AppendEntriesResponse>(context);
                } while (aer.Success == false);

                followerMatchIndex = aer.LastLogIndex;
                followerNextIndex = aer.LastLogIndex + 1;
                return true;
            }
        }

        private string GetDebugInformation()
        {
            // TODO: Full debug information (maching name, port, ip, etc)
            return Environment.MachineName;
        }

        private void WaitHeartbeat()
        {
            //TODO: This need to wait the random timeout
            //TODO: need to abort when shutting down / no longer leading 
            //TODO: Should explciitly be sleeping (to avoid having runnable threads)
            Thread.Sleep(250);
        }

        /// <summary>
        /// This method is expected to run for a long time (lifetime of the connection)
        /// and can never throw. We expect this to be on a separate thread
        /// </summary>
        public void AcceptNewConnection(Stream stream, TcpClient tcpClient)
        {
            RemoteConnection remoteConnection = null;
            try
            {
                remoteConnection = new RemoteConnection(stream);
                try
                {
                    RachisHello initialMessage;
                    TransactionOperationContext context;
                    using (ContextPool.AllocateOperationContext(out context))
                        initialMessage = remoteConnection.InitFollower(context);

                    if (initialMessage.TopologyId != TopologyId &&
                        string.IsNullOrEmpty(TopologyId) == false)
                    {
                        throw new InvalidOperationException(
                            $"{initialMessage.DebugSourceIdentifier} attempted to connect to us with topology id {initialMessage.TopologyId} but our topology id is already set ({TopologyId}). " +
                            $"Rejecting connection from outside our cluster, this is likely an old server trying to connect to us.");
                    }

                    switch (initialMessage.InitialMessageType)
                    {
                        case InitialMessageType.RequestVote:
                            throw new NotImplementedException("Too young to vote");
                        case InitialMessageType.AppendEntries:
                            if (HandleFirstAppendEntriesNegotiation(remoteConnection) == false)
                                return;// did not accept connection
                            FollowerSteadyState(remoteConnection);
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
                if (_log.IsInfoEnabled)
                {
                    _log.Info("Failed to process incoming connection", e);
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

                try
                {
                    tcpClient?.Dispose();
                }
                catch (Exception)
                {
                }

            }
        }

        private void FollowerSteadyState(RemoteConnection connection)
        {
            var entries = new List<RachisEntry>();
            while (true)
            {
                entries.Clear();

                // TODO: how do we shutdown? probably just close the TCP connection
                TransactionOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                {
                    var appendEntries = connection.Read<AppendEntries>(context);
                    if (appendEntries.EntriesCount != 0)
                    {
                        for (int i = 0; i < appendEntries.EntriesCount; i++)
                        {
                            entries.Add(connection.Read<RachisEntry>(context));
                        }
                    }
                    long lastLogIndex;
                    // we start the tx after we finished reading from the network
                    using (var tx = context.OpenWriteTransaction())
                    {
                        if (entries.Count > 0)
                        {
                            AppendToLog(context, entries);
                        }

                        lastLogIndex = GetLogEntriesRange(context).Item2;

                        var lastEntryIndexToCommit = Math.Min(
                            lastLogIndex,
                            appendEntries.LeaderCommit);

                        var lastAppliedIndex = GetLastAppliedIndex(context);

                        if (lastEntryIndexToCommit != lastAppliedIndex)
                        {
                            _stateMachine.Apply(context, lastEntryIndexToCommit);
                        }

                        tx.Commit();
                    }

                    connection.Send(context, new AppendEntriesResponse
                    {
                        CurrentTerm = CurrentTerm,
                        LastLogIndex = lastLogIndex,
                        Success = true
                    });
                }
            }
        }

        private unsafe void AppendToLog(TransactionOperationContext context, List<RachisEntry> entries)
        {
            Debug.Assert(entries.Count > 0);
            Debug.Assert(context.Transaction != null);
            var table = context.Transaction.InnerTransaction.OpenTable(_logsTable, EntriesSlice);

            long reversedEntryIndex = -1;
            Slice key;
            using (Slice.External(context.Transaction.InnerTransaction.Allocator, (byte*)&reversedEntryIndex, sizeof(long), out key))
            {
                foreach (var entry in entries)
                {
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

                    table.Insert(new TableValueBuilder
                    {
                        reversedEntryIndex,
                        entry.Term,
                        {entry.Entry.BasePointer, entry.Entry.Size}
                    });
                }
            }
        }

        private bool HandleFirstAppendEntriesNegotiation(RemoteConnection connection)
        {
            TransactionOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var fstAppendEntries = connection.Read<AppendEntries>(context);

                if (fstAppendEntries.Term < CurrentTerm)
                {
                    connection.Send(context, new AppendEntriesResponse
                    {
                        Success = false,
                        Message = $"The incoming term {fstAppendEntries.Term} is smaller than current term {CurrentTerm} and is therefor rejected",
                        CurrentTerm = CurrentTerm
                    });
                    connection.Dispose();
                    return false;
                }

                //TODO: If leader, need to step down here
                //TODO: If follower, need to close all followers connections
                //TODO: maybe have a follower lock that only one thread can have?

                // only the leader can send append entries, so if we accepted it, it's the leader
                DebugCurrentLeader = connection.DebugSource;

                if (fstAppendEntries.Term > CurrentTerm)
                {
                    UpdateCurrentTerm(fstAppendEntries.Term);
                }

                var prevTerm = GetTermFor(fstAppendEntries.PrevLogIndex) ?? 0;
                if (prevTerm != fstAppendEntries.PrevLogTerm)
                {
                    // we now have a mismatch with the log position, and need to negotiate it with 
                    // the leader
                    NegotiateMatchEntryWithLeaderAndApplyEntries(context, connection, fstAppendEntries);
                }
                else
                {
                    // this (or the negotiation above) completes the negotiation process
                    connection.Send(context, new AppendEntriesResponse
                    {
                        Success = true,
                        Message = $"Found a log index / term match at {fstAppendEntries.PrevLogIndex} with term {prevTerm}",
                        CurrentTerm = CurrentTerm,
                        LastLogIndex = fstAppendEntries.PrevLogIndex
                    });
                }

                // at this point, the leader will send us a snapshot message
                // in most cases, it is an empty snaphsot, then start regular append entries
                // the reason we send this is to simplify the # of states in the protocol

                var snapshot = connection.Read<InstallSnapshot>(context);

                Debug.Assert(snapshot.SnapshotSize == 0); // for now, until we implement it
                if (snapshot.SnapshotSize != 0)
                {
                    //TODO: read snapshot from stream
                    //TODO: might be large, so need to write to disk (temp folder)
                    //TODO: then need to apply it, might take a while, so need to 
                    //TODO: send periodic heartbeats to other side so it can keep track 
                    //TODO: of what we are doing

                    //TODO: install snapshot always contains the latest topology from the leader
                }
                connection.Send(context, new AppendEntriesResponse
                {
                    Success = true,
                    Message = $"Negotiation completed, now at {snapshot.LastIncludedIndex} with term {snapshot.LastIncludedTerm}",
                    CurrentTerm = CurrentTerm,
                    LastLogIndex = snapshot.LastIncludedIndex
                });
            }
            return true;
        }

        private void NegotiateMatchEntryWithLeaderAndApplyEntries(TransactionOperationContext context, RemoteConnection connection, AppendEntries aer)
        {
            if (aer.EntriesCount != 0)
                // if leader sent entries, we can't negotiate, so it invalid state, shouldn't happen
                throw new InvalidOperationException(
                    "BUG: Need to negotiate with the leader, but it sent entries, so can't negotiate");
            long minIndex;
            long maxIndex;
            long midpointTerm;
            long midpointIndex;
            Debug.Assert(context.Transaction == null, "Transaction is not expected to be opened here");
            using (context.OpenReadTransaction())
            {
                var logEntriesRange = GetLogEntriesRange(context);

                if (logEntriesRange.Item1 == 0) // no entries at all
                {
                    connection.Send(context, new AppendEntriesResponse
                    {
                        Success = true,
                        Message = "No entries at all here, give me everything from the start",
                        CurrentTerm = CurrentTerm,
                        LastLogIndex = 0
                    });

                    return; // leader will know where to start from here
                }

                minIndex = logEntriesRange.Item1;
                maxIndex = Math.Min(
                    logEntriesRange.Item2, // max
                    aer.PrevLogIndex
                );

                midpointIndex = maxIndex + minIndex / 2;

                midpointTerm = GetTermForKnownExisting(context, midpointIndex);
            }


            while (minIndex < maxIndex)
            {
                // TODO: cancellation
                //_cancellationTokenSource.Token.ThrowIfCancellationRequested();

                connection.Send(context, new AppendEntriesResponse
                {
                    Success = true,
                    Message =
                        $"Term/Index mismatch from leader, need to figure out at what point the logs match, range: {maxIndex} - {minIndex} | {midpointIndex} in term {midpointTerm}",
                    CurrentTerm = CurrentTerm,
                    Negotiation = new Negotiation
                    {
                        MaxIndex = maxIndex,
                        MinIndex = minIndex,
                        MidpointIndex = midpointIndex,
                        MidpointTerm = midpointTerm
                    }
                });

                var response = connection.Read<AppendEntries>(context);
                if (GetTermFor(response.PrevLogIndex) == response.PrevLogTerm)
                {
                    minIndex = midpointIndex + 1;
                }
                else
                {
                    maxIndex = midpointIndex - 1;
                }
                midpointIndex = (maxIndex + minIndex) / 2;
                using (context.OpenReadTransaction())
                    midpointTerm = GetTermForKnownExisting(context, midpointIndex);
            }

            connection.Send(context, new AppendEntriesResponse
            {
                Success = true,
                Message = $"Found a log index / term match at {midpointIndex} with term {midpointTerm}",
                CurrentTerm = CurrentTerm,
                LastLogIndex = midpointIndex
            });
        }

        public unsafe BlittableJsonReaderObject GetEntry(TransactionOperationContext context, long index)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(_logsTable, EntriesSlice);
            var reversedIndex = Bits.SwapBytes(index);
            Slice key;
            using (Slice.External(context.Allocator, (byte*)&reversedIndex, sizeof(long), out key))
            {
                TableValueReader reader;
                if (table.ReadByKey(key, out reader) == false)
                    return null;
                int size;
                var ptr = reader.Read(2, out size);
                return new BlittableJsonReaderObject(ptr, size, context);
            }
        }

        public long GetLastAppliedIndex(TransactionOperationContext context)
        {
            Debug.Assert(context.Transaction != null);

            var state = context.Transaction.InnerTransaction.ReadTree(GlobalStateSlice);
            var read = state.Read(LastAppliedSlice);
            if (read == null)
                return 0;
            return read.Reader.ReadLittleEndianInt64();
        }

        public long GetLastCommitIndex(TransactionOperationContext context)
        {
            Debug.Assert(context.Transaction != null);

            var state = context.Transaction.InnerTransaction.ReadTree(GlobalStateSlice);
            var read = state.Read(LastAppliedSlice);
            if (read == null)
                return 0;
            return read.Reader.ReadLittleEndianInt64();
        }


        public unsafe void SetLastAppliedIndex(TransactionOperationContext context, long value)
        {
            Debug.Assert(context.Transaction != null);

            var state = context.Transaction.InnerTransaction.ReadTree(GlobalStateSlice);
            var read = state.Read(LastAppliedSlice);
            if (read != null)
            {
                var oldValue = read.Reader.ReadLittleEndianInt64();
                if (oldValue >= value)
                    throw new InvalidOperationException(
                        $"Cannot reduce the last applied index (is {oldValue} but was requested to reduce to {value})");
            }

            *(long*)state.DirectAdd(LastAppliedSlice, sizeof(long)) = value;
        }

        private unsafe Tuple<long, long> GetLogEntriesRange(TransactionOperationContext context)
        {
            Debug.Assert(context.Transaction != null);

            var table = context.Transaction.InnerTransaction.OpenTable(_logsTable, EntriesSlice);
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

        private long GetTermForKnownExisting(TransactionOperationContext context, long index)
        {
            var termFor = GetTermFor(context, index);
            if (termFor == null)
                throw new InvalidOperationException("Expected the index " + index +
                                                    " to have a term in the entries, but got null");
            return termFor.Value;
        }
        private long? GetTermFor(long index)
        {
            TransactionOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                return GetTermFor(context, index);
            }
        }

        private static unsafe long? GetTermFor(TransactionOperationContext context, long index)
        {
            Debug.Assert(context.Transaction != null);
            var table = context.Transaction.InnerTransaction.OpenTable(_logsTable, EntriesSlice);
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

        private unsafe void UpdateCurrentTerm(long term)
        {
            if (term == CurrentTerm)
                return;

            // TODO
            // if leader, need to become following

            TransactionOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    // we check it here again because now we are under the tx lock, so we can't get into concurrency issues

                    if (term < CurrentTerm)
                        throw new ConcurrencyException($"The current term {CurrentTerm} is larger than {term}, aborting change");

                    var state = tx.InnerTransaction.CreateTree(GlobalStateSlice);
                    *(long*)state.DirectAdd(CurrentTermSlice, sizeof(long)) = term;

                    CurrentTerm = term;

                    tx.Commit();
                }
            }
        }


        public void Dispose()
        {
            _stateMachine?.Dispose();
            ContextPool?.Dispose();
            _persistentState?.Dispose();
            _options?.Dispose();
        }
    }
}