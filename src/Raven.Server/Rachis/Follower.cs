using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Client.Http;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Logging;
using Voron;
using Voron.Data;
using Voron.Data.Tables;

namespace Raven.Server.Rachis
{
    public class Follower : IDisposable
    {
        private readonly RachisConsensus _engine;
        private readonly RemoteConnection _connection;
        private Thread _thread;

        public Follower(RachisConsensus engine, RemoteConnection remoteConnection)
        {
            _engine = engine;
            _connection = remoteConnection;
        }

        private void FollowerSteadyState()
        {
            var entries = new List<RachisEntry>();
            long lastCommit = 0, lastTruncate = 0;
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"Follower {_engine.Tag}: Entering steady state");
            }
            while (true)
            {
                entries.Clear();

                using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    var appendEntries = _connection.Read<AppendEntries>(context);

                    if (appendEntries.Term != _engine.CurrentTerm)
                    {
                        _connection.Send(context, new AppendEntriesResponse
                        {
                            CurrentTerm = _engine.CurrentTerm,
                            Message = "The current term that I have " + _engine.CurrentTerm + " doesn't match " + appendEntries.Term,
                            Success = false,
                        });
                        if (_engine.Log.IsInfoEnabled && entries.Count > 0)
                        {
                            _engine.Log.Info($"Follower {_engine.Tag}: Got invalid term {appendEntries.Term} while the current term is {_engine.CurrentTerm}, aborting connection...");
                        }

                        return;
                    }
                    
                    
                    _engine.Timeout.Defer(_connection.Source);
                    var sp = Stopwatch.StartNew();
                    if (appendEntries.EntriesCount != 0)
                    {
                        for (int i = 0; i < appendEntries.EntriesCount; i++)
                        {
                            entries.Add(_connection.ReadRachisEntry(context));
                            _engine.Timeout.Defer(_connection.Source);
                        }
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info($"Follower {_engine.Tag}: Got non empty append entries request with {entries.Count} entries. Last: ({entries[entries.Count - 1].Index} - {entries[entries.Count - 1].Flags})"
#if DEBUG
                                + $"[{string.Join(" ,", entries.Select(x => x.ToString()))}]"
#endif
                                );
                        }
                    }

                    var lastLogIndex = appendEntries.PrevLogIndex;

                    // don't start write transaction fro noop
                    if (lastCommit != appendEntries.LeaderCommit ||
                        lastTruncate != appendEntries.TruncateLogBefore ||
                        entries.Count != 0)
                    {
                        bool removedFromTopology = false;
                        // we start the tx after we finished reading from the network
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info($"Follower {_engine.Tag}: Ready to start tx in {sp.Elapsed}");
                        }
                        using (var tx = context.OpenWriteTransaction())
                        {
                            if (_engine.Log.IsInfoEnabled)
                            {
                                _engine.Log.Info($"Follower {_engine.Tag}: Tx running in {sp.Elapsed}");
                            }
                            if (entries.Count > 0)
                            {
                                using (var lastTopology = _engine.AppendToLog(context, entries))
                                {
                                    if (lastTopology != null)
                                    {
                                        if (_engine.Log.IsInfoEnabled)
                                        {
                                            _engine.Log.Info($"Topology changed to {lastTopology}");
                                        }

                                        var topology = JsonDeserializationRachis<ClusterTopology>.Deserialize(lastTopology);
                                        if (topology.Members.ContainsKey(_engine.Tag) ||
                                            topology.Promotables.ContainsKey(_engine.Tag) ||
                                            topology.Watchers.ContainsKey(_engine.Tag))
                                        {
                                            RachisConsensus.SetTopology(_engine, context, topology);
                                        }
                                        else
                                        {
                                            removedFromTopology = true;
                                        }
                                    }
                                }
                            }

                            lastLogIndex = _engine.GetLastEntryIndex(context);

                            var lastEntryIndexToCommit = Math.Min(
                                lastLogIndex,
                                appendEntries.LeaderCommit);


                            var lastAppliedIndex = _engine.GetLastCommitIndex(context);

                            if (lastEntryIndexToCommit > lastAppliedIndex)
                            {
                                _engine.Apply(context, lastEntryIndexToCommit, null);
                            }

                            lastTruncate = Math.Min(appendEntries.TruncateLogBefore, lastEntryIndexToCommit);
                            _engine.TruncateLogBefore(context, lastTruncate);
                            lastCommit = lastEntryIndexToCommit;
                            if (_engine.Log.IsInfoEnabled)
                            {
                                _engine.Log.Info($"Follower {_engine.Tag}: Ready to commit in {sp.Elapsed}");
                            }
                            tx.Commit();
                        }

                        if (_engine.Log.IsInfoEnabled && entries.Count > 0)
                        {
                            _engine.Log.Info($"Follower {_engine.Tag}: Processing entries request with {entries.Count} entries took {sp.Elapsed}");
                        }

                        if (removedFromTopology)
                        {
                            if (_engine.Log.IsInfoEnabled)
                            {
                                _engine.Log.Info($"Was notified that I was removed from the node topoloyg, will be moving to passive mode now.");
                            }
                            _engine.SetNewState(RachisConsensus.State.Passive, null, appendEntries.Term,
                                               "I was kicked out of the cluster and moved to passive mode");
                            return;
                        }
                    }

                    if (appendEntries.ForceElections)
                    {
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info($"Follower {_engine.Tag}: Got a request to become candidate from the leader.");
                        }
                        _engine.SwitchToCandidateState("Was asked to do so by my leader", forced: true);
                        return;
                    }

                    _connection.Send(context, new AppendEntriesResponse
                    {
                        CurrentTerm = _engine.CurrentTerm,
                        LastLogIndex = lastLogIndex,
                        Success = true
                    });

                    _engine.Timeout.Defer(_connection.Source);

                    _engine.ReportLeaderTime(appendEntries.TimeAsLeader);

                }
            }
        }

        private LogLengthNegotiation CheckIfValidLeader()
        {
            TransactionOperationContext context;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            {
                var logLength = _connection.Read<LogLengthNegotiation>(context);
               
                if (logLength.Term < _engine.CurrentTerm)
                {
                    _connection.Send(context, new LogLengthNegotiationResponse
                    {
                        Status = LogLengthNegotiationResponse.ResponseStatus.Rejected,
                        Message = $"The incoming term {logLength.Term} is smaller than current term {_engine.CurrentTerm} and is therefor rejected",
                        CurrentTerm = _engine.CurrentTerm
                    });
                    _connection.Dispose();
                    return null;
                }

                _engine.Timeout.Defer(_connection.Source);
                return logLength;
            }
        }

        private void NegotiateWithLeader(TransactionOperationContext context, LogLengthNegotiation negotiation)
        {
            // only the leader can send append entries, so if we accepted it, it's the leader
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"Follower {_engine.Tag}: Got a negotiation request for term {negotiation.Term} where our term is {_engine.CurrentTerm}");
            }
            if (negotiation.Term > _engine.CurrentTerm)
            {
                _engine.FoundAboutHigherTerm(negotiation.Term);
            }

            long prevTerm;
            using (context.OpenReadTransaction())
            {
                prevTerm = _engine.GetTermFor(context, negotiation.PrevLogIndex) ?? 0;
            }
            if (prevTerm != negotiation.PrevLogTerm)
            {
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info($"Follower {_engine.Tag}: Got a negotiation request with PrevLogTerm={negotiation.PrevLogTerm} while our PrevLogTerm={prevTerm}" +
                                     $" will negotiate to find next matched index");
                }
                // we now have a mismatch with the log position, and need to negotiate it with 
                // the leader
                NegotiateMatchEntryWithLeaderAndApplyEntries(context, _connection, negotiation);
            }
            else
            {
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info($"Follower {_engine.Tag}: Got a negotiation request with identical PrevLogTerm will continue to steady state");
                }
                // this (or the negotiation above) completes the negotiation process
                _connection.Send(context, new LogLengthNegotiationResponse
                {
                    Status = LogLengthNegotiationResponse.ResponseStatus.Acceptable,
                    Message = $"Found a log index / term match at {negotiation.PrevLogIndex} with term {prevTerm}",
                    CurrentTerm = _engine.CurrentTerm,
                    LastLogIndex = negotiation.PrevLogIndex
                });
            }

            _engine.Timeout.Defer(_connection.Source);

            // at this point, the leader will send us a snapshot message
            // in most cases, it is an empty snapshot, then start regular append entries
            // the reason we send this is to simplify the # of states in the protocol

            var snapshot = _connection.ReadInstallSnapshot(context);

            using (context.OpenWriteTransaction())
            {
                var lastCommitIndex = _engine.GetLastCommitIndex(context);
                if (snapshot.LastIncludedIndex < lastCommitIndex)
                {
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info(
                            $"Follower {_engine.Tag}: Got installed snapshot with last index={snapshot.LastIncludedIndex} while our lastCommitIndex={lastCommitIndex}, will just ignore it");
                    }
                    //This is okay to ignore because we will just get the commited entries again and skip them
                    ReadInstallSnapshotAndIgnoreContent(context);
                }
                else if (InstallSnapshot(context))
                {
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info(
                            $"Follower {_engine.Tag}: Installed snapshot with last index={snapshot.LastIncludedIndex} with LastIncludedTerm={snapshot.LastIncludedTerm} ");
                    }
                    _engine.SetLastCommitIndex(context, snapshot.LastIncludedIndex, snapshot.LastIncludedTerm);
                    _engine.ClearLogEntriesAndSetLastTruncate(context, snapshot.LastIncludedIndex, snapshot.LastIncludedTerm);
                }
                else
                {
                    var lastEntryIndex = _engine.GetLastEntryIndex(context);
                    if (lastEntryIndex < snapshot.LastIncludedIndex)
                    {
                        var message =
                            $"The snapshot installation had failed because the last included index {snapshot.LastIncludedIndex} in term {snapshot.LastIncludedTerm} doesn't match the last entry {lastEntryIndex}";
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info($"Follower {_engine.Tag}: {message}");
                        }
                        throw new InvalidOperationException(message);
                    }
                }

                // snapshot always has the latest topology
                if (snapshot.Topology == null)
                {
                    var message = "Expected to get topology on snapshot";
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"Follower {_engine.Tag}: {message}");
                    }
                    throw new InvalidOperationException(message);
                }
                using (var topologyJson = context.ReadObject(snapshot.Topology, "topology"))
                {
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"Follower {_engine.Tag}: topology on install snapshot: {topologyJson}");
                    }

                    var topology = JsonDeserializationRachis<ClusterTopology>.Deserialize(topologyJson);

                    RachisConsensus.SetTopology(_engine, context, topology);
                }

                context.Transaction.Commit();
            }
            //Here we send the LastIncludedIndex as our matched index even for the case where our lastCommitIndex is greater
            //So we could validate that the entries sent by the leader are indeed the same as the ones we have.
            _connection.Send(context, new InstallSnapshotResponse
            {
                Done = true,
                CurrentTerm = _engine.CurrentTerm,
                LastLogIndex = snapshot.LastIncludedIndex
            });

            _engine.Timeout.Defer(_connection.Source);
            
            // notify the state machine
            _engine.SnapshotInstalled(context, snapshot.LastIncludedIndex);

            _engine.Timeout.Defer(_connection.Source);
        }

        private unsafe bool InstallSnapshot(TransactionOperationContext context)
        {
            var txw = context.Transaction.InnerTransaction;
            var sp = Stopwatch.StartNew();
            var reader = _connection.CreateReader();
            while (true)
            {
                var type = reader.ReadInt32();
                if (type == -1)
                    return false;

                int size;
                long entries;
                switch ((RootObjectType)type)
                {
                    case RootObjectType.None:
                        return true;
                    case RootObjectType.VariableSizeTree:

                        size = reader.ReadInt32();
                        reader.ReadExactly(size);
                        Slice treeName;// will be freed on context close
                        Slice.From(context.Allocator, reader.Buffer, 0, size, ByteStringType.Immutable, out treeName);
                        txw.DeleteTree(treeName);
                        var tree = txw.CreateTree(treeName);

                        entries = reader.ReadInt64();
                        for (long i = 0; i < entries; i++)
                        {
                            MaybeNotifyLeaderThatWeAreSillAlive(context, sp);

                            size = reader.ReadInt32();
                            reader.ReadExactly(size);
                            Slice valKey;
                            using (
                                Slice.From(context.Allocator, reader.Buffer, 0, size, ByteStringType.Immutable,
                                    out valKey))
                            {
                                size = reader.ReadInt32();
                                reader.ReadExactly(size);

                                byte* ptr;
                                using (tree.DirectAdd(valKey, size, out ptr))
                                {
                                    fixed (byte* pBuffer = reader.Buffer)
                                    {
                                        Memory.Copy(ptr, pBuffer, size);
                                    }
                                }
                            }
                        }


                        break;
                    case RootObjectType.Table:

                        size = reader.ReadInt32();
                        reader.ReadExactly(size);
                        Slice tableName;// will be freed on context close
                        Slice.From(context.Allocator, reader.Buffer, 0, size, ByteStringType.Immutable,
                            out tableName);
                        var tableTree = txw.ReadTree(tableName, RootObjectType.Table);

                        // Get the table schema
                        var schemaSize = tableTree.GetDataSize(TableSchema.SchemasSlice);
                        var schemaPtr = tableTree.DirectRead(TableSchema.SchemasSlice);
                        if (schemaPtr == null)
                            throw new InvalidOperationException(
                                "When trying to install snapshot, found missing table " + tableName);

                        var schema = TableSchema.ReadFrom(txw.Allocator, schemaPtr, schemaSize);

                        var table = txw.OpenTable(schema, tableName);

                        // delete the table
                        TableValueReader tvr;
                        while (true)
                        {
                            if (table.SeekOnePrimaryKey(Slices.AfterAllKeys, out tvr) == false)
                                break;
                            table.Delete(tvr.Id);

                            MaybeNotifyLeaderThatWeAreSillAlive(context, sp);
                        }

                        entries = reader.ReadInt64();
                        for (long i = 0; i < entries; i++)
                        {
                            MaybeNotifyLeaderThatWeAreSillAlive(context, sp);

                            size = reader.ReadInt32();
                            reader.ReadExactly(size);
                            fixed (byte* pBuffer = reader.Buffer)
                            {
                                tvr = new TableValueReader(pBuffer, size);
                                table.Insert(ref tvr);
                            }
                        }
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(type), type.ToString());
                }
            }
        }

        private void ReadInstallSnapshotAndIgnoreContent(TransactionOperationContext context)
        {
            var sp = Stopwatch.StartNew();
            var reader = _connection.CreateReader();
            while (true)
            {
                var type = reader.ReadInt32();
                if (type == -1)
                    return;

                int size;
                long entries;
                switch ((RootObjectType)type)
                {
                    case RootObjectType.None:
                        return;
                    case RootObjectType.VariableSizeTree:

                        size = reader.ReadInt32();
                        reader.ReadExactly(size);

                        entries = reader.ReadInt64();
                        for (long i = 0; i < entries; i++)
                        {
                            MaybeNotifyLeaderThatWeAreSillAlive(context, sp);

                            size = reader.ReadInt32();
                            reader.ReadExactly(size);
                            size = reader.ReadInt32();
                            reader.ReadExactly(size);
                        }
                        break;
                    case RootObjectType.Table:

                        size = reader.ReadInt32();
                        reader.ReadExactly(size);

                        entries = reader.ReadInt64();
                        for (long i = 0; i < entries; i++)
                        {
                            MaybeNotifyLeaderThatWeAreSillAlive(context,  sp);

                            size = reader.ReadInt32();
                            reader.ReadExactly(size);
                        }
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(type), type.ToString());
                }
            }
        }

        private void MaybeNotifyLeaderThatWeAreSillAlive(TransactionOperationContext context, Stopwatch sp)
        {
            if (sp.ElapsedMilliseconds <= _engine.ElectionTimeout.TotalMilliseconds / 4)
                return;

            sp.Restart();
            _engine.Timeout.Defer(_connection.Source);
            _connection.Send(context, new InstallSnapshotResponse
            {
                Done = false,
                CurrentTerm = -1,
                LastLogIndex = -1
            });
        }

        private void NegotiateMatchEntryWithLeaderAndApplyEntries(TransactionOperationContext context,
            RemoteConnection connection, LogLengthNegotiation negotiation)
        {
            long minIndex;
            long maxIndex;
            long midpointTerm;
            long midpointIndex;
            using (context.OpenReadTransaction())
            {
                minIndex = _engine.GetFirstEntryIndex(context);

                if (minIndex == 0) // no entries at all
                {
                    connection.Send(context, new LogLengthNegotiationResponse
                    {
                        Status = LogLengthNegotiationResponse.ResponseStatus.Acceptable,
                        Message = "No entries at all here, give me everything from the start",
                        CurrentTerm = _engine.CurrentTerm,
                        LastLogIndex = 0
                    });

                    return; // leader will know where to start from here
                }

                maxIndex = Math.Min(
                    _engine.GetLastEntryIndex(context), // max
                    negotiation.PrevLogIndex
                );

                midpointIndex = (maxIndex + minIndex) / 2;

                midpointTerm = _engine.GetTermForKnownExisting(context, midpointIndex);
            }


            while (minIndex < maxIndex)
            {
                _engine.Timeout.Defer(_connection.Source);

                // TODO: cancellation
                //_cancellationTokenSource.Token.ThrowIfCancellationRequested();

                connection.Send(context, new LogLengthNegotiationResponse
                {
                    Status = LogLengthNegotiationResponse.ResponseStatus.Negotiation,
                    Message =
                        $"Term/Index mismatch from leader, need to figure out at what point the logs match, range: {maxIndex} - {minIndex} | {midpointIndex} in term {midpointTerm}",
                    CurrentTerm = _engine.CurrentTerm,
                    MaxIndex = maxIndex,
                    MinIndex = minIndex,
                    MidpointIndex = midpointIndex,
                    MidpointTerm = midpointTerm
                });

                var response = connection.Read<LogLengthNegotiation>(context);
                
                _engine.Timeout.Defer(_connection.Source);
                if (response.Truncated)
                {
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"Follower {_engine.Tag}: Got a truncated response from the leader will request all entries");
                    }
                    connection.Send(context, new LogLengthNegotiationResponse
                    {
                        Status = LogLengthNegotiationResponse.ResponseStatus.Acceptable,
                        Message = $"We have entries that are already truncated at the leader, will ask for full snapshot",
                        CurrentTerm = _engine.CurrentTerm,
                        LastLogIndex = 0
                    });
                    return;
                }
                using (context.OpenReadTransaction())
                {
                    if (_engine.GetTermFor(context, response.PrevLogIndex) == response.PrevLogTerm)
                    {
                        minIndex = midpointIndex + 1;
                    }
                    else
                    {
                        maxIndex = midpointIndex - 1;
                    }
                }
                midpointIndex = (maxIndex + minIndex) / 2;
                using (context.OpenReadTransaction())
                    midpointTerm = _engine.GetTermForKnownExisting(context, midpointIndex);
            }
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"Follower {_engine.Tag}: agreed upon last matched index = {midpointIndex} on term = {_engine.CurrentTerm}");
            }
            connection.Send(context, new LogLengthNegotiationResponse()
            {
                Status = LogLengthNegotiationResponse.ResponseStatus.Acceptable,
                Message = $"Found a log index / term match at {midpointIndex} with term {midpointTerm}",
                CurrentTerm = _engine.CurrentTerm,
                LastLogIndex = midpointIndex
            });
        }

        public void TryAcceptConnection()
        {
            var negotiation = CheckIfValidLeader();
            if (negotiation == null)
            {
                _connection.Dispose();
                return; // did not accept connection
            }
            // if leader / candidate, this remove them from play and revert to follower mode
            var engineCurrentTerm = _engine.CurrentTerm;
            _engine.SetNewState(RachisConsensus.State.Follower, this, engineCurrentTerm,
                $"Accepted a new connection from {_connection.Source} in term {negotiation.Term}");
            _engine.LeaderTag = _connection.Source;
            _engine.Timeout.Start(_engine.SwitchToCandidateStateOnTimeout);

            _thread = new Thread(Run)
            {
                Name = $"Follower thread from {_connection} in term {negotiation.Term}",
                IsBackground = true
            };
            _thread.Start(negotiation);
        }

        private void Run(object obj)
        {
            try
            {
                using (this)
                {
                    try
                    {
                        TransactionOperationContext context;
                        using (_engine.ContextPool.AllocateOperationContext(out context))
                        {
                            NegotiateWithLeader(context, (LogLengthNegotiation)obj);
                        }
                        FollowerSteadyState();
                    }
                    catch (OperationCanceledException)
                    {
                    }
                    catch (ObjectDisposedException)
                    {
                    }
                    catch (AggregateException ae)
                        when (
                            ae.InnerException is OperationCanceledException ||
                            ae.InnerException is ObjectDisposedException)
                    {
                    }
                    catch (Exception e)
                    {
                        TransactionOperationContext context;
                        using (_engine.ContextPool.AllocateOperationContext(out context))
                        {
                            _connection.Send(context, e);
                        }
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info($"Follower {_engine.Tag}: Failed to talk to leader", e);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info("Failed to dispose follower when talking leader: " + _engine.Tag, e);
                }
            }
        }

        public void Dispose()
        {
            _connection.Dispose();
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"Follower {_engine.Tag}:Dispose");
            }
            if (_thread != null &&
                _thread.ManagedThreadId != Thread.CurrentThread.ManagedThreadId)
                _thread.Join();
        }
    }
}