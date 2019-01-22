using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Utils;
using Voron;
using Voron.Data;
using Voron.Data.Tables;

namespace Raven.Server.Rachis
{
    public class Follower : IDisposable
    {
        private static int _uniqueId;

        private readonly RachisConsensus _engine;
        private readonly long _term;
        private readonly RemoteConnection _connection;
        private PoolOfThreads.LongRunningWork _followerLongRunningWork;

        private readonly string _debugName;
        private readonly RachisLogRecorder _debugRecorder;

        public Follower(RachisConsensus engine, long term, RemoteConnection remoteConnection)
        {
            _engine = engine;
            _connection = remoteConnection;
            _term = term;

            // this will give us a unique identifier for this follower
            var uniqueId = Interlocked.Increment(ref _uniqueId);
            _debugName = $"Follower in term {_term} (id: {uniqueId})";
            _debugRecorder = _engine.InMemoryDebug.GetNewRecorder(_debugName);
            _debugRecorder.Start();
        }

        public override string ToString()
        {
            return $"Follower {_engine.Tag} of leader {_connection.Source} in term {_term}";
        }

        private void FollowerSteadyState()
        {
            var entries = new List<RachisEntry>();
            long lastCommit = 0, lastTruncate = 0;
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"{ToString()}: Entering steady state");
            }
            
            while (true)
            {
               
                entries.Clear();

                using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    _debugRecorder.Record("Wait for entries");
                    var appendEntries = _connection.Read<AppendEntries>(context);
                    var sp = Stopwatch.StartNew();
                   
                    if (appendEntries.Term != _engine.CurrentTerm)
                    {
                        _connection.Send(context, new AppendEntriesResponse
                        {
                            CurrentTerm = _engine.CurrentTerm,
                            Message = "The current term that I have " + _engine.CurrentTerm + " doesn't match " + appendEntries.Term,
                            Success = false
                        });
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info($"{ToString()}: Got invalid term {appendEntries.Term} while the current term is {_engine.CurrentTerm}, aborting connection...");
                        }

                        return;
                    }

                    ClusterCommandsVersionManager.SetClusterVersion(appendEntries.MinCommandVersion);

                    _debugRecorder.Record("Got entries");
                    _engine.Timeout.Defer(_connection.Source);
                    if (appendEntries.EntriesCount != 0)
                    {
                        for (int i = 0; i < appendEntries.EntriesCount; i++)
                        {
                            entries.Add(_connection.ReadRachisEntry(context));
                            _engine.Timeout.Defer(_connection.Source);
                        }
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info($"{ToString()}: Got non empty append entries request with {entries.Count} entries. Last: ({entries[entries.Count - 1].Index} - {entries[entries.Count - 1].Flags})"
#if DEBUG
                                + $"[{string.Join(" ,", entries.Select(x => x.ToString()))}]"
#endif
                                );
                        }
                    }

                    var lastLogIndex = appendEntries.PrevLogIndex;

                    // don't start write transaction for noop
                    if (lastCommit != appendEntries.LeaderCommit ||
                        lastTruncate != appendEntries.TruncateLogBefore ||
                        entries.Count != 0)
                    {
                        using (var cts = new CancellationTokenSource())
                        {
                            // applying the leader state may take a while, we need to ping
                            // the server and let us know that we are still there
                            var task = Concurrent_SendAppendEntriesPendingToLeaderAsync(cts, _term, lastLogIndex);
                            try
                            {
                                bool hasRemovedFromTopology;

                                (hasRemovedFromTopology, lastLogIndex, lastTruncate, lastCommit) = ApplyLeaderStateToLocalState(sp,
                                    context,
                                    entries,
                                    appendEntries);

                                if (hasRemovedFromTopology)
                                {
                                    if (_engine.Log.IsInfoEnabled)
                                    {
                                        _engine.Log.Info("Was notified that I was removed from the node topology, will be moving to passive mode now.");
                                    }

                                    _engine.SetNewState(RachisState.Passive, null, appendEntries.Term,
                                        "I was kicked out of the cluster and moved to passive mode");
                                    return;
                                }
                            }
                            catch (RachisInvalidOperationException)
                            {
                                // on raft protocol violation propagate the error and close this follower. 
                                throw;
                            }
                            catch (ConcurrencyException)
                            {
                                // the term was changed
                                throw;
                            }
                            catch (Exception e)
                            {
                                if (_engine.Log.IsInfoEnabled)
                                {
                                    _engine.Log.Info($"Failed to apply leader state to local state with {entries.Count:#,#;;0} entries with leader commit: {appendEntries.LeaderCommit}, term: {appendEntries.Term}. Prev log index: {appendEntries.PrevLogIndex}", e);
                                }
                            }
                            finally
                            {
                                // here we need to wait until the concurrent send pending to leader
                                // is completed to avoid concurrent writes to the leader
                                cts.Cancel();
                                task.Wait(CancellationToken.None);
                            }
                        }
                    }

                    if (appendEntries.ForceElections)
                    {
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info($"{ToString()}: Got a request to become candidate from the leader.");
                        }
                        _engine.SwitchToCandidateState("Was asked to do so by my leader", forced: true);
                        return;
                    }
                    _debugRecorder.Record("Processing entries is completed");
                    _connection.Send(context, new AppendEntriesResponse
                    {
                        CurrentTerm = _term,
                        LastLogIndex = lastLogIndex,
                        Success = true
                    });

                    if (sp.Elapsed > _engine.ElectionTimeout / 2)
                    {
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info($"{ToString()}: Took a long time to complete the cycle with {entries.Count} entries: {sp.Elapsed}");
                        }
                    }
                    
                    _engine.Timeout.Defer(_connection.Source);
                    _engine.ReportLeaderTime(appendEntries.TimeAsLeader);

                    _debugRecorder.Record("Cycle done");
                    _debugRecorder.Start();
                }
            }
        }

        private async Task Concurrent_SendAppendEntriesPendingToLeaderAsync(CancellationTokenSource cts, long currentTerm, long lastLogIndex)
        {
            var timeoutPeriod = _engine.Timeout.TimeoutPeriod / 4;
            var timeToWait = TimeSpan.FromMilliseconds(timeoutPeriod);
            using (_engine.ContextPool.AllocateOperationContext(out JsonOperationContext timeoutCtx))
            {
                while (cts.IsCancellationRequested == false)
                {
                    try
                    {
                        await TimeoutManager.WaitFor(timeToWait, cts.Token);
                        if (cts.IsCancellationRequested)
                            break;
                        _engine.Timeout.Defer(_connection.Source);
                        _connection.Send(timeoutCtx, new AppendEntriesResponse
                        {
                            Pending = true,
                            Success = false,
                            CurrentTerm = currentTerm,
                            LastLogIndex = lastLogIndex
                        });
                    }
                    catch (TimeoutException)
                    {
                        break;
                    }
                    catch (Exception e) when (RachisConsensus.IsExpectedException(e))
                    {
                        break;
                    }
                }
            }
        }

        private (bool HasRemovedFromTopology, long LastLogIndex, long LastTruncate,  long LastCommit)  ApplyLeaderStateToLocalState(Stopwatch sp, TransactionOperationContext context, List<RachisEntry> entries, AppendEntries appendEntries)
        {
            long lastLogIndex; 
            long lastTruncate;
            long lastCommit;
            
            bool removedFromTopology = false;
            // we start the tx after we finished reading from the network
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"{ToString()}: Ready to start tx in {sp.Elapsed}");
            }

            using (var tx = context.OpenWriteTransaction())
            {
                _engine.ValidateTerm(_term);

                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info($"{ToString()}: Tx running in {sp.Elapsed}");
                }

                if (entries.Count > 0)
                {
                    var (lastTopology, lastTopologyIndex) = _engine.AppendToLog(context, entries);
                    using (lastTopology)
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
                                _engine.ClearAppendedEntriesAfter(context, lastTopologyIndex);
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
                    lastAppliedIndex = _engine.Apply(context, lastEntryIndexToCommit, null, sp);
                }

                lastTruncate = Math.Min(appendEntries.TruncateLogBefore, lastAppliedIndex);
                _engine.TruncateLogBefore(context, lastTruncate);

                lastCommit = lastEntryIndexToCommit;
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info($"{ToString()}: Ready to commit in {sp.Elapsed}");
                }

                tx.Commit();
            }

            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"{ToString()}: Processing entries request with {entries.Count} entries took {sp.Elapsed}");
            }

            return (HasRemovedFromTopology: removedFromTopology,  LastLogIndex: lastLogIndex,  LastTruncate: lastTruncate,  LastCommit: lastCommit);
        }

        public static bool CheckIfValidLeader(RachisConsensus engine, RemoteConnection connection, out LogLengthNegotiation negotiation)
        {
            negotiation = null;
            using (engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var logLength = connection.Read<LogLengthNegotiation>(context);

                if (logLength.Term < engine.CurrentTerm)
                {
                    var msg = $"The incoming term {logLength.Term} is smaller than current term {engine.CurrentTerm} and is therefor rejected (From thread: {logLength.SendingThread})";
                    if (engine.Log.IsInfoEnabled)
                    {
                        engine.Log.Info(msg);
                    }
                    connection.Send(context, new LogLengthNegotiationResponse
                    {
                        Status = LogLengthNegotiationResponse.ResponseStatus.Rejected,
                        Message = msg,
                        CurrentTerm = engine.CurrentTerm
                    });
                    connection.Dispose();
                    return false;
                }
                if (engine.Log.IsInfoEnabled)
                {
                    engine.Log.Info($"The incoming term { logLength.Term} is from a valid leader (From thread: {logLength.SendingThread})");
                }
                engine.FoundAboutHigherTerm(logLength.Term, "Setting the term of the new leader");
                engine.Timeout.Defer(connection.Source);
                negotiation = logLength;
            }
            return true;
        }

        private void NegotiateWithLeader(TransactionOperationContext context, LogLengthNegotiation negotiation)
        {
            _debugRecorder.Start();
            // only the leader can send append entries, so if we accepted it, it's the leader
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"{ToString()}: Got a negotiation request for term {negotiation.Term} where our term is {_term}");
            }
            if (negotiation.Term != _term)
            {
                //  Our leader is no longer a valid one
                var msg = $"The term was changed after leader validation from {_term} to {negotiation.Term}, so you are no longer a valid leader";
                _connection.Send(context, new LogLengthNegotiationResponse
                {
                    Status = LogLengthNegotiationResponse.ResponseStatus.Rejected,
                    Message = msg,
                    CurrentTerm = _term,
                    LastLogIndex = negotiation.PrevLogIndex
                });
                throw new InvalidOperationException($"We close this follower because: {msg}");
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
                    _engine.Log.Info($"{ToString()}: Got a negotiation request with PrevLogTerm={negotiation.PrevLogTerm} while our PrevLogTerm={prevTerm}" +
                                     " will negotiate to find next matched index");
                }
                // we now have a mismatch with the log position, and need to negotiate it with 
                // the leader
                NegotiateMatchEntryWithLeaderAndApplyEntries(context, _connection, negotiation);
            }
            else
            {
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info($"{ToString()}: Got a negotiation request with identical PrevLogTerm will continue to steady state");
                }
                // this (or the negotiation above) completes the negotiation process
                _connection.Send(context, new LogLengthNegotiationResponse
                {
                    Status = LogLengthNegotiationResponse.ResponseStatus.Acceptable,
                    Message = $"Found a log index / term match at {negotiation.PrevLogIndex} with term {prevTerm}",
                    CurrentTerm = _term,
                    LastLogIndex = negotiation.PrevLogIndex
                });
            }
            _debugRecorder.Record("Matching Negotiation is over, waiting for snapshot");
            _engine.Timeout.Defer(_connection.Source);

            // at this point, the leader will send us a snapshot message
            // in most cases, it is an empty snapshot, then start regular append entries
            // the reason we send this is to simplify the # of states in the protocol

            var snapshot = _connection.ReadInstallSnapshot(context);
            _debugRecorder.Record("Snapshot received");
            using (context.OpenWriteTransaction())
            {
                var lastTerm = _engine.GetTermFor(context, snapshot.LastIncludedIndex);
                var lastCommitIndex = _engine.GetLastEntryIndex(context);
                if (snapshot.LastIncludedTerm == lastTerm && snapshot.LastIncludedIndex < lastCommitIndex)
                {
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info(
                            $"{ToString()}: Got installed snapshot with last index={snapshot.LastIncludedIndex} while our lastCommitIndex={lastCommitIndex}, will just ignore it");
                    }

                    //This is okay to ignore because we will just get the committed entries again and skip them
                    ReadInstallSnapshotAndIgnoreContent(context);
                }
                else if (InstallSnapshot(context))
                {
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info(
                            $"{ToString()}: Installed snapshot with last index={snapshot.LastIncludedIndex} with LastIncludedTerm={snapshot.LastIncludedTerm} ");
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
                            _engine.Log.Info($"{ToString()}: {message}");
                        }
                        throw new InvalidOperationException(message);
                    }
                }

                // snapshot always has the latest topology
                if (snapshot.Topology == null)
                {
                    const string message = "Expected to get topology on snapshot";
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"{ToString()}: {message}");
                    }
                    throw new InvalidOperationException(message);
                }
                using (var topologyJson = context.ReadObject(snapshot.Topology, "topology"))
                {
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"{ToString()}: topology on install snapshot: {topologyJson}");
                    }

                    var topology = JsonDeserializationRachis<ClusterTopology>.Deserialize(topologyJson);

                    RachisConsensus.SetTopology(_engine, context, topology);
                }

                context.Transaction.Commit();
            }

            _engine.Timeout.Defer(_connection.Source);
            _debugRecorder.Record("Invoking StateMachine.SnapshotInstalled");

            // notify the state machine, we do this in an async manner, and start
            // the operator in a separate thread to avoid timeouts while this is
            // going on

            var task = Task.Run(() => _engine.SnapshotInstalledAsync(snapshot.LastIncludedIndex));

            var sp = Stopwatch.StartNew();

            var timeToWait = (int)(_engine.ElectionTimeout.TotalMilliseconds / 4);

            while (task.Wait(timeToWait) == false)
            {
                // this may take a while, so we let the other side know that
                // we are still processing, and we reset our own timer while
                // this is happening
                MaybeNotifyLeaderThatWeAreStillAlive(context, sp);
            }

            _debugRecorder.Record("Done with StateMachine.SnapshotInstalled");

            // we might have moved from passive node, so we need to start the timeout clock
            _engine.Timeout.Start(_engine.SwitchToCandidateStateOnTimeout);

            _debugRecorder.Record("Snapshot installed");
            //Here we send the LastIncludedIndex as our matched index even for the case where our lastCommitIndex is greater
            //So we could validate that the entries sent by the leader are indeed the same as the ones we have.
            _connection.Send(context, new InstallSnapshotResponse
            {
                Done = true,
                CurrentTerm = _term,
                LastLogIndex = snapshot.LastIncludedIndex
            });

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
                            MaybeNotifyLeaderThatWeAreStillAlive(context, sp);

                            size = reader.ReadInt32();
                            reader.ReadExactly(size);
                            using (Slice.From(context.Allocator, reader.Buffer, 0, size, ByteStringType.Immutable, out Slice valKey))
                            {
                                size = reader.ReadInt32();
                                reader.ReadExactly(size);

                                using (tree.DirectAdd(valKey, size, out byte* ptr))
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

                            MaybeNotifyLeaderThatWeAreStillAlive(context, sp);
                        }

                        entries = reader.ReadInt64();
                        for (long i = 0; i < entries; i++)
                        {
                            MaybeNotifyLeaderThatWeAreStillAlive(context, sp);

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
                            MaybeNotifyLeaderThatWeAreStillAlive(context, sp);

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
                            MaybeNotifyLeaderThatWeAreStillAlive(context, sp);

                            size = reader.ReadInt32();
                            reader.ReadExactly(size);
                        }
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(type), type.ToString());
                }
            }
        }

        private void MaybeNotifyLeaderThatWeAreStillAlive(TransactionOperationContext context, Stopwatch sp)
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
                        CurrentTerm = _term,
                        LastLogIndex = 0,
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

            while ((midpointTerm == negotiation.PrevLogTerm && midpointIndex == negotiation.PrevLogIndex) == false)
            {
                _engine.Timeout.Defer(_connection.Source);

                _engine.ValidateTerm(_term);

                if (midpointIndex == negotiation.PrevLogIndex && midpointTerm != negotiation.PrevLogTerm)
                {
                    // our appended entries has been diverged, same index with different terms.
                    var msg = $"Our appended entries has been diverged, same index with different terms. " +
                              $"My index/term {midpointIndex}/{midpointTerm}, while yours is {negotiation.PrevLogIndex}/{negotiation.PrevLogTerm}.";
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"{ToString()}: {msg}");
                    }
                    connection.Send(context, new LogLengthNegotiationResponse
                    {
                        Status = LogLengthNegotiationResponse.ResponseStatus.Acceptable,
                        Message = msg,
                        CurrentTerm = _term,
                        LastLogIndex = 0
                    });
                    return;
                }

                connection.Send(context, new LogLengthNegotiationResponse
                {
                    Status = LogLengthNegotiationResponse.ResponseStatus.Negotiation,
                    Message =
                        $"Term/Index mismatch from leader, need to figure out at what point the logs match, range: {maxIndex} - {minIndex} | {midpointIndex} in term {midpointTerm}",
                    CurrentTerm = _term,
                    MaxIndex = maxIndex,
                    MinIndex = minIndex,
                    MidpointIndex = midpointIndex,
                    MidpointTerm = midpointTerm
                });

                negotiation = connection.Read<LogLengthNegotiation>(context);

                _engine.Timeout.Defer(_connection.Source);
                if (negotiation.Truncated)
                {
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"{ToString()}: Got a truncated response from the leader will request all entries");
                    }
                    connection.Send(context, new LogLengthNegotiationResponse
                    {
                        Status = LogLengthNegotiationResponse.ResponseStatus.Acceptable,
                        Message = "We have entries that are already truncated at the leader, will ask for full snapshot",
                        CurrentTerm = _term,
                        LastLogIndex = 0
                    });
                    return;
                }
                using (context.OpenReadTransaction())
                {
                    if (_engine.GetTermFor(context, negotiation.PrevLogIndex) == negotiation.PrevLogTerm)
                    {
                        minIndex = Math.Min(midpointIndex + 1, maxIndex);
                    }
                    else
                    {
                        maxIndex = Math.Max(midpointIndex - 1, minIndex);
                    }
                }
                midpointIndex = (maxIndex + minIndex) / 2;
                using (context.OpenReadTransaction())
                    midpointTerm = _engine.GetTermForKnownExisting(context, midpointIndex);
            }

            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"{ToString()}: agreed upon last matched index = {midpointIndex} on term = {midpointTerm}");
            }

            connection.Send(context, new LogLengthNegotiationResponse
            {
                Status = LogLengthNegotiationResponse.ResponseStatus.Acceptable,
                Message = $"Found a log index / term match at {midpointIndex} with term {midpointTerm}",
                CurrentTerm = _term,
                LastLogIndex = midpointIndex,
            });
        }
        
        public void AcceptConnection(LogLengthNegotiation negotiation)
        {
            if(_engine.CurrentState != RachisState.Passive)
                _engine.Timeout.Start(_engine.SwitchToCandidateStateOnTimeout);
            
            // if leader / candidate, this remove them from play and revert to follower mode
            _engine.SetNewState(RachisState.Follower, this, _term,
                $"Accepted a new connection from {_connection.Source} in term {negotiation.Term}");
            _engine.LeaderTag = _connection.Source;
            
            _debugRecorder.Record("Follower connection accepted");

            _followerLongRunningWork = 
                PoolOfThreads.GlobalRavenThreadPool.LongRunning(
                    action: x => Run(x),
                    state: negotiation,
                    name: $"Follower thread from {_connection} in term {negotiation.Term}");
                
        }

        private void Run(object obj)
        {
            try
            {
                try
                {
                    Thread.CurrentThread.Priority = ThreadPriority.AboveNormal;
                }
                catch (Exception e)
                {
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"{_debugName} was unable to set the thread priority, will continue with the same priority", e);
                    }
                }

                using (this)
                {
                    try
                    {
                        using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                        {
                            NegotiateWithLeader(context, (LogLengthNegotiation)obj);
                        }

                        FollowerSteadyState();
                    }
                    catch (Exception e) when (RachisConsensus.IsExpectedException(e))
                    {
                    }
                    catch (Exception e)
                    {
                        _debugRecorder.Record($"Sending error: {e}");
                        using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                        {
                            _connection.Send(context, e);
                        }

                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info($"{ToString()}: Failed to talk to leader", e);
                        }
                    }
                }
            }
            catch (Exception e) when (RachisConsensus.IsExpectedException(e))
            {
            }
            catch (Exception e)
            {
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info("Failed to dispose follower when talking to the leader: " + _engine.Tag, e);
                }
            }
        }

        public void Dispose()
        {
            _connection.Dispose();
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"{ToString()}: Disposing");
            }
            
            if (_followerLongRunningWork != null && _followerLongRunningWork.ManagedThreadId != Thread.CurrentThread.ManagedThreadId)
                _followerLongRunningWork.Join(int.MaxValue);
            
            _engine.InMemoryDebug.RemoveRecorder(_debugName);
        }
    }
}
