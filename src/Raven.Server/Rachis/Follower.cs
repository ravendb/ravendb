using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Rachis.Remote;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Utils;
using Voron;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.Tables;
using Voron.Impl;

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
            long lastCommit = 0, lastTruncate = 0, lastAcknowledgedIndex = 0;
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"{ToString()}: Entering steady state");
            }

            while (true)
            {
                entries.Clear();

                using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    using (context.OpenReadTransaction())
                    {
                        if (_engine.RequestSnapshot)
                        {
                            if (_engine.Log.IsInfoEnabled)
                            {
                                _engine.Log.Info($"{ToString()}: Snapshot was requested, so we close this follower.");
                            }
                            return;
                        }
                    }

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
                            using (var cts = new CancellationTokenSource())
                            {
                                var task = Concurrent_SendAppendEntriesPendingToLeaderAsync(cts, _term, appendEntries.PrevLogIndex);
                                entries.Add(_connection.ReadRachisEntry(context));
                                cts.Cancel();
                                task.Wait(CancellationToken.None);
                            }

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

                    // don't start write transaction for noop
                    if (lastCommit != appendEntries.LeaderCommit ||
                        lastTruncate != appendEntries.TruncateLogBefore ||
                        entries.Count != 0)
                    {
                        using (var cts = new CancellationTokenSource())
                        {
                            // applying the leader state may take a while, we need to ping
                            // the server and let us know that we are still there
                            var task = Concurrent_SendAppendEntriesPendingToLeaderAsync(cts, _term, appendEntries.PrevLogIndex);
                            try
                            {
                                bool hasRemovedFromTopology;

                                (hasRemovedFromTopology, lastAcknowledgedIndex, lastTruncate, lastCommit) = ApplyLeaderStateToLocalState(sp,
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
                        LastLogIndex = lastAcknowledgedIndex,
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
                        timeoutCtx.Reset();
                        timeoutCtx.Renew();

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

        private (bool HasRemovedFromTopology, long LastAcknowledgedIndex, long LastTruncate,  long LastCommit)  ApplyLeaderStateToLocalState(Stopwatch sp, TransactionOperationContext context, List<RachisEntry> entries, AppendEntries appendEntries)
        {
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

                var lastEntryIndexToCommit = Math.Min(
                    _engine.GetLastEntryIndex(context),
                    appendEntries.LeaderCommit);

                var lastAppliedIndex = _engine.GetLastCommitIndex(context);
                var lastAppliedTerm = _engine.GetTermFor(context, lastEntryIndexToCommit);

                // we start to commit only after we have any log with a term of the current leader
                if (lastEntryIndexToCommit > lastAppliedIndex && lastAppliedTerm == appendEntries.Term)
                {
                    lastAppliedIndex = _engine.Apply(context, lastEntryIndexToCommit, null, sp);
                }

                lastTruncate = Math.Min(appendEntries.TruncateLogBefore, lastAppliedIndex);
                _engine.TruncateLogBefore(context, lastTruncate);

                lastCommit = lastAppliedIndex;
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

            var lastAcknowledgedIndex = entries.Count == 0 ? appendEntries.PrevLogIndex : entries[entries.Count - 1].Index;

            return (HasRemovedFromTopology: removedFromTopology, LastAcknowledgedIndex: lastAcknowledgedIndex,  LastTruncate: lastTruncate,  LastCommit: lastCommit);
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
                _engine.Log.Info($"{ToString()}: Got a negotiation request for term {negotiation.Term} where our term is {_term}.");
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
            bool requestSnapshot;
            bool requestFullSnapshot = false;
            using (context.OpenReadTransaction())
            {
                prevTerm = _engine.GetTermFor(context, negotiation.PrevLogIndex) ?? 0;
                requestSnapshot = _engine.GetSnapshotRequest(context);
            }

            if (requestSnapshot)
            {
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info($"{ToString()}: Request snapshot by the admin");
                }
                _connection.Send(context, new LogLengthNegotiationResponse
                {
                    Status = LogLengthNegotiationResponse.ResponseStatus.Acceptable,
                    Message = "Request snapshot by the admin",
                    CurrentTerm = _term,
                    LastLogIndex = 0
                });
                requestFullSnapshot = true;
            }
            else if (prevTerm != negotiation.PrevLogTerm)
            {
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info($"{ToString()}: Got a negotiation request with PrevLogTerm={negotiation.PrevLogTerm} while our PrevLogTerm={prevTerm}" +
                                     " will negotiate to find next matched index");
                }
                // we now have a mismatch with the log position, and need to negotiate it with 
                // the leader
                requestFullSnapshot = NegotiateMatchEntryWithLeaderAndApplyEntries(context, _connection, negotiation);
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
            _debugRecorder.Record("Start receiving snapshot");

            // reading the snapshot from network and committing it to the disk might take a long time. 
            using (var cts = new CancellationTokenSource())
            {
                KeepAliveAndExecuteAction(() => ReadAndCommitSnapshot(context, snapshot, cts.Token), cts, "ReadAndCommitSnapshot");
            }

            _debugRecorder.Record("Snapshot was received and committed");

            // notify the state machine, we do this in an async manner, and start
            // the operator in a separate thread to avoid timeouts while this is
            // going on
            using (var cts = new CancellationTokenSource())
            {
                KeepAliveAndExecuteAction(() =>
                {
                    _engine.SnapshotInstalled(snapshot.LastIncludedIndex, requestFullSnapshot, cts.Token);
                }, cts, "SnapshotInstalledAsync");
            }

            _debugRecorder.Record("Done with StateMachine.SnapshotInstalled");
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

        private void KeepAliveAndExecuteAction(Action action, CancellationTokenSource cts, string debug)
        {
            var task = Task.Run(action, cts.Token);
            try
            {
                WaitForTaskCompletion(task, debug);
            }
            catch
            {
                cts.Cancel();
                task.Wait(TimeSpan.FromSeconds(60));
                throw;
            }
        }

        private void WaitForTaskCompletion(Task task, string debug)
        {
            var sp = Stopwatch.StartNew();

            var timeToWait = (int)(_engine.ElectionTimeout.TotalMilliseconds / 4);


            while (task.Wait(timeToWait) == false)
            {
                using (_engine.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    // this may take a while, so we let the other side know that
                    // we are still processing, and we reset our own timer while
                    // this is happening
                    MaybeNotifyLeaderThatWeAreStillAlive(context, sp);
                }
            }

            if (task.IsFaulted)
            {
                throw new InvalidOperationException($"{ToString()} encounter an error during {debug}", task.Exception);
            }
        }

        private void ReadAndCommitSnapshot(TransactionOperationContext context, InstallSnapshot snapshot, CancellationToken token)
        {
            using (context.OpenWriteTransaction())
            {
                var lastTerm = _engine.GetTermFor(context, snapshot.LastIncludedIndex);
                var lastCommitIndex = _engine.GetLastEntryIndex(context);

                if (_engine.GetSnapshotRequest(context) == false && 
                    snapshot.LastIncludedTerm == lastTerm && snapshot.LastIncludedIndex < lastCommitIndex)
                {
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info(
                            $"{ToString()}: Got installed snapshot with last index={snapshot.LastIncludedIndex} while our lastCommitIndex={lastCommitIndex}, will just ignore it");
                    }

                    //This is okay to ignore because we will just get the committed entries again and skip them
                    ReadInstallSnapshotAndIgnoreContent(token);
                }
                else if (InstallSnapshot(context, token))
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

                _engine.SetSnapshotRequest(context, false);

                context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += t =>
                {
                    if (t is LowLevelTransaction llt && llt.Committed)
                    {
                        // we might have moved from passive node, so we need to start the timeout clock
                        _engine.Timeout.Start(_engine.SwitchToCandidateStateOnTimeout);
                    }
                };

                context.Transaction.Commit();
            }
        }

        private bool InstallSnapshot(TransactionOperationContext context, CancellationToken token)
        {
            var txw = context.Transaction.InnerTransaction;

            var fileName = $"snapshot.{Guid.NewGuid():N}";
            var filePath = context.Environment.Options.DataPager.Options.TempPath.Combine(fileName);
            
            using (var temp = new StreamsTempFile(filePath.FullPath, context.Environment))
            using(var stream = temp.StartNewStream())
            using(var remoteReader = _connection.CreateReaderToStream(stream))
            {
                if (ReadSnapshot(remoteReader, context, txw, dryRun: true, token) == false)
                    return false;

                stream.Seek(0, SeekOrigin.Begin);
                using (var fileReader = new StreamSnapshotReader(stream))
                {
                    ReadSnapshot(fileReader, context, txw, dryRun: false, token);
                }
            }
            
            return true;
        }
        
        private unsafe bool ReadSnapshot(SnapshotReader reader, TransactionOperationContext context, Transaction txw, bool dryRun, CancellationToken token)
        {
            var type = reader.ReadInt32();
            if (type == -1)
                return false;
            
            while (true)
            {
                token.ThrowIfCancellationRequested();

                int size;
                long entries;
                switch ((RootObjectType)type)
                {
                    case RootObjectType.None:
                        return true;
                    case RootObjectType.VariableSizeTree:
                        size = reader.ReadInt32();
                        reader.ReadExactly(size);

                        Tree tree = null;
                        if (dryRun == false)
                        {
                            Slice.From(context.Allocator, reader.Buffer, 0, size, ByteStringType.Immutable, out Slice treeName); // The Slice will be freed on context close
                            txw.DeleteTree(treeName);
                            tree = txw.CreateTree(treeName);
                        }

                        entries = reader.ReadInt64();
                        for (long i = 0; i < entries; i++)
                        {
                            token.ThrowIfCancellationRequested();
                            size = reader.ReadInt32();
                            reader.ReadExactly(size);
                            using (Slice.From(context.Allocator, reader.Buffer, 0, size, ByteStringType.Immutable, out Slice valKey))
                            {
                                size = reader.ReadInt32();
                                reader.ReadExactly(size);

                                if (dryRun == false)
                                {
                                    using (tree.DirectAdd(valKey, size, out byte* ptr))
                                    {
                                        fixed (byte* pBuffer = reader.Buffer)
                                        {
                                            Memory.Copy(ptr, pBuffer, size);
                                        }
                                    }
                                }
                            }
                        }
                        break;
                    case RootObjectType.Table:

                        size = reader.ReadInt32();
                        reader.ReadExactly(size);

                        TableValueReader tvr;
                        Table table = null;
                        if (dryRun == false)
                        {
                            Slice.From(context.Allocator, reader.Buffer, 0, size, ByteStringType.Immutable,
                                out Slice tableName);//The Slice will be freed on context close
                            var tableTree = txw.ReadTree(tableName, RootObjectType.Table);

                            // Get the table schema
                            var schemaSize = tableTree.GetDataSize(TableSchema.SchemasSlice);
                            var schemaPtr = tableTree.DirectRead(TableSchema.SchemasSlice);
                            if (schemaPtr == null)
                                throw new InvalidOperationException(
                                    "When trying to install snapshot, found missing table " + tableName);

                            var schema = TableSchema.ReadFrom(txw.Allocator, schemaPtr, schemaSize);

                            table = txw.OpenTable(schema, tableName);

                            // delete the table
                            while (true)
                            {
                                token.ThrowIfCancellationRequested();
                                if (table.SeekOnePrimaryKey(Slices.AfterAllKeys, out tvr) == false)
                                    break;
                                table.Delete(tvr.Id);
                            }
                        }

                        entries = reader.ReadInt64();
                        for (long i = 0; i < entries; i++)
                        {
                            token.ThrowIfCancellationRequested();
                            size = reader.ReadInt32();
                            reader.ReadExactly(size);

                            if (dryRun == false)
                            {
                                fixed (byte* pBuffer = reader.Buffer)
                                {
                                    tvr = new TableValueReader(pBuffer, size);
                                    table.Insert(ref tvr);
                                }
                            }
                        }
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(type), type.ToString());
                }
                
                type = reader.ReadInt32();
            }
        }

        private void ReadInstallSnapshotAndIgnoreContent(CancellationToken token)
        {
            var reader = _connection.CreateReader();
            while (true)
            {
                token.ThrowIfCancellationRequested();

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
                            token.ThrowIfCancellationRequested();

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
                            token.ThrowIfCancellationRequested();

                            size = reader.ReadInt32();
                            reader.ReadExactly(size);
                        }
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(type), type.ToString());
                }
            }
        }

        private void MaybeNotifyLeaderThatWeAreStillAlive(JsonOperationContext context, Stopwatch sp)
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

        private bool NegotiateMatchEntryWithLeaderAndApplyEntries(TransactionOperationContext context, RemoteConnection connection, LogLengthNegotiation negotiation)
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
                    RequestAllEntries(context, connection, "No entries at all here, give me everything from the start");
                    return true; // leader will know where to start from here
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
                    if (CanHandleLogDivergence(context, negotiation, ref midpointIndex, ref midpointTerm, ref minIndex, ref maxIndex) == false)
                    {
                        RequestAllEntries(context, connection, "all my entries are invalid, will require snapshot.");
                        return true;
                    }
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

                    RequestAllEntries(context, connection, "We have entries that are already truncated at the leader, will ask for full snapshot");
                    return true;
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

            return false;
        }

        private bool CanHandleLogDivergence(TransactionOperationContext context, LogLengthNegotiation negotiation, ref long midpointIndex, ref long midpointTerm,
            ref long minIndex, ref long maxIndex)
        {
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info(
                    $"{ToString()}: Our appended entries has been diverged, same index with different terms. " +
                    $"My index/term {midpointIndex}/{midpointTerm}, while yours is {negotiation.PrevLogIndex}/{negotiation.PrevLogTerm}.");
            }

            using (context.OpenReadTransaction())
            {
                do 
                {
                    // try to find any log in the previous term
                    midpointIndex--;
                    midpointTerm = _engine.GetTermFor(context, midpointIndex) ?? 0;
                } while (midpointTerm >= negotiation.PrevLogTerm && midpointIndex > 0);

                if (midpointTerm == 0 || midpointIndex == 0)
                    return false;

                // start the binary search again with those boundaries
                minIndex = Math.Min(midpointIndex, _engine.GetFirstEntryIndex(context));
                maxIndex = midpointIndex;
                midpointIndex = (minIndex + maxIndex) / 2;
                midpointTerm = _engine.GetTermForKnownExisting(context, midpointIndex);

                if (maxIndex < minIndex)
                {
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"{ToString()}: Got minIndex: {minIndex} bigger than maxIndex: {maxIndex} will request the entire snapshot. " +
                                         $"midpointIndex: {midpointIndex}, midpointTerm: {midpointTerm}.");
                    }

                    Debug.Assert(false, "This is a safeguard against any potential bug here, so in worst case we request the entire snapshot");
                    return false;
                }
            }

            return true;
        }

        private void RequestAllEntries(TransactionOperationContext context, RemoteConnection connection, string message)
        {
            connection.Send(context,
                new LogLengthNegotiationResponse
                {
                    Status = LogLengthNegotiationResponse.ResponseStatus.Acceptable,
                    Message = message,
                    CurrentTerm = _term,
                    LastLogIndex = 0
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
            
            _engine.InMemoryDebug.RemoveRecorderOlderThan(DateTime.UtcNow.AddMinutes(-5));
        }
    }
}
