﻿using System;
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
using Sparrow.Server.Utils;
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
        public RachisConsensus Engine => _engine;
        private readonly long _term;
        private readonly RemoteConnection _connection;
        public RemoteConnection Connection => _connection;
        private PoolOfThreads.LongRunningWork _followerLongRunningWork;

        private readonly string _debugName;
        private readonly RachisLogRecorder _debugRecorder;
        public RachisLogRecorder DebugRecorder => _debugRecorder;
        public FollowerDebugView.FollowerPhase Phase = FollowerDebugView.FollowerPhase.Initial;
        public RaftDebugView ToDebugView => new FollowerDebugView(this);

        public Follower(RachisConsensus engine, long term, RemoteConnection remoteConnection)
        {
            _engine = engine;
            _connection = remoteConnection;
            _term = term;
            engine.Follower = this;

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

            AppendEntriesResponse lastAer = null;

            var sw = Stopwatch.StartNew();

            while (true)
            {
                entries.Clear();

                using (_engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
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

                    _debugRecorder.Record($"Got {appendEntries.EntriesCount} entries");
                    _engine.Timeout.Defer(_connection.Source);
                    if (appendEntries.EntriesCount != 0)
                    {
                        using (var cts = new CancellationTokenSource())
                        {
                            var task = Concurrent_SendAppendEntriesPendingToLeaderAsync(cts, _term, appendEntries.PrevLogIndex);
                            try
                            {
                                for (int i = 0; i < appendEntries.EntriesCount; i++)
                                {
                                    entries.Add(_connection.ReadRachisEntry(context));
                                }
                            }
                            finally
                            {
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
                    _debugRecorder.Record($"Finished reading {entries.Count} entries from stream");

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
                    var curAer = new AppendEntriesResponse { CurrentTerm = _term, LastLogIndex = lastAcknowledgedIndex, LastCommitIndex = lastCommit, Success = true };

                    bool shouldLog = false;
                    if (sw.Elapsed.TotalMilliseconds > 10_000)
                    {
                        shouldLog = true;
                        sw.Restart();
                    }
                    else
                    {
                        shouldLog = curAer.Equals(lastAer) == false;
                    }

                    _connection.Send(context, curAer, shouldLog);
                    lastAer = curAer;


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

        private (bool HasRemovedFromTopology, long LastAcknowledgedIndex, long LastTruncate, long LastCommit) ApplyLeaderStateToLocalState(Stopwatch sp, ClusterOperationContext context, List<RachisEntry> entries, AppendEntries appendEntries)
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

            return (HasRemovedFromTopology: removedFromTopology, LastAcknowledgedIndex: lastAcknowledgedIndex, LastTruncate: lastTruncate, LastCommit: lastCommit);
        }

        public static bool CheckIfValidLeader(RachisConsensus engine, RemoteConnection connection, out LogLengthNegotiation negotiation)
        {
            negotiation = null;
            using (engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
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

        private void NegotiateWithLeader(ClusterOperationContext context, LogLengthNegotiation negotiation)
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
            
            Phase = FollowerDebugView.FollowerPhase.Snapshot;
            var snapshot = _connection.ReadInstallSnapshot(context);
            _debugRecorder.Record($"Got snapshot info: last included index:{snapshot.LastIncludedIndex} at term {snapshot.LastIncludedTerm}");

            // reading the snapshot from network and committing it to the disk might take a long time. 
            Task onFullSnapshotInstalledTask = null;
            using (var cts = new CancellationTokenSource())
            {
                KeepAliveAndExecuteAction(() =>
                {
                    onFullSnapshotInstalledTask = ReadAndCommitSnapshot(snapshot, cts.Token);
                }, cts, "ReadAndCommitSnapshot");
            }


            // notify the state machine, we do this in an async manner, and start
            // the operator in a separate thread to avoid timeouts while this is
            // going on
            using (var cts = new CancellationTokenSource())
            {
                KeepAliveAndExecuteAction(() =>
                {
                    _engine.AfterSnapshotInstalled(snapshot.LastIncludedIndex, onFullSnapshotInstalledTask, cts.Token);
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

        private Task ReadAndCommitSnapshot(InstallSnapshot snapshot, CancellationToken token)
        {
            Task onFullSnapshotInstalledTask = null;
            _debugRecorder.Record("Start receiving the snapshot");

            using (_engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
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

                    onFullSnapshotInstalledTask = _engine.OnSnapshotInstalled(context, snapshot.LastIncludedIndex, token);
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
            _debugRecorder.Record("Snapshot was successfully received and committed");

            return onFullSnapshotInstalledTask;
        }

        private bool InstallSnapshot(ClusterOperationContext context, CancellationToken token)
        {
            var txw = context.Transaction.InnerTransaction;

            var fileName = $"snapshot.{Guid.NewGuid():N}";
            var filePath = context.Environment.Options.DataPager.Options.TempPath.Combine(fileName);

            using (var temp = new StreamsTempFile(filePath.FullPath, context.Environment))
            using (var stream = temp.StartNewStream())
            using (var remoteReader = _connection.CreateReaderToStream(_debugRecorder, stream))
            {
                if (ReadSnapshot(remoteReader, context, txw, dryRun: true, token) == false)
                    return false;

                _debugRecorder.Record($"Finished reading the snapshot from stream with total size of {remoteReader.ReadSize}");
                _debugRecorder.Record("Start applying the snapshot");

                stream.Seek(0, SeekOrigin.Begin);
                using (var fileReader = new StreamSnapshotReader(_debugRecorder, stream))
                {
                    ReadSnapshot(fileReader, context, txw, dryRun: false, token);
                }

                _debugRecorder.Record("Finished applying the snapshot");
            }

            return true;
        }

        private unsafe bool ReadSnapshot(SnapshotReader reader, ClusterOperationContext context, Transaction txw, bool dryRun, CancellationToken token)
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
                        Slice.From(context.Allocator, reader.Buffer, 0, size, ByteStringType.Immutable, out Slice treeName); // The Slice will be freed on context close

                        entries = reader.ReadInt64();
                        var flags = TreeFlags.FixedSizeTrees;

                        if (dryRun == false)
                        {
                            _debugRecorder.Record($"Install {treeName}");
                            txw.DeleteTree(treeName);
                            tree = txw.CreateTree(treeName);
                        }

                        if (_connection.Features.Cluster.MultiTree)
                            flags = (TreeFlags)reader.ReadInt32();

                        for (long i = 0; i < entries; i++)
                        {
                            token.ThrowIfCancellationRequested();
                            // read key
                            size = reader.ReadInt32();
                            reader.ReadExactly(size);

                            using (Slice.From(context.Allocator, reader.Buffer, 0, size, ByteStringType.Immutable, out Slice valKey))
                            {
                                switch (flags)
                                {
                                    case TreeFlags.None:

                                        // this is a very specific code to block receiving 'CompareExchangeByExpiration' which is a multi-value tree
                                        // while here we expect a normal tree
                                        if (SliceComparer.Equals(valKey, CompareExchangeExpirationStorage.CompareExchangeByExpiration))
                                            throw new InvalidOperationException($"{valKey} is a multi-tree, please upgrade the leader node.");

                                        // read value
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
                                        break;
                                    case TreeFlags.MultiValueTrees:
                                        var multiEntries = reader.ReadInt64();
                                        for (int j = 0; j < multiEntries; j++)
                                        {
                                            token.ThrowIfCancellationRequested();

                                            size = reader.ReadInt32();
                                            reader.ReadExactly(size);

                                            if (dryRun == false)
                                            {
                                                using (Slice.From(context.Allocator, reader.Buffer, 0, size, ByteStringType.Immutable, out Slice multiVal))
                                                {
                                                    tree.MultiAdd(valKey, multiVal);
                                                }
                                            }
                                        }
                                        break;
                                    default:
                                        throw new ArgumentOutOfRangeException($"Got unkonwn type '{type}'");
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
                            _debugRecorder.Record($"Install {tableName}");

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
            var reader = _connection.CreateReader(_debugRecorder);
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

        private bool NegotiateMatchEntryWithLeaderAndApplyEntries(ClusterOperationContext context, RemoteConnection connection, LogLengthNegotiation negotiation)
        {
            long minIndex;
            long maxIndex;
            long midpointTerm;
            long midpointIndex;
            long lastIndex;

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

                lastIndex = _engine.GetLastEntryIndex(context);

                midpointIndex = (maxIndex + minIndex) / 2;

                midpointTerm = _engine.GetTermForKnownExisting(context, midpointIndex);
            }

            while ((midpointTerm == negotiation.PrevLogTerm && midpointIndex == negotiation.PrevLogIndex) == false)
            {
                _engine.Timeout.Defer(_connection.Source);

                _engine.ValidateTerm(_term);

                if (midpointIndex == negotiation.PrevLogIndex && midpointTerm != negotiation.PrevLogTerm)
                {
                    if (HandleDivergenceAtFirstLeaderEntry(context, negotiation, out var lastCommittedIndex))
                    {
                        connection.Send(context, new LogLengthNegotiationResponse
                        {
                            Status = LogLengthNegotiationResponse.ResponseStatus.Acceptable,
                            Message = $"agreed on our last committed index {lastCommittedIndex}",
                            CurrentTerm = _term,
                            LastLogIndex = lastCommittedIndex,
                        });
                        return false;
                    }

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
                    if (lastIndex + 1 == negotiation.PrevLogIndex)
                    {
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info($"{ToString()}: leader first entry = {negotiation.PrevLogIndex} is the one we need (our last is {lastIndex})");
                        }

                        connection.Send(context, new LogLengthNegotiationResponse
                        {
                            Status = LogLengthNegotiationResponse.ResponseStatus.Acceptable,
                            Message = $"agreed on our last index {lastIndex}",
                            CurrentTerm = _term,
                            LastLogIndex = lastIndex,
                        });

                        // leader's first entry is the next we need 
                        return false;
                    }

                    if (lastIndex + 1 < negotiation.PrevLogIndex)
                    {
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info($"{ToString()}: Got a truncated response from the leader will request all entries");
                        }

                        RequestAllEntries(context, connection, "We have entries that are already truncated at the leader, will ask for full snapshot");
                        return true;
                    }

                    if (HandleDivergenceAtFirstLeaderEntry(context, negotiation, out var lastCommittedIndex))
                    {
                        connection.Send(context, new LogLengthNegotiationResponse
                        {
                            Status = LogLengthNegotiationResponse.ResponseStatus.Acceptable,
                            Message = $"agreed on our last committed index {lastCommittedIndex}",
                            CurrentTerm = _term,
                            LastLogIndex = lastCommittedIndex,
                        });
                        return false;
                    } 

                    // the leader already truncated the suggested index
                    // Let's try to negotiate from that index upto our last appended index
                    maxIndex = lastIndex;
                    minIndex = negotiation.PrevLogIndex;
                }
                else
                {
                    using (context.OpenReadTransaction())
                    {
                        if (_engine.GetTermFor(context, negotiation.PrevLogIndex) == negotiation.PrevLogTerm)
                        {
                            if (negotiation.PrevLogIndex < midpointIndex)
                                //If the value from the leader is lower, it mean that the term of the follower mid value in the leader doesn't match to the term in the follower 
                                maxIndex = Math.Max(midpointIndex - 1, minIndex);
                            
                            minIndex = Math.Min(negotiation.PrevLogIndex, maxIndex);
                        }
                        else
                        {
                            maxIndex = Math.Max(negotiation.PrevLogIndex - 1, minIndex);
                        }
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

        private bool HandleDivergenceAtFirstLeaderEntry(ClusterOperationContext context, LogLengthNegotiation negotiation, out long lastCommittedIndex)
        {
            lastCommittedIndex = -1;

            using (context.OpenReadTransaction())
            {
                var term = _engine.GetTermFor(context, negotiation.PrevLogIndex);
                if (term != negotiation.PrevLogTerm)
                {
                    // divergence at the first leader entry
                    lastCommittedIndex = _engine.GetLastCommitIndex(context);
                    if (lastCommittedIndex + 1 == negotiation.PrevLogIndex)
                    {
                        if (_engine.Log.IsInfoEnabled)
                        {
                            _engine.Log.Info($"{ToString()}: found divergence at the first leader entry");
                        }

                        // leader's first entry is the next we need 
                        return true;
                    }
                }
            }

            return false;
        }

        private bool CanHandleLogDivergence(ClusterOperationContext context, LogLengthNegotiation negotiation, ref long midpointIndex, ref long midpointTerm,
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

        private void RequestAllEntries(ClusterOperationContext context, RemoteConnection connection, string message)
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
            if (_engine.CurrentState != RachisState.Passive)
                _engine.Timeout.Start(_engine.SwitchToCandidateStateOnTimeout);

            // if leader / candidate, this remove them from play and revert to follower mode
            _engine.SetNewState(RachisState.Follower, this, _term,
                $"Accepted a new connection from {_connection.Source} in term {negotiation.Term}",
                beforeStateChangedEvent: () => _engine.LeaderTag = _connection.Source);

            _debugRecorder.Record("Follower connection accepted");

            _followerLongRunningWork =
                PoolOfThreads.GlobalRavenThreadPool.LongRunning(
                    action: x => Run(x),
                    state: negotiation,
                    ThreadNames.ForFollower($"Follower thread from {_connection} in term {negotiation.Term}", _connection.ToString(), negotiation.Term));
        }

        private void Run(object obj)
        {
            try
            {
                ThreadHelper.TrySetThreadPriority(ThreadPriority.AboveNormal, _debugName, _engine.Log);

                using (this)
                {
                    try
                    {
                        _engine.ForTestingPurposes?.LeaderLock?.HangThreadIfLocked();
                        Phase = FollowerDebugView.FollowerPhase.Negotiation;
                        using (_engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                        {
                            NegotiateWithLeader(context, (LogLengthNegotiation)obj);
                        }

                        Phase = FollowerDebugView.FollowerPhase.Steady;
                        FollowerSteadyState();
                    }
                    catch (Exception e) when (RachisConsensus.IsExpectedException(e))
                    {
                    }
                    catch (Exception e)
                    {
                        _debugRecorder.Record($"Sending error: {e}");
                        using (_engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
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
