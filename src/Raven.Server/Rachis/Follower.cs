using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Server.Rachis.Commands;
using Raven.Server.Rachis.Remote;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Utils;
using Sparrow.Utils;

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
            if (_engine.Log.IsDebugEnabled)
            {
                _engine.Log.Debug($"{ToString()}: Entering steady state");
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

                    long committedTerm = _engine.CurrentCommittedState.Term;
                    if (appendEntries.Term != committedTerm)
                    {
                        _connection.Send(context, new AppendEntriesResponse
                        {
                            CurrentTerm = committedTerm,
                            Message = $"The current term that I have {committedTerm} doesn't match {appendEntries.Term}",
                            Success = false
                        });
                        if (_engine.Log.IsWarnEnabled)
                        {
                            _engine.Log.Warn($"{ToString()}: Got invalid term {appendEntries.Term} while the current term is {committedTerm}, aborting connection...");
                        }

                        return;
                    }

                    _engine.CommandsVersionManager.SetClusterVersion(appendEntries.MinCommandVersion);

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
                        if (_engine.Log.IsDebugEnabled)
                        {
                            _engine.Log.Debug($"{ToString()}: Got non empty append entries request with {entries.Count} entries. Last: ({entries[entries.Count - 1].Index} - {entries[entries.Count - 1].Flags})"
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
                                (bool hasRemovedFromTopology, lastAcknowledgedIndex, lastTruncate, lastCommit) =
                                    ApplyLeaderStateToLocalState(sp, entries, appendEntries);

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
                                task.Wait();
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

        private (bool HasRemovedFromTopology, long LastAcknowledgedIndex, long LastTruncate, long LastCommit) ApplyLeaderStateToLocalState(Stopwatch sp, List<RachisEntry> entries, AppendEntries appendEntries)
        {
            // we start the tx after we finished reading from the network
            if (_engine.Log.IsDebugEnabled)
            {
                _engine.Log.Debug($"{ToString()}: Ready to start tx in {sp.Elapsed}");
            }

            var command = new FollowerApplyCommand(_engine, _term, entries, appendEntries, sp);
            _engine.TxMerger.EnqueueSync(command);

            if (_engine.Log.IsDebugEnabled)
            {
                _engine.Log.Debug($"{ToString()}: Processing entries request with {entries.Count} entries took {sp.Elapsed}");
            }

            var lastAcknowledgedIndex = entries.Count == 0 ? appendEntries.PrevLogIndex : entries[^1].Index;

            return (HasRemovedFromTopology: command.RemovedFromTopology, LastAcknowledgedIndex: lastAcknowledgedIndex, LastTruncate: command.LastTruncate, LastCommit: command.LastCommit);
        }

        public static (bool Success, LogLengthNegotiation Negotiation) CheckIfValidLeader(RachisConsensus engine, RemoteConnection connection)
        {
            using (engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            {
                var logLength = connection.Read<LogLengthNegotiation>(context);

                long currentTerm = engine.CurrentCommittedState.Term;
                if (logLength.Term < currentTerm)
                {
                    var msg = $"The incoming term {logLength.Term} is smaller than current term {currentTerm} and is therefor rejected (From thread: {logLength.SendingThread})";
                    if (engine.Log.IsInfoEnabled)
                    {
                        engine.Log.Info(msg);
                    }
                    connection.Send(context, new LogLengthNegotiationResponse
                    {
                        Status = LogLengthNegotiationResponse.ResponseStatus.Rejected,
                        Message = msg,
                        CurrentTerm = currentTerm
                    });
                    connection.Dispose();
                    return (false, null);
                }
                if (engine.Log.IsDebugEnabled)
                {
                    engine.Log.Debug($"The incoming term {logLength.Term} is from a valid leader (From thread: {logLength.SendingThread})");
                }
                engine.FoundAboutHigherTerm(logLength.Term, "Setting the term of the new leader");
                engine.Timeout.Defer(connection.Source);

                return (true, logLength);
            }
        }

        private void NegotiateWithLeader(ClusterOperationContext context, LogLengthNegotiation negotiation)
        {
            _debugRecorder.Start();
            // only the leader can send append entries, so if we accepted it, it's the leader
            if (_engine.Log.IsDebugEnabled)
            {
                _engine.Log.Debug($"{ToString()}: Got a negotiation request for term {negotiation.Term} where our term is {_term}.");
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
                if (_engine.Log.IsDebugEnabled)
                {
                    _engine.Log.Debug($"{ToString()}: Got a negotiation request with PrevLogTerm={negotiation.PrevLogTerm} while our PrevLogTerm={prevTerm}" +
                                     " will negotiate to find next matched index");
                }
                // we now have a mismatch with the log position, and need to negotiate it with 
                // the leader
                NegotiateMatchEntryWithLeaderAndApplyEntries(context, _connection, negotiation);
            }
            else
            {
                if (_engine.Log.IsDebugEnabled)
                {
                    _engine.Log.Debug($"{ToString()}: Got a negotiation request with identical PrevLogTerm will continue to steady state");
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
                KeepAliveAndExecuteAction(async () =>
                {
                    onFullSnapshotInstalledTask = await ReadAndCommitSnapshotAsync(snapshot, cts.Token);
                }, cts, "ReadAndCommitSnapshot");
            }


            // notify the state machine, we do this in an async manner, and start
            // the operator in a separate thread to avoid timeouts while this is
            // going on
            using (var cts = new CancellationTokenSource())
            {
                KeepAliveAndExecuteAction(() => _engine.AfterSnapshotInstalledAsync(snapshot.LastIncludedIndex, onFullSnapshotInstalledTask, cts.Token)
                , cts, "SnapshotInstalledAsync");
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

        private void KeepAliveAndExecuteAction(Func<Task> func, CancellationTokenSource cts, string debug)
        {
            var task = func?.Invoke().WaitAsync(cts.Token);
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

        private async Task<Task> ReadAndCommitSnapshotAsync(InstallSnapshot snapshot, CancellationToken token)
        {
            _debugRecorder.Record("Start receiving the snapshot");

            var command = new FollowerReadAndCommitSnapshotCommand(_engine, this, snapshot, token);
            await _engine.TxMerger.Enqueue(command);

            _debugRecorder.Record("Snapshot was successfully received and committed");

            return command.OnFullSnapshotInstalledTask;
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

                _engine.ValidateLatestTerm(_term);

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

            if (_engine.Log.IsDebugEnabled)
            {
                _engine.Log.Debug($"{ToString()}: agreed upon last matched index = {midpointIndex} on term = {midpointTerm}");
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

        public async Task AcceptConnectionAsync(LogLengthNegotiation negotiation)
        {
            if (_engine.CurrentCommittedState.State != RachisState.Passive)
                _engine.Timeout.Start(_engine.SwitchToCandidateStateOnTimeout);

            // if leader / candidate, this remove them from play and revert to follower mode
            await _engine.SetNewStateAsync(RachisState.Follower, this, _term,
                $"Accepted a new connection from {_connection.Source} in term {negotiation.Term}", beforeStateChangedEvent: () =>
                {
                    _engine.LeaderTag = _connection.Source;
                });

            _debugRecorder.Record("Follower connection accepted");

            _followerLongRunningWork =
                PoolOfThreads.GlobalRavenThreadPool.LongRunning(
                    action: Run,
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
                var logLevel = e is IOException
                    ? LogLevel.Debug
                    : LogLevel.Info;

                if (_engine.Log.IsEnabled(logLevel))
                {
                    _engine.Log.Log(logLevel, "Failed to dispose follower when talking to the leader: " + _engine.Tag, e);
                }
            }
        }

        public void Dispose()
        {
            _connection.Dispose();
            if (_engine.Log.IsDebugEnabled)
            {
                _engine.Log.Debug($"{ToString()}: Disposing");
            }

            if (_followerLongRunningWork != null && _followerLongRunningWork.ManagedThreadId != Thread.CurrentThread.ManagedThreadId)
                _followerLongRunningWork.Join(int.MaxValue);

            _engine.InMemoryDebug.RemoveRecorderOlderThan(DateTime.UtcNow.AddMinutes(-5));
        }
    }
}
