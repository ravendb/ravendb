using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Rachis
{
    public class Follower : IDisposable
    {
        private readonly RachisConsensus _engine;
        private readonly RemoteConnection _connection;
        private Thread _thread;
        public string DebugCurrentLeader { get; private set; }

        public Follower(RachisConsensus engine, RemoteConnection remoteConnection)
        {
            _engine = engine;
            _connection = remoteConnection;
        }

        private void FollowerSteadyState()
        {
            var entries = new List<RachisEntry>();
            while (true)
            {
                entries.Clear();

                // TODO: how do we shutdown? probably just close the TCP connection
                TransactionOperationContext context;
                using (_engine.ContextPool.AllocateOperationContext(out context))
                {
                    var appendEntries = _connection.Read<AppendEntries>(context);
                    _engine.Timeout.Defer();
                    if (appendEntries.EntriesCount != 0)
                    {
                        for (int i = 0; i < appendEntries.EntriesCount; i++)
                        {
                            entries.Add(_connection.ReadRachisEntry(context));
                            _engine.Timeout.Defer();
                        }
                    }
                    long lastLogIndex;
                    // we start the tx after we finished reading from the network
                    using (var tx = context.OpenWriteTransaction())
                    {
                        if (entries.Count > 0)
                        {
                            _engine.AppendToLog(context, entries);
                        }

                        lastLogIndex = _engine.GetLogEntriesRange(context).Item2;

                        var lastEntryIndexToCommit = Math.Min(
                            lastLogIndex,
                            appendEntries.LeaderCommit);

                        var lastAppliedIndex = _engine.GetLastCommitIndex(context);

                        if (lastEntryIndexToCommit != lastAppliedIndex)
                        {
                            _engine.Apply(context, lastEntryIndexToCommit);
                        }

                        tx.Commit();
                    }

                    _connection.Send(context, new AppendEntriesResponse
                    {
                        CurrentTerm = _engine.CurrentTerm,
                        LastLogIndex = lastLogIndex,
                        Success = true
                    });

                    _engine.Timeout.Defer();

                }
            }
        }


        private AppendEntries CheckIfValidAppendEntries()
        {
            TransactionOperationContext context;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            {
                var fstAppendEntries = _connection.Read<AppendEntries>(context);

                if (fstAppendEntries.Term < _engine.CurrentTerm)
                {
                    _connection.Send(context, new AppendEntriesResponse
                    {
                        Success = false,
                        Message =
                            $"The incoming term {fstAppendEntries.Term} is smaller than current term {_engine.CurrentTerm} and is therefor rejected",
                        CurrentTerm = _engine.CurrentTerm
                    });
                    _connection.Dispose();
                    return null;
                }

                _engine.Timeout.Defer();
                return fstAppendEntries;
            }
        }

        private void NegotiateWithLeader(TransactionOperationContext context, AppendEntries fstAppendEntries)
        {

            // only the leader can send append entries, so if we accepted it, it's the leader
            DebugCurrentLeader = _connection.DebugSource;

            if (fstAppendEntries.Term > _engine.CurrentTerm)
            {
                _engine.FoundAboutHigherTerm(fstAppendEntries.Term);
            }

            var prevTerm = _engine.GetTermFor(fstAppendEntries.PrevLogIndex) ?? 0;
            if (prevTerm != fstAppendEntries.PrevLogTerm)
            {
                // we now have a mismatch with the log position, and need to negotiate it with 
                // the leader
                NegotiateMatchEntryWithLeaderAndApplyEntries(context, _connection, fstAppendEntries);
            }
            else
            {
                // this (or the negotiation above) completes the negotiation process
                _connection.Send(context, new AppendEntriesResponse
                {
                    Success = true,
                    Message = $"Found a log index / term match at {fstAppendEntries.PrevLogIndex} with term {prevTerm}",
                    CurrentTerm = _engine.CurrentTerm,
                    LastLogIndex = fstAppendEntries.PrevLogIndex
                });
            }

            // at this point, the leader will send us a snapshot message
            // in most cases, it is an empty snaphsot, then start regular append entries
            // the reason we send this is to simplify the # of states in the protocol

            var snapshot = _connection.Read<InstallSnapshot>(context);

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
            _connection.Send(context, new AppendEntriesResponse
            {
                Success = true,
                Message =
                    $"Negotiation completed, now at {snapshot.LastIncludedIndex} with term {snapshot.LastIncludedTerm}",
                CurrentTerm = _engine.CurrentTerm,
                LastLogIndex = snapshot.LastIncludedIndex
            });
            _engine.Timeout.Defer();
        }

        private void NegotiateMatchEntryWithLeaderAndApplyEntries(TransactionOperationContext context,
            RemoteConnection connection, AppendEntries aer)
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
                var logEntriesRange = _engine.GetLogEntriesRange(context);

                if (logEntriesRange.Item1 == 0) // no entries at all
                {
                    connection.Send(context, new AppendEntriesResponse
                    {
                        Success = true,
                        Message = "No entries at all here, give me everything from the start",
                        CurrentTerm = _engine.CurrentTerm,
                        LastLogIndex = 0
                    });

                    return; // leader will know where to start from here
                }

                minIndex = logEntriesRange.Item1;
                maxIndex = Math.Min(
                    logEntriesRange.Item2, // max
                    aer.PrevLogIndex
                );

                midpointIndex = (maxIndex + minIndex)/2;

                midpointTerm = _engine.GetTermForKnownExisting(context, midpointIndex);
            }


            while (minIndex < maxIndex)
            {
                _engine.Timeout.Defer();

                // TODO: cancellation
                //_cancellationTokenSource.Token.ThrowIfCancellationRequested();

                connection.Send(context, new AppendEntriesResponse
                {
                    Success = true,
                    Message =
                        $"Term/Index mismatch from leader, need to figure out at what point the logs match, range: {maxIndex} - {minIndex} | {midpointIndex} in term {midpointTerm}",
                    CurrentTerm = _engine.CurrentTerm,
                    Negotiation = new Negotiation
                    {
                        MaxIndex = maxIndex,
                        MinIndex = minIndex,
                        MidpointIndex = midpointIndex,
                        MidpointTerm = midpointTerm
                    }
                });

                var response = connection.Read<AppendEntries>(context);
                if (_engine.GetTermFor(response.PrevLogIndex) == response.PrevLogTerm)
                {
                    minIndex = midpointIndex + 1;
                }
                else
                {
                    maxIndex = midpointIndex - 1;
                }
                midpointIndex = (maxIndex + minIndex)/2;
                using (context.OpenReadTransaction())
                    midpointTerm = _engine.GetTermForKnownExisting(context, midpointIndex);
            }

            connection.Send(context, new AppendEntriesResponse
            {
                Success = true,
                Message = $"Found a log index / term match at {midpointIndex} with term {midpointTerm}",
                CurrentTerm = _engine.CurrentTerm,
                LastLogIndex = midpointIndex
            });
        }

        public void TryAcceptConnection()
        {
            var validAppendEntries = CheckIfValidAppendEntries();
            if (validAppendEntries == null)
            {
                _connection.Dispose();
                return; // did not accept connection
            }

            // if leader / candidate, this remove them from play and revert to follower mode
            _engine.SetNewState(RachisConsensus.State.Follower, this);
            _engine.Timeout.Start(_engine.SwitchToCandidateState);

            _thread = new Thread(Run)
            {
                Name = "Follower thread from " + _connection.DebugSource,
                IsBackground = true
            };
            _thread.Start(validAppendEntries);
        }

        private void Run(object obj)
        {
            try
            {
                using (this)
                {
                    TransactionOperationContext context;
                    using (_engine.ContextPool.AllocateOperationContext(out context))
                    {
                        NegotiateWithLeader(context, (AppendEntries)obj);
                    }

                    FollowerSteadyState();
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
            catch (AggregateException ae)
                when (ae.InnerException is OperationCanceledException || ae.InnerException is ObjectDisposedException)
            {
            }
            catch (Exception e)
            {
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info("Failed to talk to leader: " + _engine.Url, e);
                }
            }
        }

        public void Dispose()
        {
            _connection.Dispose();

            if (_thread != null &&
                _thread.ManagedThreadId != Thread.CurrentThread.ManagedThreadId)
                _thread.Join();
        }
    }
}