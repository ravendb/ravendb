using System;
using System.Threading;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Rachis.Remote;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Rachis
{
    public class Elector : IDisposable
    {
        private const int TimeToWaitForElectorToFinishInMs = 60 * 1000;

        private readonly RachisConsensus _engine;
        private readonly RemoteConnection _connection;
        private PoolOfThreads.LongRunningWork _electorLongRunningWork;
        private bool _electionWon;

        public Elector(RachisConsensus engine, RemoteConnection connection)
        {
            _engine = engine;
            _connection = connection;
        }

        public void RunAndWait()
        {
            var name = $"Elector for candidate {_connection.Source}";

            _electorLongRunningWork = PoolOfThreads.GlobalRavenThreadPool.LongRunning(x => HandleVoteRequest(), null, name);

            if (_electorLongRunningWork.Join(TimeToWaitForElectorToFinishInMs) == false)
                throw new InvalidOperationException($"{name} did not finish processing in {TimeToWaitForElectorToFinishInMs}ms."); // throwing will dispose the elector
        }

        public override string ToString()
        {
            return $"Elector {_engine.Tag} for {_connection.Source}";
        }

        private void HandleVoteRequest()
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
                        _engine.Log.Info("Elector was unable to set the thread priority, will continue with the same priority", e);
                    }
                }

                while (_engine.IsDisposed == false)
                {
                    using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    {
                        var rv = _connection.Read<RequestVote>(context);

                        if (_engine.Log.IsInfoEnabled)
                        {
                            var election = rv.IsTrialElection ? "Trial" : "Real";
                            _engine.Log.Info($"Received ({election}) 'RequestVote' from {rv.Source}: Election is {rv.ElectionResult} in term {rv.Term} while our current term is {_engine.CurrentTerm}, " +
                                $"Forced election is {rv.IsForcedElection}. (Sent from:{rv.SendingThread})");
                        }

                        //We are getting a request to vote for our known leader
                        if (_engine.LeaderTag == rv.Source)
                        {
                            _engine.LeaderTag = null;
                            //If we are followers we want to drop the connection with the leader right away.
                            //We shouldn't be in any other state since if we are candidate our leaderTag should be null but its safer to verify.
                            if (_engine.CurrentState == RachisState.Follower)
                                _engine.SetNewState(RachisState.Follower, null, _engine.CurrentTerm, $"We got a vote request from our leader {rv.Source} so we switch to leaderless state.");
                        }

                        ClusterTopology clusterTopology;
                        long lastLogIndex;
                        long lastLogTerm;
                        string whoGotMyVoteIn;
                        long lastVotedTerm;

                        using (context.OpenReadTransaction())
                        {
                            lastLogIndex = _engine.GetLastEntryIndex(context);
                            lastLogTerm = _engine.GetTermForKnownExisting(context, lastLogIndex);
                            (whoGotMyVoteIn, lastVotedTerm) = _engine.GetWhoGotMyVoteIn(context, rv.Term);

                            clusterTopology = _engine.GetTopology(context);
                        }

                        // this should be only the case when we where once in a cluster, then we were brought down and our data was wiped.
                        if (clusterTopology.TopologyId == null)
                        {
                            _connection.Send(context, new RequestVoteResponse
                            {
                                Term = rv.Term,
                                VoteGranted = true,
                                Message = "I might vote for you, because I'm not part of any cluster."
                            });
                            continue;
                        }

                        if (clusterTopology.Members.ContainsKey(rv.Source) == false &&
                            clusterTopology.Promotables.ContainsKey(rv.Source) == false &&
                            clusterTopology.Watchers.ContainsKey(rv.Source) == false)
                        {
                            _connection.Send(context, new RequestVoteResponse
                            {
                                Term = _engine.CurrentTerm,
                                VoteGranted = false,
                                // we only report to the node asking for our vote if we are the leader, this gives
                                // the oust node a authoritative confirmation that they were removed from the cluster
                                NotInTopology = _engine.CurrentState == RachisState.Leader,
                                Message = $"Node {rv.Source} is not in my topology, cannot vote for it"
                            });
                            return;
                        }

                        var currentTerm = _engine.CurrentTerm;
                        if (rv.Term == currentTerm && rv.ElectionResult == ElectionResult.Won)
                        {
                            if (Follower.CheckIfValidLeader(_engine, _connection, out var negotiation))
                            {
                                var follower = new Follower(_engine, negotiation.Term, _connection);
                                follower.AcceptConnection(negotiation);
                                _electionWon = true;
                            }

                            return;
                        }

                        if (rv.ElectionResult != ElectionResult.InProgress)
                        {
                            return;
                        }

                        if (rv.Term <= _engine.CurrentTerm)
                        {
                            _connection.Send(context, new RequestVoteResponse
                            {
                                Term = _engine.CurrentTerm,
                                VoteGranted = false,
                                Message = "My term is higher or equals to yours"
                            });
                            return;
                        }

                        if (rv.LastLogTerm < lastLogTerm)
                        {
                            _connection.Send(context, new RequestVoteResponse
                                {
                                    Term = _engine.CurrentTerm,
                                    VoteGranted = false,
                                    Message = $"My last log term is {lastLogTerm} and higher than yours {rv.LastLogTerm}"
                                });
                            return;
                        }


                        if (rv.IsForcedElection == false &&
                            (
                                _engine.CurrentState == RachisState.Leader ||
                                _engine.CurrentState == RachisState.LeaderElect
                            )
                        )
                        {
                            _connection.Send(context, new RequestVoteResponse
                                {
                                    Term = _engine.CurrentLeader.Term,
                                    VoteGranted = false,
                                    Message = "I'm a leader in good standing, coup will be resisted"
                                });
                            return;
                        }

                        if (whoGotMyVoteIn != null && whoGotMyVoteIn != rv.Source)
                        {
                            _connection.Send(context, new RequestVoteResponse
                                {
                                    Term = _engine.CurrentTerm,
                                    VoteGranted = false,
                                    Message = $"Already voted in {rv.LastLogTerm}, for {whoGotMyVoteIn}"
                                });
                            continue;
                        }

                        if (lastVotedTerm > rv.Term)
                        {
                            _connection.Send(context, new RequestVoteResponse
                                {
                                    Term = _engine.CurrentTerm,
                                    VoteGranted = false,
                                    Message = $"Already voted for another node in {lastVotedTerm}"
                                });
                            continue;
                        }

                        if (rv.Term > _engine.CurrentTerm + 1)
                        {
                            // trail election is often done on the current term + 1, but if there is any
                            // election on a term that is greater than the current term plus one, we should
                            // consider this an indication that the cluster was able to move past our term
                            // and update the term accordingly
                            using (context.OpenWriteTransaction())
                            {
                                // double checking things under the transaction lock
                                if (rv.Term > _engine.CurrentTerm + 1)
                                {
                                    _engine.CastVoteInTerm(context, rv.Term - 1, null, "Noticed that the term in the cluster grew beyond what I was familiar with, increasing it");
                                }
                                context.Transaction.Commit();
                            }

                            _connection.Send(context, new RequestVoteResponse
                            {
                                Term = _engine.CurrentTerm,
                                VoteGranted = false,
                                Message = $"Increasing my term to {_engine.CurrentTerm}"
                            });
                            continue;
                        }

                        if (rv.IsTrialElection)
                        {
                            if (_engine.Timeout.ExpiredLastDeferral(_engine.ElectionTimeout.TotalMilliseconds / 2, out string currentLeader) == false
                                && string.IsNullOrEmpty(currentLeader) == false) // if we are leaderless we can't refuse to cast our vote.
                            {
                                _connection.Send(context, new RequestVoteResponse
                                    {
                                        Term = _engine.CurrentTerm,
                                        VoteGranted = false,
                                        Message = $"My leader {currentLeader} is keeping me up to date, so I don't want to vote for you"
                                    });
                                continue;
                            }

                            if (lastLogTerm == rv.LastLogTerm && lastLogIndex > rv.LastLogIndex)
                            {
                                _connection.Send(context, new RequestVoteResponse
                                    {
                                        Term = _engine.CurrentTerm,
                                        VoteGranted = false,
                                        Message = $"My log {lastLogIndex} is more up to date than yours {rv.LastLogIndex}"
                                    });
                                continue;
                            }

                            _connection.Send(context, new RequestVoteResponse
                            {
                                Term = rv.Term,
                                VoteGranted = true,
                                Message = "I might vote for you"
                            });
                            continue;
                        }


                        _engine.ForTestingPurposes?.BeforeCastingForRealElection();

                        HandleVoteResult result;
                        using (context.OpenWriteTransaction())
                        {
                            result = ShouldGrantVote(context, lastLogIndex, rv);
                            if (result.DeclineVote == false)
                            {
                                _engine.CastVoteInTerm(context, rv.Term, rv.Source, "Casting vote as elector");
                                context.Transaction.Commit();
                            }
                        }

                        if (result.DeclineVote)
                        {
                            _connection.Send(context, new RequestVoteResponse
                            {
                                Term = result.VotedTerm,
                                VoteGranted = false,
                                Message = result.DeclineReason
                            });
                        }
                        else
                        {
                            _connection.Send(context, new RequestVoteResponse
                            {
                                Term = rv.Term,
                                VoteGranted = true,
                                Message = "I've voted for you"
                            });
                        }
                    }
                }
            }
            catch (Exception e) when (IsExpectedException(e))
            {
            }
            catch (Exception e)
            {
                if (_engine.Log.IsInfoEnabled)
                {
                    _engine.Log.Info($"Failed to talk to candidate: {_engine.Tag}", e);
                }
            }
        }

        private static bool IsExpectedException(Exception e)
        {
            if (e is AggregateException)
                return IsExpectedException(e.InnerException);

            return e is OperationCanceledException || e is ObjectDisposedException;
        }

        private class HandleVoteResult
        {
            public string DeclineReason;
            public bool DeclineVote;
            public long VotedTerm;
        }

        private HandleVoteResult ShouldGrantVote(TransactionOperationContext context, long lastIndex, RequestVote rv)
        {
            var result = new HandleVoteResult();
            var lastLogIndexUnderWriteLock = _engine.GetLastEntryIndex(context);
            var lastLogTermUnderWriteLock = _engine.GetTermFor(context, lastLogIndexUnderWriteLock);

            if (lastLogIndexUnderWriteLock != lastIndex)
            {
                result.DeclineVote = true;
                result.DeclineReason = "Log was changed";
                return result;
            }

            if (lastLogTermUnderWriteLock > rv.LastLogTerm)
            {
                result.DeclineVote = true;
                result.DeclineReason = $"My last log term {lastLogTermUnderWriteLock}, is higher than yours {rv.LastLogTerm}.";
                return result;
            }

            if (lastLogIndexUnderWriteLock > rv.LastLogIndex)
            {
                result.DeclineVote = true;
                result.DeclineReason = $"Vote declined because my last log index {lastLogIndexUnderWriteLock} is more up to date than yours {rv.LastLogIndex}";
                return result;
            }

            var (whoGotMyVoteIn, votedTerm) = _engine.GetWhoGotMyVoteIn(context, rv.Term);
            result.VotedTerm = votedTerm;

            if (whoGotMyVoteIn != null && whoGotMyVoteIn != rv.Source)
            {
                result.DeclineVote = true;
                result.DeclineReason = $"Already voted in {rv.LastLogTerm}, for {whoGotMyVoteIn}";
                return result;
            }

            if (votedTerm >= rv.Term)
            {
                result.DeclineVote = true;
                result.DeclineReason = $"Already voted in {rv.LastLogTerm}, for another node in higher term: {votedTerm}";
                return result;
            }

            return result;
        }

        public void Dispose()
        {
            if (_electionWon == false)
                _connection.Dispose();

            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"{ToString()}: Disposing");
            }

            if (_electorLongRunningWork != null && _electorLongRunningWork.ManagedThreadId != Thread.CurrentThread.ManagedThreadId)
                _electorLongRunningWork.Join(int.MaxValue);

            _engine.InMemoryDebug.RemoveRecorderOlderThan(DateTime.UtcNow.AddMinutes(-5));
        }
    }
}
