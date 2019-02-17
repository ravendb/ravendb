using System;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Rachis
{
    public class Elector
    {
        private readonly RachisConsensus _engine;
        private readonly RemoteConnection _connection;
        private PoolOfThreads.LongRunningWork _electorLongRunningWork;
        private bool _electionWon;

        public Elector(RachisConsensus engine, RemoteConnection connection)
        {
            _engine = engine;
            _connection = connection;
        }

        public void Run()
        {
            _electorLongRunningWork = PoolOfThreads.GlobalRavenThreadPool.LongRunning(x => HandleVoteRequest(), null, $"Elector for candidate {_connection.Source}");
        }

        public void HandleVoteRequest()
        {
            try
            {
                while (true)
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
                                _engine.SetNewState(RachisState.Follower,null,_engine.CurrentTerm,$"We got a vote request from our leader {rv.Source} so we switch to leaderless state.");
                        }

                        ClusterTopology clusterTopology;
                        long lastIndex;
                        long lastTerm;
                        string whoGotMyVoteIn;
                        long lastVotedTerm;

                        using (context.OpenReadTransaction())
                        {
                            lastIndex = _engine.GetLastEntryIndex(context);
                            lastTerm = _engine.GetTermForKnownExisting(context, lastIndex);
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
                            _connection.Dispose();
                            return;
                        }

                        var currentTerm = _engine.CurrentTerm;
                        if (rv.Term == currentTerm && rv.ElectionResult == ElectionResult.Won)
                        {
                            _electionWon = true;
                            if (Follower.CheckIfValidLeader(_engine, _connection,out var negotiation))
                            {
                                var follower = new Follower(_engine, negotiation.Term, _connection);
                                follower.AcceptConnection(negotiation);
                            }
                            return;
                        }

                        if (rv.ElectionResult != ElectionResult.InProgress)
                        {
                            _connection.Dispose();
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
                            _connection.Dispose();
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
                            _connection.Dispose();
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
                        if(lastVotedTerm > rv.Term)
                        {
                            _connection.Send(context, new RequestVoteResponse
                            {
                                Term = _engine.CurrentTerm,
                                VoteGranted = false,
                                Message = $"Already voted for another node in {lastVotedTerm}"
                            });
                            continue;
                        }

                        if (lastTerm > rv.LastLogTerm)
                        {
                            // we aren't going to vote for this guy, but we need to check if it is more up to date
                            // in the state of the cluster than we are
                            if(rv.Term > _engine.CurrentTerm + 1)
                            {
                                // trail election is often done on the current term + 1, but if there is any
                                // election on a term that is greater than the current term plus one, we should
                                // consider this an indication that the cluster was able to move past our term
                                // and update the term accordingly
                                using (context.OpenWriteTransaction())
                                {
                                    // double checking things under the transaction lock
                                    if(rv.Term > _engine.CurrentTerm + 1)
                                    {
                                        _engine.CastVoteInTerm(context, rv.Term -1 , null, "Noticed that the term in the cluster grew beyond what I was familiar with, increasing it");
                                    }
                                    context.Transaction.Commit();
                                }
                            }

                            _connection.Send(context, new RequestVoteResponse
                            {
                                Term = _engine.CurrentTerm,
                                VoteGranted = false,
                                Message = $"My last log entry is of term {lastTerm} / {lastIndex} while yours is {rv.LastLogTerm}, so I'm more up to date"
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

                            if (lastTerm == rv.LastLogTerm && lastIndex > rv.LastLogIndex)
                            {
                                _connection.Send(context, new RequestVoteResponse
                                {
                                    Term = _engine.CurrentTerm,
                                    VoteGranted = false,
                                    Message = $"My log {lastIndex} is more up to date than yours {rv.LastLogIndex}"
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

                        HandleVoteResult result;
                        using (context.OpenWriteTransaction())
                        {
                            result = ShouldGrantVote(context, lastIndex, rv, lastTerm);
                            if(result.DeclineVote == false)
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
            finally
            {
                if (_electionWon == false)
                {
                    _connection.Dispose();
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

        private HandleVoteResult ShouldGrantVote(TransactionOperationContext context, long lastIndex, RequestVote rv, long lastTerm)
        {
            var result = new HandleVoteResult();
            var lastEntryUnderWriteLock = _engine.GetLastEntryIndex(context);
            if (lastEntryUnderWriteLock != lastIndex)
            {
                result.DeclineVote = true;
                result.DeclineReason = "Log was changed";
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
            if (lastTerm == rv.LastLogTerm && lastIndex > rv.LastLogIndex)
            {
                result.DeclineVote = true;
                result.DeclineReason = $"Vote declined because my log {lastIndex} is more up to date than yours {rv.LastLogIndex}";
                return result;
            }

            return result;
        }
    }
}
