using System;
using System.Threading;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Rachis
{
    public class Elector
    {
        private readonly RachisConsensus _engine;
        private readonly RemoteConnection _connection;
        private Thread _thread;
        private bool _electionWon;

        public Elector(RachisConsensus engine, RemoteConnection connection)
        {
            _engine = engine;
            _connection = connection;
        }

        public void Run()
        {
            _thread = new Thread(HandleVoteRequest)
            {
                Name = $"{_engine.Tag} elector for candidate {_connection.Source}"
            };
            _thread.Start();
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
                                // the oust node a authorotative confirmation that they were removed from the cluster
                                NotInTopology = _engine.CurrentState == RachisState.Leader,
                                Message = $"Node {rv.Source} is not in my topology, cannot vote for it"
                            });
                            _connection.Dispose();
                            return;
                        }

                        if (rv.Term == _engine.CurrentTerm && rv.ElectionResult == ElectionResult.Won)
                        {
                            _electionWon = true;
                            var follower = new Follower(_engine, _connection);
                            follower.TryAcceptConnection();
                            return;
                        }

                        if (rv.Term == _engine.CurrentTerm && rv.ElectionResult == ElectionResult.Lost)
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
                                Term = _engine.CurrentTerm,
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
                            _connection.Send(context, new RequestVoteResponse
                            {
                                Term = _engine.CurrentTerm,
                                VoteGranted = false,
                                Message = $"My last log entry is of term {lastTerm} / {lastIndex} while yours is {rv.LastLogTerm}, so I'm more up to date"
                            });
                            continue;
                        }

                        if (lastIndex > rv.LastLogIndex)
                        {
                            _connection.Send(context, new RequestVoteResponse
                            {
                                Term = _engine.CurrentTerm,
                                VoteGranted = false,
                                Message =
                                    $"My last log entry is of term {lastTerm} / {lastIndex} while yours is {rv.LastLogTerm} / {rv.LastLogIndex}, so I'm more up to date"
                            });
                            continue;
                        }

                        if (rv.IsTrialElection)
                        {
                            if (_engine.Timeout.ExpiredLastDeferral(_engine.ElectionTimeout.TotalMilliseconds / 2, out string currentLeader) == false)
                            {
                                _connection.Send(context, new RequestVoteResponse
                                {
                                    Term = _engine.CurrentTerm,
                                    VoteGranted = false,
                                    Message = $"My leader {currentLeader} is keeping me up to date, so I don't want to vote for you"
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

                        bool alreadyVoted = false;
                        using (context.OpenWriteTransaction())
                        {
                            long votedTerm;
                            (whoGotMyVoteIn,votedTerm) = _engine.GetWhoGotMyVoteIn(context, rv.Term);
                            if (whoGotMyVoteIn != null && whoGotMyVoteIn != rv.Source)
                            {
                                alreadyVoted = true;
                            }
                            else if(votedTerm > rv.Term)
                            {
                                alreadyVoted = true;
                                whoGotMyVoteIn = "another node in higher term: " + votedTerm;
                            }
                            else
                            {
                                _engine.CastVoteInTerm(context, rv.Term, rv.Source, "Casting vote as elector");
                            }
                            context.Transaction.Commit();
                        }
                        if (alreadyVoted)
                        {
                            _connection.Send(context, new RequestVoteResponse
                            {
                                Term = _engine.CurrentTerm,
                                VoteGranted = false,
                                Message = $"Already voted in {rv.LastLogTerm}, for {whoGotMyVoteIn}"
                            });
                        }
                        else
                        {
                            _connection.Send(context, new RequestVoteResponse
                            {
                                Term = _engine.CurrentTerm,
                                VoteGranted = true,
                                Message = "I've voted for you"
                            });
                        }
                    }
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
                    _engine.Log.Info("Failed to talk to candidate: " + _engine.Tag, e);
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
    }
}
