using System;
using System.IO;
using System.Threading;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Rachis.Commands;
using Raven.Server.Rachis.Remote;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.Server.Utils;

namespace Raven.Server.Rachis
{
    public sealed class Elector : IDisposable
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
            _engine.AppendElector(this);

            _electorLongRunningWork = PoolOfThreads.GlobalRavenThreadPool.LongRunning(HandleVoteRequest, null, ThreadNames.ForElector($"Elector for candidate {_connection.Source}", _connection.Source));
        }

        public override string ToString()
        {
            return $"Elector {_engine.Tag} for {_connection.Source}";
        }

        private void HandleVoteRequest(object obj)
        {

            try
            {
                ThreadHelper.TrySetThreadPriority(ThreadPriority.AboveNormal, ToString(), _engine.Log);

                using (this)
                {
                    while (_engine.IsDisposed == false)
                    {

                        var current = _engine.CurrentCommittedState;
                        _engine.ForTestingPurposes?.LeaderLock?.HangThreadIfLocked();

                        using (_engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                        {
                            var rv = _connection.Read<RequestVote>(context);

                            if (_engine.Log.IsDebugEnabled)
                            {
                                var election = rv.IsTrialElection ? "Trial" : "Real";
                                _engine.Log.Debug($"Received ({election}) 'RequestVote' from {rv.Source}: Election is {rv.ElectionResult} in term {rv.Term} while our current term is {current.Term}, " +
                                                 $"Forced election is {rv.IsForcedElection}. (Sent from:{rv.SendingThread})");
                            }

                            //We are getting a request to vote for our known leader
                            if (_engine.LeaderTag == rv.Source)
                            {
                                _engine.LeaderTag = null;
                                //If we are followers we want to drop the connection with the leader right away.
                                //We shouldn't be in any other state since if we are candidate our leaderTag should be null but its safer to verify.
                                if (_engine.CurrentCommittedState.State == RachisState.Follower)
                                    _engine.SetNewState(RachisState.Follower, null, current.Term, $"We got a vote request from our leader {rv.Source} so we switch to leaderless state.");
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
                                    Term = current.Term,
                                    VoteGranted = false,
                                    // we only report to the node asking for our vote if we are the leader, this gives
                                    // the oust node a authoritative confirmation that they were removed from the cluster
                                    NotInTopology = current.State == RachisState.Leader,
                                    Message = $"Node {rv.Source} is not in my topology, cannot vote for it"
                                });
                                return;
                            }

                            if (rv.Term == current.Term && rv.ElectionResult == ElectionResult.Won)
                            {
                                var r = Follower.CheckIfValidLeader(_engine, _connection);
                                if (r.Success)
                                {
                                    _electionWon = true;
                                    try
                                    {
                                        var follower = new Follower(_engine, r.Negotiation.Term, _connection);
                                        follower.AcceptConnectionAsync(r.Negotiation).GetAwaiter().GetResult();
                                    }
                                    catch
                                    {
                                        _electionWon = false;
                                        throw;
                                    }
                                }

                                return;
                            }

                            if (rv.ElectionResult != ElectionResult.InProgress)
                            {
                                return;
                            }

                            if (rv.Term <= current.Term)
                            {
                                _connection.Send(context, new RequestVoteResponse
                                {
                                    Term = current.Term,
                                    VoteGranted = false,
                                    Message = "My term is higher or equals to yours"
                                });
                                return;
                            }

                            if (rv.LastLogTerm < lastLogTerm)
                            {
                                _connection.Send(context, new RequestVoteResponse
                                {
                                    Term = current.Term,
                                    VoteGranted = false,
                                    Message = $"My last log term is {lastLogTerm} and higher than yours {rv.LastLogTerm}"
                                });
                                return;
                            }


                            if (rv.IsForcedElection == false &&
                                (
                                    current.State == RachisState.Leader ||
                                    current.State == RachisState.LeaderElect
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
                                    Term = current.Term,
                                    VoteGranted = false,
                                    Message = $"Already voted in {rv.LastLogTerm}, for {whoGotMyVoteIn}"
                                });
                                continue;
                            }

                            if (lastVotedTerm > rv.Term)
                            {
                                _connection.Send(context, new RequestVoteResponse
                                {
                                    Term = current.Term,
                                    VoteGranted = false,
                                    Message = $"Already voted for another node in {lastVotedTerm}"
                                });
                                continue;
                            }

                            if (rv.Term > current.Term + 1)
                            {
                                // trail election is often done on the current term + 1, but if there is any
                                // election on a term that is greater than the current term plus one, we should
                                // consider this an indication that the cluster was able to move past our term
                                // and update the term accordingly
                                var castVoteInTermCommand = new ElectorCastVoteInTermCommand(_engine, rv);
                                _engine.TxMerger.EnqueueSync(castVoteInTermCommand);

                                _connection.Send(context, new RequestVoteResponse
                                {
                                    Term = current.Term,
                                    VoteGranted = false,
                                    Message = $"Increasing my term to {current.Term}"
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
                                        Term = current.Term,
                                        VoteGranted = false,
                                        Message = $"My leader {currentLeader} is keeping me up to date, so I don't want to vote for you"
                                    });
                                    continue;
                                }

                                if (lastLogTerm == rv.LastLogTerm && lastLogIndex > rv.LastLogIndex)
                                {
                                    _connection.Send(context, new RequestVoteResponse
                                    {
                                        Term = current.Term,
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

                            _engine.ForTestingPurposes?.HoldOnLeaderElect?.ReleaseOnLeaderElect();

                            var castVoteInTermWithShouldGrantVoteCommand = new ElectorCastVoteInTermWithShouldGrantVoteCommand(_engine, rv, lastLogIndex);
                            _engine.TxMerger.EnqueueSync(castVoteInTermWithShouldGrantVoteCommand);

                            var result = castVoteInTermWithShouldGrantVoteCommand.VoteResult;

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
            }
            catch (Exception e) when (IsExpectedException(e))
            {
                // ignored
            }
            catch (Exception e)
            {
                var logLevel = e is IOException 
                    ? LogLevel.Debug 
                    : LogLevel.Warn;

                if (_engine.Log.IsEnabled(logLevel))
                    _engine.Log.Log(logLevel, $"Failed to talk to candidate: {_engine.Tag}", e);
            }
        }

        private static bool IsExpectedException(Exception e)
        {
            if (e is AggregateException)
                return IsExpectedException(e.InnerException);

            return e is OperationCanceledException || e is ObjectDisposedException;
        }

        internal sealed class HandleVoteResult
        {
            public string DeclineReason;
            public bool DeclineVote;
            public long VotedTerm;
        }

        public void Dispose()
        {
            if (_electionWon == false)
                _connection.Dispose();

            if (_engine.Log.IsDebugEnabled)
            {
                _engine.Log.Debug($"{ToString()}: Disposing");
            }

            if (_electorLongRunningWork != null && _electorLongRunningWork.ManagedThreadId != Thread.CurrentThread.ManagedThreadId)
                _electorLongRunningWork.Join(int.MaxValue);
        }
    }
}
