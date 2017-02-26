using System;
using System.Threading;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Rachis
{
    public class Elector : IDisposable
    {
        private readonly RachisConsensus _engine;
        private readonly RemoteConnection _connection;
        private Thread _thread;

        public Elector(RachisConsensus engine, RemoteConnection connection)
        {
            _engine = engine;
            _connection = connection;
        }

        public void HandleVoteRequest()
        {
            try
            {
                while (true)
                {
                    TransactionOperationContext context;
                    using (_engine.ContextPool.AllocateOperationContext(out context))
                    {
                        var rv = _connection.Read<RequestVote>(context);
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

                        if (_engine.CurrentState == RachisConsensus.State.Leader ||
                            _engine.CurrentState == RachisConsensus.State.LeaderElect)
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

                        long lastIndex;
                        long lastTerm;
                        string whoGotMyVoteIn;

                        using (context.OpenReadTransaction())
                        {
                            lastIndex = _engine.GetLastEntryIndex(context);
                            lastTerm = _engine.GetTermForKnownExisting(context, lastIndex);
                            whoGotMyVoteIn = _engine.GetWhoGotMyVoteIn(context, rv.Term);
                        }

                        if (whoGotMyVoteIn != null && whoGotMyVoteIn != rv.Source)
                        {
                            _connection.Send(context, new RequestVoteResponse
                            {
                                Term = _engine.CurrentTerm,
                                VoteGranted = false,
                                Message = $"Already voted in {rv.LastLogTerm}, for {whoGotMyVoteIn}"
                            });
                            _connection.Dispose();
                            return;
                        }

                        if (lastTerm > rv.LastLogTerm)
                        {
                            _connection.Send(context, new RequestVoteResponse
                            {
                                Term = _engine.CurrentTerm,
                                VoteGranted = false,
                                Message = $"My last log entry is of term {lastTerm} while yours is {rv.LastLogTerm}, so I'm more up to date"
                            });
                            _connection.Dispose();
                            return;
                        }

                        if (lastIndex > rv.LastLogIndex)
                        {
                            _connection.Send(context, new RequestVoteResponse
                            {
                                Term = _engine.CurrentTerm,
                                VoteGranted = false,
                                Message = $"My last log entry is of term {lastTerm} / {lastIndex} while yours is {rv.LastLogTerm} / {rv.LastLogIndex}, so I'm more up to date"
                            });
                            _connection.Dispose();
                            return;
                        }

                        if (rv.IsTrialElection)
                        {
                            string currentLeader;
                            if (_engine.Timeout.ExpiredLastDeferral(_engine.ElectionTimeoutMs / 2,out currentLeader) == false)
                            {
                                _connection.Send(context, new RequestVoteResponse
                                {
                                    Term = _engine.CurrentTerm,
                                    VoteGranted = false,
                                    Message = $"My leader {currentLeader} is keeping me up to date, so I don't want to vote for you"
                                });
                                _connection.Dispose();
                                return;
                            }

                            _connection.Send(context, new RequestVoteResponse
                            {
                                Term = rv.Term,
                                VoteGranted = true,
                                Message = "I might vote for you"
                            });
                            if (_thread == null) // let's wait for this in another thread
                            {
                                _thread = new Thread(HandleVoteRequest)
                                {
                                    Name =
                                        "Elector thread for " +
                                        (new Uri(rv.Source).Fragment ?? rv.Source) + " > " + (new Uri(_engine.Url).Fragment ?? _engine.Url),
                                    IsBackground = true
                                };
                                _thread.Start();
                                return;
                            }
                            continue;
                        }

                        bool alreadyVoted = false;
                        using (context.OpenWriteTransaction())
                        {
                            whoGotMyVoteIn = _engine.GetWhoGotMyVoteIn(context, rv.Term);
                            if (whoGotMyVoteIn != null && whoGotMyVoteIn != rv.Source)
                            {
                                alreadyVoted = true;
                            }
                            else
                            {
                                _engine.CastVoteInTerm(context, rv.Term, rv.Source);
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
                            _engine.SetNewState(RachisConsensus.State.Follower, this, rv.Term);

                            _connection.Send(context, new RequestVoteResponse
                            {
                                Term = _engine.CurrentTerm,
                                VoteGranted = true,
                                Message = "I've voted for you"
                            });
                        }
                        _connection.Dispose();
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
                    _engine.Log.Info("Failed to talk to leader: " + _engine.Url, e);
                }
            }
        }

        public void Dispose()
        {
            _connection?.Dispose();
            if (_thread != null &&
                _thread.ManagedThreadId != Thread.CurrentThread.ManagedThreadId)
                _thread.Join();
        }
    }
}