using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Sparrow.Threading;

namespace Raven.Server.Rachis
{
    public class Candidate : IDisposable
    {
        private TaskCompletionSource<object> _stateChange = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly RachisConsensus _engine;
        private readonly List<CandidateAmbassador> _voters = new List<CandidateAmbassador>();
        private readonly ManualResetEvent _peersWaiting = new ManualResetEvent(false);
        private Thread _thread;
        public long RunRealElectionAtTerm { get; private set; }

        private MultipleUseFlag _running = new MultipleUseFlag();
        public bool Running => _running.IsRaised();


        public Candidate(RachisConsensus engine)
        {
            _engine = engine;
        }

        public long ElectionTerm { get; private set; }

        private void Run()
        {
            using (this)
            {
                try
                {
                    // Operation may fail, that's why we don't RaiseOrDie
                    _running.Raise();
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"Candidate {_engine.Tag}: Starting elections");
                    }
                    ClusterTopology clusterTopology;
                    using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        ElectionTerm = _engine.CurrentTerm + 1;

                        clusterTopology = _engine.GetTopology(context);

                        tx.Commit();
                    }

                    if (clusterTopology.Members.Count == 1)
                    {
                        _engine.SwitchToLeaderState(ElectionTerm, "I\'m the only one in the cluster, so no need for elections, I rule.");
                        return;
                    }

                    if (IsForcedElection)
                    {
                        CastVoteForSelf();
                    }

                    foreach (var voter in clusterTopology.Members)
                    {
                        if (voter.Key == _engine.Tag)
                            continue; // we already voted for ourselves
                        var candidateAmbassador = new CandidateAmbassador(_engine, this, voter.Key, voter.Value,
                            _engine.ClusterCertificate);
                        _voters.Add(candidateAmbassador);
                        try
                        {
                            _engine.AppendStateDisposable(this, candidateAmbassador);
                        }
                        catch (ConcurrencyException)
                        {
                            return; // we lost the election, because someone else changed our state to follower
                        }
                        candidateAmbassador.Start();
                    }
                    while (_running)
                    {
                        if (_peersWaiting.WaitOne(_engine.Timeout.TimeoutPeriod) == false)
                        {
                            // timeout? 
                            if (IsForcedElection)
                            {
                                CastVoteForSelf();
                            }
                            else
                            {
                                ElectionTerm = _engine.CurrentTerm + 1;
                            }
                            _engine.RandomizeTimeout(extend: true);

                            StateChange(); // will wake ambassadors and make them ping peers again
                            continue;
                        }
                        if (_running == false)
                            return;

                        _peersWaiting.Reset();

                        bool removedFromTopology = false;
                        var trialElectionsCount = 1;
                        var realElectionsCount = 1;
                        foreach (var ambassador in _voters)
                        {
                            if (ambassador.NotInTopology)
                            {
                                removedFromTopology = true;
                                break;
                            }
                            if (ambassador.ReadlElectionWonAtTerm == ElectionTerm)
                                realElectionsCount++;
                            if (ambassador.TrialElectionWonAtTerm == ElectionTerm)
                                trialElectionsCount++;
                        }

                        var majority = ((_voters.Count + 1) / 2) + 1;

                        if (removedFromTopology)
                        {
                            if (_engine.Log.IsInfoEnabled)
                            {
                                _engine.Log.Info($"Candidate {_engine.Tag}: A leader node has indicated that I'm not in their topology, I was probably kicked out. Moving to passive mode");
                            }
                            var engineCurrentTerm = _engine.CurrentTerm;
                            _engine.SetNewState(RachisConsensus.State.Passive, this, engineCurrentTerm,
                                "I just learned from the leader that I\'m not in their topology, moving to passive state");
                            break;
                        }

                        if (realElectionsCount >= majority)
                        {
                            _running.Lower();
                            _engine.SwitchToLeaderState(ElectionTerm, $"Was elected by {majority} nodes to leadership");
                            break;
                        }
                        if (RunRealElectionAtTerm != ElectionTerm &&
                            trialElectionsCount >= majority)
                        {
                            CastVoteForSelf();
                        }
                    }
                }
                catch (Exception e)
                {
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"Candidate {_engine.Tag}:Failure during candidacy run", e);
                    }
                }
            }
        }

        private void CastVoteForSelf()
        {
            using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                ElectionTerm = _engine.CurrentTerm + 1;

                _engine.CastVoteInTerm(context, ElectionTerm, _engine.Tag);

                RunRealElectionAtTerm = ElectionTerm;

                tx.Commit();
            }
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"Candidate {_engine.Tag}: casting vote for self ElectionTerm={ElectionTerm} RunRealElectionAtTerm={RunRealElectionAtTerm}");
            }
            StateChange();
        }

        public bool IsForcedElection { get; set; }

        public Dictionary<string, NodeStatus> GetStatus()
        {
            var dic = new Dictionary<string, NodeStatus>();

            foreach (var voter in _voters)
            {
                var nodeStatus = new NodeStatus { Connected = voter.Status == AmbassadorStatus.Connected };
                if (nodeStatus.Connected == false)
                {
                    nodeStatus.ErrorDetails = voter.StatusMessage;
                }
                dic.Add(voter.Tag,nodeStatus);
            }
           
            return dic;
        }

        private void StateChange()
        {
            var tcs = Interlocked.Exchange(ref _stateChange, new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously));
            tcs.TrySetResult(null);
        }

        public void WaitForChangeInState()
        {
            _peersWaiting.Set();
            _stateChange.Task.Wait();
        }

        public void Start()
        {
            _thread = new Thread(Run)
            {
                Name = "Candidate for - " + _engine.Tag,
                IsBackground = true
            };
            _thread.Start();
        }

        public void Dispose()
        {
            _running.Lower();
            _stateChange.TrySetCanceled();
            _peersWaiting.Set();
            //TODO: shutdown notification of some kind?
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"Candidate {_engine.Tag}: Dispose");
            }
            if (_thread != null && _thread.ManagedThreadId != Thread.CurrentThread.ManagedThreadId)
                _thread.Join();
        }
    }
}
