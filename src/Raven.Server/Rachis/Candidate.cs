using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Sparrow.Threading;
using Raven.Server.Utils;

namespace Raven.Server.Rachis
{
    public class Candidate : IDisposable
    {
        private TaskCompletionSource<object> _stateChange = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly RachisConsensus _engine;
        private readonly List<CandidateAmbassador> _voters = new List<CandidateAmbassador>();
        private readonly ManualResetEvent _peersWaiting = new ManualResetEvent(false);
        private PoolOfThreads.LongRunningWork _longRunningWork;
        public long RunRealElectionAtTerm { get; private set; }

        private readonly MultipleUseFlag _running = new MultipleUseFlag(true);
        public bool Running => _running.IsRaised();
        public volatile ElectionResult ElectionResult;

        public Candidate(RachisConsensus engine)
        {
            _engine = engine;
        }

        public long ElectionTerm { get; private set; }

        private void Run()
        {
            try
            {
                try
                {
                    // Operation may fail, that's why we don't RaiseOrDie
                    _running.Raise();
                    ElectionTerm = _engine.CurrentTerm;
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"Candidate {_engine.Tag}: Starting elections");
                    }
                    ClusterTopology clusterTopology;
                    using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        clusterTopology = _engine.GetTopology(context);
                    }

                    if (clusterTopology.Members.Count == 1)
                    {
                        CastVoteForSelf(ElectionTerm + 1, "Single member cluster, natural leader");
                        _engine.SwitchToLeaderState(ElectionTerm, ClusterCommandsVersionManager.CurrentClusterMinimalVersion,
                            "I'm the only one in the cluster, so no need for elections, I rule.");
                        return;
                    }

                    if (IsForcedElection)
                    {
                        CastVoteForSelf(ElectionTerm + 1, "Voting for self in forced elections");
                    }
                    else
                    {
                        ElectionTerm = ElectionTerm + 1;
                    }

                    foreach (var voter in clusterTopology.Members)
                    {
                        if (voter.Key == _engine.Tag)
                            continue; // we already voted for ourselves
                        var candidateAmbassador = new CandidateAmbassador(_engine, this, voter.Key, voter.Value);
                        _voters.Add(candidateAmbassador);
                        try
                        {
                            _engine.AppendStateDisposable(this, candidateAmbassador);
                        }
                        catch (ConcurrencyException)
                        {
                            foreach (var ambassador in _voters)
                            {
                                ambassador.Dispose();
                            }
                            return; // we lost the election, because someone else changed our state to follower
                        }
                        candidateAmbassador.Start();
                    }
                    while (_running && _engine.CurrentState == RachisState.Candidate)
                    {
                        if (_peersWaiting.WaitOne(_engine.Timeout.TimeoutPeriod) == false)
                        {
                            ElectionTerm = _engine.CurrentTerm + 1;
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
                            if (ambassador.RealElectionWonAtTerm == ElectionTerm)
                                realElectionsCount++;
                            if (ambassador.TrialElectionWonAtTerm == ElectionTerm)
                                trialElectionsCount++;
                        }

                        var majority = ((_voters.Count + 1) / 2) + 1;

                        if (removedFromTopology)
                        {
                            if (_engine.Log.IsInfoEnabled)
                            {
                                _engine.Log.Info(
                                    $"Candidate {_engine.Tag}: A leader node has indicated that I'm not in their topology, I was probably kicked out. Moving to passive mode");
                            }
                            _engine.SetNewState(RachisState.Passive, this, _engine.CurrentTerm,
                                "I just learned from the leader that I'm not in their topology, moving to passive state");
                            break;
                        }

                        if (realElectionsCount >= majority)
                        {
                            ElectionResult = ElectionResult.Won;
                            _running.Lower();

                            var connections = new Dictionary<string, RemoteConnection>();
                            var versions = new List<int>
                            {
                                ClusterCommandsVersionManager.MyCommandsVersion
                            };

                            foreach (var candidateAmbassador in _voters)
                            {
                                if (candidateAmbassador.ClusterCommandsVersion > 0)
                                {
                                    versions.Add(candidateAmbassador.ClusterCommandsVersion);
                                }

                                if (candidateAmbassador.TryGetPublishedConnection(out var connection))
                                {
                                    connections[candidateAmbassador.Tag] = connection;
                                }
                            }
                            StateChange();

                            var minimalVersion = ClusterCommandsVersionManager.GetClusterMinimalVersion(versions, _engine.MaximalVersion);
                            string msg = $"Was elected by {realElectionsCount} nodes for leadership in term {ElectionTerm} with cluster version of {minimalVersion}";

                            _engine.SwitchToLeaderState(ElectionTerm, minimalVersion, msg, connections);
                            break;
                        }
                        if (RunRealElectionAtTerm != ElectionTerm &&
                            trialElectionsCount >= majority)
                        {
                            CastVoteForSelf(ElectionTerm, "Won in the trial elections");
                        }
                    }
                }
                catch (Exception e)
                {
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info($"Candidate {_engine.Tag}: Failure during candidacy run with current state of {_engine.CurrentState}", e);
                    }
                    if (_engine.CurrentState == RachisState.Candidate)
                    {
                        // if we are still a candidate, start the candidacy again.
                        _engine.SwitchToCandidateState("An error occurred during the last candidacy: " + e);
                    }
                    else if (_engine.CurrentState != RachisState.Passive)
                    {
                        _engine.Timeout.Start(_engine.SwitchToCandidateStateOnTimeout);
                    }
                }
            }
            finally
            {
                try
                {
                    Dispose();
                }
                catch (Exception)
                {
                    // nothing to be done here
                }
            }
        }

        private void CastVoteForSelf(long electionTerm, string reason, bool setStateChange = true)
        {
            using (_engine.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                _engine.CastVoteInTerm(context, electionTerm, _engine.Tag, reason);

                ElectionTerm = RunRealElectionAtTerm = electionTerm;
                
                tx.Commit();
            }
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"Candidate {_engine.Tag}: casting vote for self ElectionTerm={electionTerm} RunRealElectionAtTerm={RunRealElectionAtTerm}");
            }

            if (setStateChange)
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
            _longRunningWork = PoolOfThreads.GlobalRavenThreadPool.LongRunning(x=>Run(), null, "Candidate for - " + _engine.Tag);
        }

        public void Dispose()
        {
            // We lost the election, if we disposing the candidate without winning 
            if (ElectionResult != ElectionResult.Won)
            {
                ElectionResult = ElectionResult.Lost;
            }
            _running.Lower();
            _stateChange.TrySetResult(null);
            _peersWaiting.Set();
            if (_engine.Log.IsInfoEnabled)
            {
                _engine.Log.Info($"Candidate {_engine.Tag}: Dispose");
            }

            if (_longRunningWork != null && _longRunningWork != PoolOfThreads.LongRunningWork.Current)
                _longRunningWork.Join(Int32.MaxValue);
        }
    }
}
