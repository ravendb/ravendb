using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Rachis
{
    public class Candidate : IDisposable
    {
        private TaskCompletionSource<object> _stateChange = new TaskCompletionSource<object>();
        private readonly RachisConsensus _engine;
        private readonly List<CandidateAmbassador> _voters = new List<CandidateAmbassador>();
        public int ElectionTimeout;
        private long _higherTerm;
        private readonly ManualResetEvent _peersWaiting = new ManualResetEvent(false);
        private Thread _thread;
        public bool RunRealElection { get; private set; }

        public Candidate(RachisConsensus engine)
        {
            _engine = engine;
            ElectionTimeout = new Random().Next((_engine.ElectionTimeoutMs / 3) * 2, _engine.ElectionTimeoutMs);
        }

        public long ElectionTerm { get; private set; }

        private void Run()
        {
            TransactionOperationContext context;
            ClusterTopology clusterTopology;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenReadTransaction())
            {
                ElectionTerm = _engine.CurrentTerm + 1;

                clusterTopology = _engine.GetTopology(context);

                tx.Commit();
            }

            RunRealElection = IsForcedElection;
            if (IsForcedElection)
            {
                CastVoteForSelf();
            }

            foreach (var voter in clusterTopology.Voters)
            {
                if (voter == _engine.Url)
                    continue; // we already voted for ourselves
                var candidateAmbassador = new CandidateAmbassador(_engine, this,voter, clusterTopology.ApiKey);
                _voters.Add(candidateAmbassador);
                _engine.AppendStateDisposable(this, candidateAmbassador);
                candidateAmbassador.Start();
            }

            while (true)
            {
                if (_peersWaiting.WaitOne(_engine.ElectionTimeoutMs) == false)
                {
                    // timeout? 
                    RunRealElection = IsForcedElection;
                    if (IsForcedElection)
                    {
                        CastVoteForSelf();
                    }

                    StateChange(); // will wake ambassadors and make them ping peers again
                    continue;
                }
                _peersWaiting.Reset();

                var term = Interlocked.Read(ref _higherTerm);
                if (term > ElectionTerm)
                {
                    if (_engine.Log.IsInfoEnabled)
                    {
                        _engine.Log.Info(
                            $"New higher term {term} has been discovered, will now wait until new candidate / leader will talk with us");
                    }
                    _engine.Timeout.Start(_engine.SwitchToCandidateState);
                    return;
                }

                var trialElectionsCount = 1;
                var realElectionsCount = 1;
                foreach (var ambassador in _voters)
                {
                    if (ambassador.ReadlElectionWonAtTerm == ElectionTerm)
                        realElectionsCount++;
                    if (ambassador.TrialElectionWonAtTerm == ElectionTerm)
                        trialElectionsCount++;
                }

                var majority = (_voters.Count/2) + 1;
                if (realElectionsCount >= majority)
                {
                    _engine.SwitchToLeaderState();
                    break;
                }
                if (trialElectionsCount >= majority)
                {
                    RunRealElection = true;
                    StateChange();
                }
            }
        }

        private void CastVoteForSelf()
        {
            TransactionOperationContext context;
            using (_engine.ContextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                ElectionTerm = _engine.CurrentTerm + 1;

                _engine.CastVoteInTerm(context, ElectionTerm, _engine.Url);

                tx.Commit();
            }

            StateChange();
        }

        public bool IsForcedElection { get; set; }

        private void StateChange()
        {
            var tcs = Interlocked.Exchange(ref _stateChange, new TaskCompletionSource<object>());
            tcs.TrySetResult(null);
        }

        public void WaitForChangeInState()
        {
            _stateChange.Task.Wait();
        }

        public void Start()
        {
            _thread = new Thread(Run)
            {
                Name = "Consensus Leader - " + _engine.Url,
                IsBackground = true
            };
            _thread.Start();
        }

        public void Dispose()
        {
            //TODO: shutdown notification of some kind?
            if (_thread != null && _thread.ManagedThreadId != Thread.CurrentThread.ManagedThreadId)
                _thread.Join();
        }

        public void HigherTermDiscovered(long term)
        {
            var old = Interlocked.Read(ref _higherTerm);
            if (old >= term)
                return;
            if (Interlocked.CompareExchange(ref _higherTerm, term, old) != old)
                return;

            _peersWaiting.Set();
        }
    }
}