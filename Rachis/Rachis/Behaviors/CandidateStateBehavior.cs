// -----------------------------------------------------------------------
//  <copyright file="CandidateStateBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Rachis.Messages;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;

namespace Rachis.Behaviors
{
    public class CandidateStateBehavior : AbstractRaftStateBehavior
    {
        private readonly bool _forcedElection;
        private readonly HashSet<string> _votesForMyLeadership = new HashSet<string>();
        private readonly Random _random;
        private bool _wonTrialElection;
        private bool _termIncreaseMightGetMyVote;

        public CandidateStateBehavior(RaftEngine engine, bool forcedElection)
            : base(engine)
        {
            _forcedElection = forcedElection;
            _wonTrialElection = forcedElection;
            _random = new Random((int)(engine.Name.GetHashCode() + DateTime.UtcNow.Ticks));
            Timeout = _random.Next(engine.Options.ElectionTimeout / 2, engine.Options.ElectionTimeout);            
        }

        public override void HandleTimeout()
        {
            _log.Info("Timeout ({1:#,#;;0} ms) for elections in term {0}", Engine.PersistentState.CurrentTerm,
                  Timeout);

            Timeout = _random.Next(Engine.Options.ElectionTimeout / 2, Engine.Options.ElectionTimeout);
            _wonTrialElection = false;
            StartElection();
        }

        private void StartElection()
        {
            LastHeartbeatTime = DateTime.UtcNow;
            _votesForMyLeadership.Clear();

            long currentTerm = Engine.PersistentState.CurrentTerm + 1;
            if (_wonTrialElection || _termIncreaseMightGetMyVote) // only in the real election (or if we have to), we increment the current term 
            {
                Engine.PersistentState.UpdateTermTo(Engine, currentTerm);
            }
            if (_wonTrialElection)// and only if we won an election do we record a firm vote for ourselves
                Engine.PersistentState.RecordVoteFor(Engine.Name, currentTerm);

            _termIncreaseMightGetMyVote = false;

            Engine.CurrentLeader = null;
            _log.Info("Calling for {0} election in term {1}",
                _wonTrialElection ? "an" : "a trial", currentTerm);

            var lastLogEntry = Engine.PersistentState.LastLogEntry();
            var rvr = new RequestVoteRequest
            {
                LastLogIndex = lastLogEntry.Index,
                LastLogTerm = lastLogEntry.Term,
                Term = currentTerm,
                From = Engine.Name,
                ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                TrialOnly = _wonTrialElection == false,
                ForcedElection = _forcedElection
            };

            var allVotingNodes = Engine.CurrentTopology.AllVotingNodes;

            // don't send to yourself the message
            foreach (var votingPeer in allVotingNodes)
            {
                if (votingPeer.Name == Engine.Name)
                    continue;
                Engine.Transport.Send(votingPeer, rvr);
            }
            Engine.EngineStatistics.Elections.LimitedSizeEnqueue(new ElectionInformation()
            {
                StartTime = DateTime.UtcNow,
                CurrentTerm = currentTerm,
                ForcedElection = _forcedElection,
                TermIncreaseMightGetMyVote = _termIncreaseMightGetMyVote,
                VotingNodes = allVotingNodes,
                WonTrialElection = _wonTrialElection
            },RaftEngineStatistics.NumberOfElectionsToTrack);
            Engine.OnCandidacyAnnounced();
            _log.Info("Voting for myself in {1}election for term {0}", currentTerm, _wonTrialElection ? " " : " trial ");
            Handle(new RequestVoteResponse
            {
                CurrentTerm = Engine.PersistentState.CurrentTerm,
                VoteGranted = true,
                Message = String.Format("{0} -> Voting for myself", Engine.Name),
                From = Engine.Name,
                ClusterTopologyId = Engine.CurrentTopology.TopologyId,
                VoteTerm = currentTerm,
                TrialOnly = _wonTrialElection == false
            });
        }

        public override RaftEngineState State
        {
            get { return RaftEngineState.Candidate; }
        }

        public override void Handle(RequestVoteResponse resp)
        {
            Engine.EngineStatistics.LastElectionInformation.Votes.Enqueue(resp);
            if (FromOurTopology(resp) == false)
            {
                _log.Info("Got a request vote response message outside my cluster topology (id: {0}), ignoring", resp.ClusterTopologyId);
                return;
            }
            
            long currentTerm = _wonTrialElection ? Engine.PersistentState.CurrentTerm : Engine.PersistentState.CurrentTerm + 1;
            if (resp.VoteTerm != currentTerm)
            {
                _log.Info("Got a vote for {2}election term {0} but current term is {1}, ignoring", resp.VoteTerm, currentTerm,
                    _wonTrialElection ? " " : "trial ");
                return;
            }
            if (resp.CurrentTerm > currentTerm)
            {
                _log.Info("CandidateStateBehavior -> UpdateCurrentTerm called, there is a new leader, moving to follower state");
                Engine.UpdateCurrentTerm(resp.CurrentTerm, null);
                return;
            }

            if (resp.VoteGranted == false)
            {
                if (resp.TermIncreaseMightGetMyVote)
                    _termIncreaseMightGetMyVote = true;
                _log.Info("Vote rejected from {0} trial: {1}", resp.From, resp.TrialOnly);
                return;
            }

            if (Engine.CurrentTopology.IsVoter(resp.From) == false) //precaution
            {
                _log.Info("Vote accepted from {0}, which isn't a voting node in our cluster", resp.From);
                return;
            }

            if (resp.TrialOnly && _wonTrialElection) // note that we can't get a vote for real election when we get a trail, because the terms would be different
            {
                _log.Info("Got a vote for trial only from {0} but we already won the trial election for this round, ignoring", resp.From);
                return;
            }

            _votesForMyLeadership.Add(resp.From);
            _log.Info("Adding to my votes: {0} (current votes: {1})", resp.From, string.Join(", ", _votesForMyLeadership));

            if (Engine.CurrentTopology.HasQuorum(_votesForMyLeadership) == false)
            {
                _log.Info("Not enough votes for leadership, votes = {0}", _votesForMyLeadership.Any() ? string.Join(", ", _votesForMyLeadership) : "empty");
                return;
            }

            if (_wonTrialElection == false)
            {
                _wonTrialElection = true;
                _log.Info("Won trial election with {0} votes from {1}, now running for real", _votesForMyLeadership.Count, string.Join(", ", _votesForMyLeadership));
                StartElection();
                return;
            }

            Engine.SetState(RaftEngineState.Leader);
            _log.Info("Selected as leader, term = {0}", resp.CurrentTerm);
        }

        public override void Dispose()
        {
            if (_log.IsDebugEnabled)
                _log.Debug("Disposing of CandidateStateBehavior");
            base.Dispose();
        }
    }
}
