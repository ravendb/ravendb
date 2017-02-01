using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Rachis.Behaviors;
using Rachis.Commands;
using Rachis.Communication;
using Rachis.Interfaces;
using Rachis.Messages;
using Rachis.Storage;
using Sparrow.Collections.LockFree;

namespace Rachis
{
    public class RaftEngine
    {
        public RaftEngine(RaftEngineOptions options,Topology bootstrap = null)
        {
            Options = options;
            Name = options.Name;
            CancellationTokenSource = new CancellationTokenSource();
            PersistentState = new PersistentState(Name,options.ElectionTimeout,options.HeartbeatTimeout);
            //CurrentTopology = bootstrap??PersistentState.GetCurrentTopology();
            PersistentState.SetCurrentTopology(bootstrap);
            var thereAreOthersInTheCluster = CurrentTopology.QuorumSize > 1;
            //if (thereAreOthersInTheCluster == false && CurrentTopology.IsVoter(Name))
            if(bootstrap != null)
            {
                PersistentState.UpdateTermTo(PersistentState.CurrentTerm + 1);// restart means new term
                SetState(RaftEngineState.Leader);
            }
            else
            {
                SetState(RaftEngineState.Follower);
            }
        }

        /// <summary>
        /// This method is intended for a leader that has to re-establish communication to its topology but is already a confirmed leader.
        /// </summary>
        internal void ReestablsihCommunicationWithTopologyAsLeader()
        {
            StateBehavior?.Dispose();
            var leaderState = new LeaderStateBehavior(this, false);
            leaderState.StartCommunicationWithPeers();
            StateBehavior = leaderState;
        }

        //TODO:Break this method into multiple methods 
        internal void SetState(RaftEngineState state,Stream stream = null)
        {
            var oldState = State;
            if (oldState == state)
                return;
            StateBehavior?.Dispose();
            switch (state)
            {
                case RaftEngineState.Follower:
                    StateBehavior = new FollowerStateBehavior(this); 
                    break;
                case RaftEngineState.FollowerAfterSteppingDown:
                    StateBehavior = new FollowerStateBehavior(this, true); 
                    break;
                case RaftEngineState.Leader:
                    var leaderState = new LeaderStateBehavior(this);
                    leaderState.StartCommunicationWithPeers();
                    StateBehavior = leaderState;
                    break;
                case RaftEngineState.Candidate:
                    StateBehavior = new CandidateStateBehavior(this);
                    StateBehavior.HandleTimeout();
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(state), state, null);
            }
        }

        public RaftEngineOptions Options { get; }
        public IRaftStateMachine StateMachine => Options.StateMachine;
        public Topology CurrentTopology => PersistentState.GetCurrentTopology();

        public string Name { get; set; }
        public PersistentState PersistentState;
        private string _currentLeader;
        public CancellationTokenSource CancellationTokenSource { get; }

        public RaftEngineState State
        {
            get
            {
                var behavior = StateBehavior;
                if (behavior == null)
                    return RaftEngineState.None;
                return behavior.State;
            }
        }

        internal AbstractRaftStateBehavior StateBehavior { get; set; }

        public string CurrentLeader
        {
            get { return _currentLeader; }
            set { _currentLeader = value; } //TODO: need to add leader select event here
        }

        public long CommitIndex => StateMachine.LastAppliedIndex;

        public void StartTopologyChange(TopologyChangeCommand topologyCommand)
        {
            
        }

        public void CommitEntries(long nextCommitIndex)
        {
            //TODO: this should be a background task
            foreach (var entry in PersistentState.LogEntriesAfter(CommitIndex, nextCommitIndex))
            {
                StateMachine.Apply(entry);                
            }
        }

        public void HandleNewConnection(Stream stream)
        {
            var messageHandler = new MessageHandler(stream);
            var header = messageHandler.ReadHeader();
            switch (header.Type)
            {
                case MessageType.AppendEntries:
                    var append = messageHandler.ReadMessageBody<AppendEntries>(header);
                    // todo: validate if we can accept it at all
                    var follower = (StateBehavior as FollowerStateBehavior);
                    if (follower == null)
                        return;
                    follower.Start(append, stream);
                    break;
                case MessageType.RequestVote:
                    var vote = messageHandler.ReadMessageBody<RequestVote>(header);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public void CommitEntries(long prevCommitIndex, long nextCommitIndex)
        {
            //TODO: this should be a background task
            if (prevCommitIndex == nextCommitIndex)
                return;
            if (nextCommitIndex < prevCommitIndex)
                //Something is very wrong
                return;
            foreach (var entry in PersistentState.LogEntriesAfter(prevCommitIndex).Take((int)nextCommitIndex- (int)prevCommitIndex))
            {
                StateMachine.Apply(entry);
                if (entry.IsTopologyChange != null && entry.IsTopologyChange.Value)
                {
                    var topologychange = Command.FromBytes<TopologyChangeCommand>(entry.Data);
                    CommitTopologyChange(topologychange);
                }
            }
        }

        private void CommitTopologyChange(TopologyChangeCommand topologychange)
        {
            //TODO: handle removed from topology and such
        }

        public void AddToCluster(NodeConnectionInfo node)
        {
            if (CurrentTopology.Contains(node.Name))
                throw new InvalidOperationException("Node " + node.Name + " is already in the cluster");

            var requestedTopology = new Topology(
                CurrentTopology.TopologyId,
                CurrentTopology.AllVotingNodes,
                node.IsNoneVoter ? CurrentTopology.NonVotingNodes.Union(new[] { node }) : CurrentTopology.NonVotingNodes,
                node.IsNoneVoter ? CurrentTopology.PromotableNodes : CurrentTopology.PromotableNodes.Union(new[] { node })
                );

            ModifyTopology(requestedTopology);
        }

        internal void ModifyTopology(Topology requested)
        {
            if (State != RaftEngineState.Leader)
                throw new NotLeadingException("Cannot modify topology from a non leader node, current leader is: " +
                                                    (CurrentLeader ?? "no leader"));

            var logEntry = PersistentState.GetLogEntry(CommitIndex);
            if (logEntry == null)
                throw new InvalidOperationException("No log entry for committed for index " + CommitIndex + ", this is probably a brand new cluster with no committed entries or a serious problem");

            if (logEntry.Term != PersistentState.CurrentTerm)
                throw new InvalidOperationException("Cannot modify the cluster topology when the committed index " + CommitIndex + " is in term " + logEntry.Term + " but the current term is " +
                                                    PersistentState.CurrentTerm + ". Wait until the leader finishes committing entries from the current term and try again");

            var tcc = new TopologyChangeCommand
            {
                Requested = requested,
                Previous = CurrentTopology,
            };

            StartTopologyChange(tcc);
            AppendCommand(tcc);
            ReestablsihCommunicationWithTopologyAsLeader();
        }

        public void AppendCommand(Command command)
        {
            if (command == null) throw new ArgumentNullException("command");

            var leaderStateBehavior = StateBehavior as LeaderStateBehavior;
            if (leaderStateBehavior == null || leaderStateBehavior.State != RaftEngineState.Leader)
                throw new NotLeadingException("Command can be appended only on leader node. This node behavior type is " +
                                                    StateBehavior.GetType().Name)
                {
                    CurrentLeader = CurrentLeader
                };


            leaderStateBehavior.AppendCommand(command);
        }

        public bool CurrentlyChangingTopology()
        {
            return false;
        }

    }
}
