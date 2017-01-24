using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Rachis.Behaviors;
using Rachis.Commands;
using Rachis.Communication;
using Rachis.Interfaces;
using Rachis.Storage;
using Sparrow.Collections.LockFree;

namespace Rachis
{
    public class RaftEngine
    {
        public RaftEngine(RaftEngineOptions options)
        {
            Options = options;
            Name = options.Name;
            CancellationTokenSource = new CancellationTokenSource();
            PersistentState = new PersistentState(Name,options.ElectionTimeout);
            CurrentTopology =    PersistentState.GetCurrentTopology();
            var thereAreOthersInTheCluster = CurrentTopology.QuorumSize > 1;
            if (thereAreOthersInTheCluster == false && CurrentTopology.IsVoter(Name))
            {
                PersistentState.UpdateTermTo(PersistentState.CurrentTerm + 1);// restart means new term
                SetState(RaftEngineState.Leader);
            }
            else
            {
                SetState(RaftEngineState.Follower);
            }
        }

        public void HandleNewConnection(ITransportBus connection)
        {
            //StateBehavior.HandleNewConnection(connection);            
            var nodeId = connection.GetNodeId();
            // We don't allow two connections from the same source at the same time, here I'm assuming
            // that if a node is connecting to us again then the old connection should be closed.
            _incomingCommunicationThreads.AddOrUpdate(nodeId, sourceId =>
            {
                var thread = new IncomingCommunicationThread(nodeId, Name, connection, this); //CommunicationDone
                thread.CommunicationDone += OnCommunicationEnded;
                return thread;
            }, (sourceId, oldThread) =>
            {
                oldThread.Dispose();
                if (oldThread.Joined == false)
                    return oldThread; //we didn't join the old thread will have to try again next time...
                var thread = new IncomingCommunicationThread(nodeId, Name, connection, this); //CommunicationDone
                thread.CommunicationDone += OnCommunicationEnded;
                return thread;
            });
        }

        internal void SetState(RaftEngineState state)
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
                    StateBehavior = new FollowerStateBehavior(this,true); 
                    break;
                case RaftEngineState.Leader:
                    StateBehavior = new LeaderStateBehavior(this);
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
        public ITransportHub Transport => Options.TransportHub;
        public Topology CurrentTopology { get; }

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
            throw new NotImplementedException();
        }

        public Task CommitEntries(LogEntry[] entries, long nextCommitIndex)
        {
            throw new NotImplementedException();
        }

        private void OnCommunicationEnded(IncomingCommunicationThread incomingCommunicationThread, Exception exception)
        {
            //TODO: log exception
            _incomingCommunicationThreads.Remove(incomingCommunicationThread.SourceId);
        }
        private readonly ConcurrentDictionary<string, IncomingCommunicationThread> _incomingCommunicationThreads = new ConcurrentDictionary<string, IncomingCommunicationThread>();
    }
}
