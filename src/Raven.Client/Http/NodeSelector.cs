using System;
using System.Threading;

namespace Raven.Client.Http
{
    public class NodeSelector
    {
        private class NodeSelectorState
        {
            public int CurrentNodeIndex;
            public Topology Topology;

            public NodeSelectorState Clone()
            {
                return new NodeSelectorState
                {
                    CurrentNodeIndex = this.CurrentNodeIndex,
                    Topology = this.Topology
                };
            }
        }        

        private NodeSelectorState _state;

        public Topology Topology => _state.Topology;

        public NodeSelector(Topology topology)
        {
            _state = new NodeSelectorState
            {
                Topology = topology
            };
        }

        public int GetCurrentNodeIndex() => _state.CurrentNodeIndex;

        public void OnFailedRequest(int nodeIndex, int? sessionId) => AdvanceToNextNode(sessionId);

        public bool OnUpdateTopology(Topology topology, bool forceUpdate = false)
        {
            if (topology == null)
                return false;

            if (_state.Topology.Etag >= topology.Etag && forceUpdate == false)
                return false;

            var state = new NodeSelectorState
            {
                Topology = topology
            };

            Interlocked.Exchange(ref _state, state);

            return true;
        }

        public (int currentIndex, ServerNode currentNode) GetCurrentNode()
        {
            var state = _state;
            return (currentIndex: state.CurrentNodeIndex,
                    currentNode: state.Topology.Nodes[state.CurrentNodeIndex]);
        }

        public (int currentIndex, ServerNode currentNode) GetNodeBySessionId(int sessionId)
        {
            var state = _state;
            var index = sessionId % state.Topology.Nodes.Count;
            return (currentIndex: index,
                    currentNode: state.Topology.Nodes[index]);
        }

        public void RestoreNodeIndex(int nodeIndex) => _state.CurrentNodeIndex = nodeIndex;

        public event Action<int> NodeSwitch;

        public void AdvanceToNextNode(int? sessionId)
        {
            var state = _state;
            if (state.Topology.Nodes.Count == 0)
                ThrowEmptyTopology();

            int nextIndex;
            var sessionIndex = (sessionId ?? 0) % state.Topology.Nodes.Count;
            do
            {
                nextIndex = state.CurrentNodeIndex + 1;
                state.CurrentNodeIndex = nextIndex < Topology.Nodes.Count ? nextIndex : 0;
            } while (nextIndex == sessionIndex);

            NodeSwitch?.Invoke(state.CurrentNodeIndex);

            Interlocked.Exchange(ref _state, state);
        }

        protected static void ThrowEmptyTopology()
        {
            throw new InvalidOperationException("Empty database topology, this shouldn't happen.");
        }

    }
}