using System;
using System.Threading;

namespace Raven.Client.Http
{
    public class NodeSelector
    {
        protected Topology _topology;

        public Topology Topology => _topology;

        protected int _currentNodeIndex;

        public NodeSelector(Topology topology)
        {
            _topology = topology;
        }

        public int GetCurrentNodeIndex()
        {
            return Volatile.Read(ref _currentNodeIndex);
        }

        public void OnFailedRequest(int nodeIndex, int? sessionId) => AdvanceToNextNode(sessionId);

        public bool OnUpdateTopology(Topology topology, bool forceUpdate = false)
        {
            if (topology == null)
                return false;

            var oldTopology = _topology;
            do
            {
                if (oldTopology.Etag >= topology.Etag && forceUpdate == false)
                    return false;

                if (forceUpdate == false)
                {
                    Interlocked.Exchange(ref _currentNodeIndex, 0);
                }

                var changed = Interlocked.CompareExchange(ref _topology, topology, oldTopology);
                if (changed == oldTopology)
                    return true;
                oldTopology = changed;
            } while (true);
        }

        public (int, ServerNode) GetCurrentNode()
        {
            if (Topology.Nodes.Count == 0)
                ThrowEmptyTopology();

            return (Volatile.Read(ref _currentNodeIndex),Topology.Nodes[_currentNodeIndex]);
        }

        public (int,ServerNode) GetNodeBySessionId(int sessionId)
        {
            var index = sessionId % Topology.Nodes.Count;
            return (index,Topology.Nodes[index]);
        }

        public void RestoreNodeIndex(int nodeIndex)
        {
            var currentNodeIndex = _currentNodeIndex;
            while (currentNodeIndex > nodeIndex)
            {
                var result = Interlocked.CompareExchange(ref _currentNodeIndex, nodeIndex, currentNodeIndex);
                if (result == currentNodeIndex)
                    return;
                currentNodeIndex = result;
            }
        }

        public event Action<int> NodeSwitch;

        private readonly object _nodeIncrementSyncObj = new object();
        public void AdvanceToNextNode(int? sessionId)
        {
            lock (_nodeIncrementSyncObj)
            {
                if (Topology.Nodes.Count == 0)
                    ThrowEmptyTopology();

                int nextIndex;
                var sessionIndex = sessionId ?? 0 % Topology.Nodes.Count;
                do
                {
                    nextIndex = _currentNodeIndex + 1;
                    _currentNodeIndex = nextIndex < Topology.Nodes.Count ? nextIndex : 0;
                } while (nextIndex == sessionIndex);

                NodeSwitch?.Invoke(_currentNodeIndex);
            }
        }

        protected static void ThrowEmptyTopology()
        {
            throw new InvalidOperationException("Empty database topology, this shouldn't happen.");
        }

    }
}