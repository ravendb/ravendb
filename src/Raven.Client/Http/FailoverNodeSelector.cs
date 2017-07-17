using System;
using System.Threading;

namespace Raven.Client.Http
{
    public class FailoverNodeSelector : INodeSelector
    {
        protected Topology _topology;

        public Topology Topology => _topology;

        private int _currentNodeIndex;

        public FailoverNodeSelector(Topology topology)
        {
            _topology = topology;
        }

        public int GetCurrentNodeIndex()
        {
            return Volatile.Read(ref _currentNodeIndex);
        }

        public virtual void OnSucceededRequest()
        {
            //nothing to do here in this type of selector
        }

        public  void OnFailedRequest(int nodeIndex)
        {
            if (Topology.Nodes.Count == 0)
                ThrowEmptyTopology();

            AtomicAdvanceNodeIndex();
        }

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

                var changed = Interlocked.CompareExchange<Topology>(ref _topology, topology, oldTopology);
                if (changed == oldTopology)
                    return true;
                oldTopology = changed;
            } while (true);
        }

        public ServerNode GetCurrentNode()
        {
            if (Topology.Nodes.Count == 0)
                ThrowEmptyTopology();
            return Topology.Nodes[_currentNodeIndex];
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
        protected void AtomicAdvanceNodeIndex()
        {
            lock (_nodeIncrementSyncObj)
            {
                var nextIndex = _currentNodeIndex + 1;
                _currentNodeIndex = nextIndex < Topology.Nodes.Count ? nextIndex : 0;

                NodeSwitch?.Invoke(_currentNodeIndex);
            }
        }

        protected static void ThrowEmptyTopology()
        {
            throw new InvalidOperationException("Empty database topology, this shouldn't happen.");
        }

    }
}