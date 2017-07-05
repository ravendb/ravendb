using System.Threading;

namespace Raven.Client.Http
{
    public class FailoverNodeSelector : INodeSelector
    {
        protected Topology _topology;

        public Topology Topology => _topology;

        protected int _currentNodeIndex;

        public FailoverNodeSelector(Topology topology)
        {
            _topology = topology;
        }

        public int GetCurrentNodeIndex()
        {
            return _currentNodeIndex;
        }

        public virtual void OnSucceededRequest()
        {
            //in failover mode nothing to do
        }

        public void OnFailedRequest(int nodeIndex)
        {
            if (Topology.Nodes.Count == 0)
                RequestExecutor.ThrowEmptyTopology();

            var nextNodeIndex = nodeIndex < Topology.Nodes.Count - 1 ? nodeIndex + 1 : 0;
            Interlocked.CompareExchange(ref _currentNodeIndex, nextNodeIndex, nodeIndex);
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
                RequestExecutor.ThrowEmptyTopology();
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
    }
}