using System.Threading;

namespace Raven.Client.Http
{
    public class LoadBalancingRoundRobinNodeSelector : FailoverNodeSelector
    {
        private int _syncFlag;

        public LoadBalancingRoundRobinNodeSelector(Topology topology) : base(topology)
        {
            _syncFlag = 0;
        }

        public override void OnSucceededRequest()
        {
            if (Interlocked.CompareExchange(ref _syncFlag, 1, 0) == 1)
                return;

            var topology = _topology;
            if (topology.Nodes.Count == 0)
                RequestExecutor.ThrowEmptyTopology();

            var current = _currentNodeIndex;
            var nextNodeIndex = _currentNodeIndex < topology.Nodes.Count - 1 ? current + 1 : 0;
            Interlocked.CompareExchange(ref _currentNodeIndex, nextNodeIndex, current);

            Interlocked.Exchange(ref _syncFlag, 0);
        }
    }
}