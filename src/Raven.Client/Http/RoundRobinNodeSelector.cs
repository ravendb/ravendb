namespace Raven.Client.Http
{
    public class RoundRobinNodeSelector : FailoverNodeSelector
    {
        public RoundRobinNodeSelector(Topology topology) : base(topology)
        {
        }

        public override void OnSucceededRequest()
        {
            var topology = _topology;
            if (topology.Nodes.Count == 0)
                ThrowEmptyTopology();

            AtomicAdvanceNodeIndex();
        }
    }
}
