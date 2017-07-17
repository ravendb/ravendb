namespace Raven.Client.Http
{
    public class RoundRobinNodeSelector : FailoverNodeSelector
    {
        public RoundRobinNodeSelector(Topology topology) : base(topology)
        {
        }

        public override INodeSelector HandleRequestWithoutSessionId() => AdvanceToNextNodeAndFetchInstance();

        public override INodeSelector CloneForNewSession()
        {
            //prevent race condition where _topology has already changed but _currentNodeIndex has not yet changed
            lock (_cloneForNewSessionSync)
            {
                var nodeSelector = new RoundRobinNodeSelector(_topology.Clone())
                {
                    _currentNodeIndex = GetCurrentNodeIndex()
                };

                return nodeSelector.AdvanceToNextNodeAndFetchInstance();
            }
        }
    }
}
