namespace Raven.Client.Http
{
    public class ClusterTopologyResponse
    {
        public string Leader;
        public string NodeTag;
        public ClusterTopology Topology;
    }
}
