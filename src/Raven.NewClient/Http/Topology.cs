using System.Collections.Generic;

namespace Raven.NewClient.Client.Http
{
    public class TopologyNode
    {
        public ServerNode Node;
        public List<TopologyNode> Outgoing;
    }

    public class Topology
    {
        public long Etag;
        public List<TopologyNode> Outgoing;
        public TopologyNode LeaderNode;
        public ReadBehavior ReadBehavior;
        public WriteBehavior WriteBehavior;
        public TopologySla SLA;
    }
}