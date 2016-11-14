using System.Collections.Generic;

namespace Raven.NewClient.Client.Http
{
    public class Topology
    {
        public long Etag;
        public List<ServerNode> Nodes;
        public ServerNode LeaderNode;
        public ReadBehavior ReadBehavior;
        public WriteBehavior WriteBehavior;
        public TopologySla SLA;
    }
}