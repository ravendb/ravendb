using System.Collections.Generic;

namespace Raven.Client.Http
{
    public sealed class Topology
    {
        public long Etag;
        public List<ServerNode> Nodes;
        public List<ServerNode> Promotables = new List<ServerNode>();
    }
}
