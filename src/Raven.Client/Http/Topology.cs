using System.Collections.Generic;

namespace Raven.Client.Http
{
    public class Topology
    {
        public long Etag;
        public List<ServerNode> Nodes;
    }
}
