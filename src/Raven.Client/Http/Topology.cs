using System.Collections.Generic;

namespace Raven.Client.Http
{
    public class Topology
    {
        public long Etag;
        public List<ServerNode> Nodes;

        public override string ToString()
        {
            return $"{{{nameof(Nodes)}: [{string.Join(",", Nodes ?? new List<ServerNode>())}], {nameof(Etag)}: {Etag}}}";
        }
    }
}
