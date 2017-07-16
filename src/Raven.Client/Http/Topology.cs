using System.Collections.Generic;
using System.Linq;

namespace Raven.Client.Http
{
    public class Topology
    {
        public long Etag;
        public List<ServerNode> Nodes;

        public Topology Clone()
        {
            return new Topology
            {
                Etag = this.Etag,
                Nodes = Nodes.ToList()
            };
        }
    }
}