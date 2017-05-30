using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Client.Http
{
    public class ClusterTopologyResponse
    {
        public string Leader;
        public string NodeTag;
        public ClusterTopology Topology;
    }
}
