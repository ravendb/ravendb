using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.NewClient.Client.Http;

namespace Raven.NewClient.Replication.Messages
{
    public class TopologyDiscoveryResponse
    {
        public TopologyNode DiscoveredTopology;

        //discovery results by DbId
        public ResponseType DiscoveryResponseType;

        public string FromDbId;

        public enum ResponseType
        {
            Ok,
            Error,
            Timeout
        }
    }

    public class TopologyDiscoveryHeader
    {
        public List<string> AlreadyVisitedDbIds;
    }
}
