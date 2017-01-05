using System.Collections.Generic;

namespace Raven.Client.Replication.Messages
{
    public class TopologyDiscoveryRequest
    {
        //already visited in the format of [src DbId -> list of dest DbIds]
        public Dictionary<string, List<string>> AlreadyVisited;

        public string OriginDbId;

        public long Timeout;
    }
}
