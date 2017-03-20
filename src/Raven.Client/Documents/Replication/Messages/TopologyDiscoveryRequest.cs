using System.Collections.Generic;

namespace Raven.Client.Documents.Replication.Messages
{
    internal class TopologyDiscoveryRequest
    {
        //already visited db ids
        public List<string> AlreadyVisited;

        // this is the requesting db, not the first db that this was requested from
        public string OriginDbId; 

        public long Timeout;
    }
}
