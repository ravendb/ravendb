using System.Collections.Generic;
using Raven.Server.ReplicationUtil;

namespace Raven.Server.Documents.Replication
{
    public class DocumentReplicationTenantData
    {
        public string Id { get; set; }

        public Dictionary<string,long> TenantChangeVector { get; set; }
    }

    public class DocumentReplicationConfiguration
    {
        public string Id { get; set; }

        public bool Disabled { get; set; }

        public List<DocumentReplicationDestination> Destinations { get; private set; } = new List<DocumentReplicationDestination>();
    }

    public class DocumentReplicationDestination
    {
        public string Url { get; set; }

        public string ApiKey { get; set; }
    }	
}
