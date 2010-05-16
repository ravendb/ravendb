using System.Collections.Generic;

namespace Raven.Bundles.Replication
{
    public class ReplicationDocument
    {
        public string Id { get; set; }

        public List<ReplicationDestination> Destinations { get; set; }

        public ReplicationDocument()
        {
            Destinations = new List<ReplicationDestination>();
            Id = ReplicationConstants.RavenReplicationDestinations;
        }
    }

    public class ReplicationDestination
    {
        public string Url { get; set; }
    }
}