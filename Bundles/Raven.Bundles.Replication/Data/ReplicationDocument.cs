using System.Collections.Generic;

namespace Raven.Bundles.Replication.Data
{
    public class ReplicationDocument
    {
        public List<ReplicationDestination> Destinations { get; set; }

        public string Id { get; set; }

        public ReplicationDocument()
        {
            Id = "Raven/Replication/Destinations";
            Destinations = new List<ReplicationDestination>();
        }
    }
}