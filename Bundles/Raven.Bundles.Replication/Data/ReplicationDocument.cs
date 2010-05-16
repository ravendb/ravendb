using System.Collections.Generic;

namespace Raven.Bundles.Replication.Data
{
    public class ReplicationDocument
    {
        public List<ReplicationDestination> Destinations { get; set; }

        public ReplicationDocument()
        {
            Destinations = new List<ReplicationDestination>();
        }
    }
}