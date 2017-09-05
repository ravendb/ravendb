using System.Collections.Generic;

namespace Raven.Server.ServerWide.Maintenance
{
    public class ClusterObserverDecisions
    {
        public string LeaderNode { get; set;}
        public long Term { get; set; }
        public bool Suspended { get; set; }
        public long Iteration { get; set; }
        public List<ClusterObserverLogEntry> ObserverLog { get; set; }

    }
}
