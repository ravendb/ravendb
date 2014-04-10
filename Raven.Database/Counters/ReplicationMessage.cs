using System.Collections.Generic;

namespace Raven.Database.Counters
{
    public class ReplicationMessage
    {
        public string SendingServerName { get; set; }
        public List<ReplictionCounter> Counters { get; set; }
    }

    public class ReplictionCounter
    {
        public string CounterName { get; set; }
        public Counter Counter { get; set; }
    }
}