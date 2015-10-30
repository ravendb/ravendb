using System.Collections.Generic;

namespace Raven.Database.Counters
{
    public class ReplicationMessage
    {
        public string SendingServerName { get; set; }
        public List<ReplicationCounter> Counters { get; set; }

        public ReplicationMessage()
        {
            Counters = new List<ReplicationCounter>();
        }
    }

    public class ReplicationCounter
    {
        public string CounterName { get; set; }
        public long Etag;
        public List<PerServerValue> ServerValues { get; set; }

        public ReplicationCounter()
        {
            ServerValues = new List<PerServerValue>();
        }

        public class PerServerValue
        {
            public string ServerName { get; set; }
            public long Positive { get; set; }
            public long Negative { get; set; }
        }
    }
}
