using System;

namespace Raven.NewClient.Database.Counters
{
    public class CounterState
    {
        public string GroupName { get; set; }

        public string CounterName { get; set; }

        public Guid ServerId { get; set; }

        public char Sign { get; set; }

        public long Value { get; set; }

        public long Etag { get; set; }
    }
}
