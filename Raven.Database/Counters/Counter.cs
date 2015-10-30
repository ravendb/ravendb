using System.Collections.Generic;

namespace Raven.Database.Counters
{
    public class Counter
    {
        public long Etag;
        public List<PerServerValue> ServerValues = new List<PerServerValue>();

        public class PerServerValue
        {
            public int SourceId { get; set; }
            public long Positive { get; set; }
            public long Negative { get; set; }
        }
    }

    public class Group
    {
        public string Name { get; set; }
        public long NumOfCounters { get; set; }
    }
}
