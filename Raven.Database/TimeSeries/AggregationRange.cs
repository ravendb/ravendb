using System;

namespace Raven.Database.TimeSeries
{
    public class AggregationRange
    {
        public string Type { get; set; }
        
        public string Key { get; set; }

        public AggregationRange(string type, string key, DateTimeOffset time)
        {
            Type = type;
            Key = key;
            Start = End = time;
        }

        public DateTimeOffset Start { get; set; }

        public DateTimeOffset End { get; set; }
    }
}
