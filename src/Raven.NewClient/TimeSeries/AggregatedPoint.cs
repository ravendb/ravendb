using System;

namespace Raven.NewClient.Abstractions.TimeSeries
{
    public class AggregatedPoint
    {
#if DEBUG
        public string DebugKey { get; set; }
#endif

        public DateTimeOffset StartAt { get; set; }
        
        public AggregationDuration Duration { get; set; }

        public AggregationValue[] Values { get; set; }

        public class AggregationValue
        {
            // Position 1
            public double Volume { get; set; }

            // Position 2
            public double High { get; set; }

            // Position 3
            public double Low { get; set; }

            // Position 4
            public double Open { get; set; }

            // Position 5
            public double Close { get; set; }

            // Position 6
            public double Sum { get; set; }
        }
    }
}
