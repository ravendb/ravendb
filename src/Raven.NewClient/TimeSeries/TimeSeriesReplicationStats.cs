using System.Collections.Generic;

namespace Raven.Abstractions.TimeSeries
{
    public class TimeSeriesReplicationStats
    {
        public List<TimeSeriesDestinationStats> Stats { get; set; }
    }
}
