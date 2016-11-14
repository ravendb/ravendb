using System.Collections.Generic;

namespace Raven.NewClient.Abstractions.TimeSeries
{
    public class TimeSeriesReplicationStats
    {
        public List<TimeSeriesDestinationStats> Stats { get; set; }
    }
}
