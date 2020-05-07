using System.Collections.Generic;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class TimeSeriesDetails
    {
        public string Id { get; set; }
        public Dictionary<string, List<TimeSeriesRangeResult>> Values { get; set; }
    }
}
