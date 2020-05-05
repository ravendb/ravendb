using System.Collections.Generic;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class TimeSeriesDetails
    {
        public string Id;
        public Dictionary<string, List<TimeSeriesRangeResult>> Values;
    }
}
