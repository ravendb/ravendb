using Raven.Client.Documents.Session.TimeSeries;

namespace Raven.Client.Documents.Indexes.TimeSeries
{
    public class TimeSeriesSegment
    {
        public string DocumentId { get; set; }

        public string Name { get; set; }

        public double[] Min { get; set; }

        public double[] Max { get; set; }

        public double[] Sum { get; set; }

        public int Count { get; set; }

        public TimeSeriesEntry[] Entries { get; set; }
    }
}
