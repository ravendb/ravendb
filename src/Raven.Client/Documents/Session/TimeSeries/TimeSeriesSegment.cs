namespace Raven.Client.Documents.Session.TimeSeries
{
    public class TimeSeriesSegment
    {
        public string DocumentId { get; set; }

        public string Name { get; set; }

        public TimeSeriesEntry[] Entries { get; set; }
    }
}
