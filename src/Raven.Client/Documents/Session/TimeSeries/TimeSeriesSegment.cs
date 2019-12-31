namespace Raven.Client.Documents.Session.TimeSeries
{
    public class TimeSeriesSegment
    {
        public string DocumentId { get; set; }

        public TimeSeriesValue[] Entries { get; set; }
    }
}
