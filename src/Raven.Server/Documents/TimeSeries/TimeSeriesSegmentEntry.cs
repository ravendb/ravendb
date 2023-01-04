using System;
using Sparrow.Json;

namespace Raven.Server.Documents.TimeSeries
{
    public class TimeSeriesSegmentEntry : IDisposable
    {
        public LazyStringValue Key;

        public LazyStringValue LuceneKey;

        public LazyStringValue DocId;

        public LazyStringValue Name;

        public string ChangeVector;

        public TimeSeriesValuesSegment Segment;

        public int SegmentSize;

        public LazyStringValue Collection;

        public DateTime Start;

        public long Etag;

        public void Dispose()
        {
            Key?.Dispose();
            LuceneKey?.Dispose();
            DocId?.Dispose();
            Name?.Dispose();
            Collection?.Dispose();
        }
    }
}
