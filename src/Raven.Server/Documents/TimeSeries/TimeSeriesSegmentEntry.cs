using System;
using Sparrow.Json;

namespace Raven.Server.Documents.TimeSeries
{
    public class TimeSeriesSegmentEntry
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
    }
}
