using System;
using Sparrow.Json;

namespace Raven.Server.Documents.TimeSeries
{
    public class TimeSeriesSegmentEntry
    {
        public LazyStringValue Key;

        public LazyStringValue DocId;

        public string Name;

        public LazyStringValue DocIdAndName;

        public string ChangeVector;

        public TimeSeriesValuesSegment Segment;

        public int SegmentSize;

        public string Collection;

        public DateTime Baseline;

        public long Etag;
    }
}
