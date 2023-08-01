using System;
using System.Runtime.CompilerServices;
using Sparrow.Json;

namespace Raven.Server.Documents.TimeSeries
{
    public sealed class TimeSeriesSegmentEntry : IDisposable
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
            if (Key != null && Key.IsDisposed == false)
                Key.Dispose();

            if (LuceneKey != null && LuceneKey.IsDisposed == false)
                LuceneKey.Dispose();

            if (DocId != null && DocId.IsDisposed == false)
                DocId.Dispose();

            if (Name != null && Name.IsDisposed == false)
                Name.Dispose();

            if (Collection != null && Collection.IsDisposed == false)
                Collection.Dispose();
        }
    }

    [Flags]
    public enum TimeSeriesSegmentEntryFields
    {
        Default = 0,
        Key = 1 << 0,
        DocIdNameAndStart = 1 << 1,
        LuceneKey = 1 << 2,
        ChangeVector = 1 << 3,
        Segment = 1 << 4,
        Collection = 1 << 5,

        ForIndexing = Key | DocIdNameAndStart | LuceneKey | Segment,
        ForEtl = Key | DocIdNameAndStart | ChangeVector | Segment,
        ForSmuggler = Key | DocIdNameAndStart | ChangeVector | Segment | Collection,
        All = Key | DocIdNameAndStart | LuceneKey | ChangeVector | Segment | Collection,
    }

    public static class EnumExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Contain(this TimeSeriesSegmentEntryFields current, TimeSeriesSegmentEntryFields flag)
        {
            return (current & flag) == flag;
        }
    }
}
