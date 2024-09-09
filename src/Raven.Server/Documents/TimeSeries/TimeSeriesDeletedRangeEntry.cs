using System;
using Sparrow.Json;

namespace Raven.Server.Documents.TimeSeries
{
    public class TimeSeriesDeletedRangeEntry : IDisposable
    {
        public LazyStringValue Key;

        public LazyStringValue LuceneKey;

        public LazyStringValue DocId;

        public LazyStringValue Name;

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
        }
    }
}
