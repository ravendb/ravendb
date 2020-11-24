using System;
using Raven.Client;
using Raven.Server.Documents.TimeSeries;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes
{
    public abstract class IndexItem : IDisposable
    {
        public readonly LazyStringValue Id;

        public readonly LazyStringValue LowerId;

        public readonly LazyStringValue SourceDocumentId;

        public readonly LazyStringValue LowerSourceDocumentId;

        public readonly long Etag;

        public DateTime LastModified;

        public readonly int Size;

        public readonly object Item;

        public readonly string IndexingKey;

        public readonly bool Empty;

        public readonly IndexItemType ItemType;

        public bool SkipLuceneDelete;

        protected IndexItem(LazyStringValue id, LazyStringValue lowerId, LazyStringValue sourceDocumentId, LazyStringValue lowerSourceDocumentId, long etag, DateTime lastModified, string indexingKey, int size, object item, bool empty, IndexItemType itemType)
        {
            Id = id;
            LowerId = lowerId;
            Etag = etag;
            LastModified = lastModified;
            Size = size;
            Item = item;
            IndexingKey = indexingKey;
            SourceDocumentId = sourceDocumentId;
            LowerSourceDocumentId = lowerSourceDocumentId;
            Empty = empty;
            ItemType = itemType;
        }

        protected abstract string ToStringInternal();

        public void Dispose()
        {
            if (Item is IDisposable disposable)
                disposable.Dispose();
        }

        public override string ToString()
        {
            return ToStringInternal();
        }
    }

    public class DocumentIndexItem : IndexItem
    {
        public DocumentIndexItem(LazyStringValue id, LazyStringValue lowerId, long etag, DateTime lastModified, int size, object item)
            : base(id, lowerId, null, null, etag, lastModified, null, size, item, empty: false, IndexItemType.Document)
        {
        }

        protected override string ToStringInternal()
        {
            return $"{Constants.Documents.Metadata.Id}: '{Id}', {Constants.Documents.Metadata.LastModified}: '{LastModified}'";
        }
    }

    public class TimeSeriesIndexItem : IndexItem
    {
        public TimeSeriesIndexItem(LazyStringValue id, LazyStringValue sourceDocumentId, long etag, DateTime lastModified, string timeSeriesName, int size, TimeSeriesSegmentEntry item)
            : base(id, id, sourceDocumentId, sourceDocumentId, etag, lastModified, timeSeriesName, size, item, empty: item.Segment.NumberOfLiveEntries == 0, IndexItemType.TimeSeries)
        {
        }

        protected override string ToStringInternal()
        {
            return $"@key: '{SourceDocumentId}|{IndexingKey}', {Constants.Documents.Metadata.LastModified}: '{LastModified}'";
        }
    }

    public class CounterIndexItem : IndexItem
    {
        public CounterIndexItem(LazyStringValue id, LazyStringValue sourceDocumentId, long etag, LazyStringValue counterName, int size, object item)
            : base(id, id, sourceDocumentId, sourceDocumentId, etag, default, counterName, size, item, empty: false, IndexItemType.Counters)
        {
        }

        protected override string ToStringInternal()
        {
            return $"@key: '{SourceDocumentId}|{IndexingKey}'";
        }
    }
}
