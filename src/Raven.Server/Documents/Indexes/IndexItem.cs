﻿using System;
using Raven.Server.Documents.Indexes.Static.Counters;
using Sparrow.Json;
using Sparrow.Server.Utils;
using Voron;

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

        public readonly IndexItemType ItemType;

        protected IndexItem(LazyStringValue id, LazyStringValue lowerId, LazyStringValue sourceDocumentId, LazyStringValue lowerSourceDocumentId, long etag, DateTime lastModified, string indexingKey, int size, object item, IndexItemType itemType)
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
            ItemType = itemType;
        }

        public void Dispose()
        {
            if (Item is IDisposable disposable)
                disposable.Dispose();
        }
    }

    public class DocumentIndexItem : IndexItem
    {
        public DocumentIndexItem(LazyStringValue id, LazyStringValue lowerId, long etag, DateTime lastModified, int size, object item)
            : base(id, lowerId, null, null, etag, lastModified, null, size, item, IndexItemType.Document)
        {
        }
    }

    public class TimeSeriesIndexItem : IndexItem
    {
        public TimeSeriesIndexItem(LazyStringValue id, LazyStringValue sourceDocumentId, long etag, DateTime lastModified, string timeSeriesName, int size, object item)
            : base(id, id, sourceDocumentId, sourceDocumentId, etag, lastModified, timeSeriesName, size, item, IndexItemType.TimeSeries)
        {
        }
    }

    public class CounterIndexItem : IndexItem
    {
        public CounterIndexItem(LazyStringValue id, LazyStringValue sourceDocumentId, long etag, LazyStringValue counterName, int size, object item)
            : base(id, id, sourceDocumentId, sourceDocumentId, etag, default, counterName, size, item, IndexItemType.Counters)
        {
        }
    }
}
