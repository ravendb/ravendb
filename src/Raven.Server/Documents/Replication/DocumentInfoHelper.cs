using System;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Sparrow.Json;
using Sparrow.Server.Utils;
using Voron;

namespace Raven.Server.Documents.Replication
{
    public sealed class DocumentInfoHelper : IDisposable
    {
        public DocumentInfoHelper(JsonOperationContext context = null)
        {
            _contextOwner = context == null;
            _context = context ?? JsonOperationContext.ShortTermSingleUse();
        }
        private LazyStringValue _tmpLazyStringInstance;
        private readonly JsonOperationContext _context;
        private readonly bool _contextOwner;
        public unsafe LazyStringValue GetShortTimeDocumentId(Slice key)
        {
            var sepIdx = key.Content.IndexOf(SpecialChars.RecordSeparator);
            return GetShortTimeDocumentId(key.Content.Ptr, sepIdx);
        }

        public unsafe LazyStringValue GetShortTimeDocumentId(LazyStringValue key)
        {
            var index = key.IndexOf((char)SpecialChars.RecordSeparator, StringComparison.OrdinalIgnoreCase);
            if (index == -1)
                return null;

            return GetShortTimeDocumentId(key.Buffer, index);
        }
        
        private unsafe LazyStringValue GetShortTimeDocumentId(byte* ptr, int sepIdx)
        {
            if (_tmpLazyStringInstance == null)
            {
                _tmpLazyStringInstance = new LazyStringValue(null, ptr, sepIdx, _context, LazyStringType.SimpleString);
                return _tmpLazyStringInstance;
            }
            //TODO No escape positions and used for documentID which we are going to disallow control characters.
            _tmpLazyStringInstance.Renew(null, ptr, sepIdx, _context, LazyStringType.SimpleString);
            return _tmpLazyStringInstance;
        }

        // TODO unify if possible with AllowedPathsValidator
        // TODO We anyway convert it to string right away and the LazyStringValue is reused by the helper so better to convert it here to prevent wrong usage
        public string GetShortTermDocumentId(ReplicationBatchItem item)
        {
            return item switch
            {
                AttachmentReplicationItem a => GetShortTimeDocumentId(a.Key),
                AttachmentTombstoneReplicationItem at => GetShortTimeDocumentId(at.Key),
                CounterReplicationItem c => c.Id,
                DocumentReplicationItem d => d.Id,
                RevisionTombstoneReplicationItem r => GetShortTimeDocumentId(r.Id),
                TimeSeriesDeletedRangeItem td => GetShortTimeDocumentId(td.Key),
                TimeSeriesReplicationItem t => GetShortTimeDocumentId(t.Key),
                _ => throw new ArgumentOutOfRangeException($"{nameof(item)} - {item}")
            };
        }

        public unsafe string GetItemInformation(ReplicationBatchItem item)
        {
            switch (item)
            {
                case AttachmentReplicationItem a:
                    return $"Attachment '{a.Name}' for {GetShortTimeDocumentId(a.Key)}";
                case AttachmentTombstoneReplicationItem at:
                    var result = AttachmentsStorage.AttachmentKey.ExtractDocIdAndAttachmentName(at.Key);
                    return $"Attachment tombstone '{result.AttachmentName}' for {result.DocId}";
                case CounterReplicationItem c:
                    return $"Counter for {c.Id}";
                case DocumentReplicationItem d:
                    if (d.Flags.Contain(DocumentFlags.Revision))
                        return $"Revision for {d.Id}";

                    return d.Data != null ? "Document " + d.Id : "Tombstone " + d.Id;
                case RevisionTombstoneReplicationItem r:
                    return "Revision for " + r.Id;
                case TimeSeriesDeletedRangeItem td:
                    return "Time Series deletion range for: " + GetShortTimeDocumentId(td.Key);
                case TimeSeriesReplicationItem t:
                    var baseline = TimeSeriesStorage.GetBaseline(t.Key.Content.Ptr, t.Key.Content.Length);
                    return $"Time Series segment of '{t.Name}' [{baseline:s} - {t.Segment.GetLastTimestamp(baseline):s}] for {GetShortTimeDocumentId(t.Key)}";
                default:
                    throw new ArgumentOutOfRangeException($"{nameof(item)} - {item}");
            }
        }
        public void Dispose()
        {
            if (_contextOwner)
                _context.Dispose();
        }
    }
}
