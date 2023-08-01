using System;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Sparrow.Binary;
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
        public unsafe LazyStringValue GetDocumentId(Slice key)
        {
            var sepIdx = key.Content.IndexOf(SpecialChars.RecordSeparator);
            if (_tmpLazyStringInstance == null)
                _tmpLazyStringInstance = new LazyStringValue(null, null, 0, _context);
            _tmpLazyStringInstance.Renew(null, key.Content.Ptr, sepIdx, _context);
            return _tmpLazyStringInstance;
        }

        public unsafe LazyStringValue GetDocumentId(LazyStringValue key)
        {
            var index = key.IndexOf((char)SpecialChars.RecordSeparator, StringComparison.OrdinalIgnoreCase);
            if (index == -1)
                return null;

            return _context.GetLazyString(key.Buffer, index);
        }

        // TODO unify if possible with AllowedPathsValidator
        public LazyStringValue GetDocumentId(ReplicationBatchItem item)
        {
            return item switch
            {
                AttachmentReplicationItem a => GetDocumentId(a.Key),
                AttachmentTombstoneReplicationItem at => GetDocumentId(at.Key),
                CounterReplicationItem c => c.Id,
                DocumentReplicationItem d => d.Id,
                RevisionTombstoneReplicationItem r => GetDocumentId(r.Id),
                TimeSeriesDeletedRangeItem td => GetDocumentId(td.Key),
                TimeSeriesReplicationItem t => GetDocumentId(t.Key),
                _ => throw new ArgumentOutOfRangeException($"{nameof(item)} - {item}")
            };
        }

        public unsafe string GetItemInformation(ReplicationBatchItem item)
        {
            switch (item)
            {
                case AttachmentReplicationItem a:
                    return $"Attachment '{a.Name}' for {GetDocumentId(a.Key)}";
                case AttachmentTombstoneReplicationItem at:
                    var result = AttachmentsStorage.ExtractDocIdAndAttachmentNameFromTombstone(at.Key);
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
                    return "Time Series deletion range for: " + GetDocumentId(td.Key);
                case TimeSeriesReplicationItem t:
                    var baseline = TimeSeriesStorage.GetBaseline(t.Key.Content.Ptr, t.Key.Content.Length);
                    return $"Time Series segment of '{t.Name}' [{baseline:s} - {t.Segment.GetLastTimestamp(baseline):s}] for {GetDocumentId(t.Key)}";
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
