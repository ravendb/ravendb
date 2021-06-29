using System;
using Raven.Server.Documents.Replication.ReplicationItems;
using Sparrow.Json;
using Sparrow.Server.Utils;
using Voron;

namespace Raven.Server.Documents.Replication
{
    public class DocumentInfoHelper : IDisposable
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
        public string GetItemInformation(ReplicationBatchItem item)
        {
            return item switch
            {
                AttachmentReplicationItem a => "Attachment for " + GetDocumentId(a.Key),
                AttachmentTombstoneReplicationItem at => "Attachment tombstone for: " + GetDocumentId(at.Key),
                CounterReplicationItem c => "Counter for " + c.Id,
                DocumentReplicationItem d => "Document " + d.Id,
                RevisionTombstoneReplicationItem r => "Revision for: " + r.Id,
                TimeSeriesDeletedRangeItem td => "Time Series deletion range for: " + GetDocumentId(td.Key),
                TimeSeriesReplicationItem t => "Time Series for: " + GetDocumentId(t.Key),
                _ => throw new ArgumentOutOfRangeException($"{nameof(item)} - {item}")
            };
        }
        public void Dispose()
        {
            if (_contextOwner)
                _context.Dispose();
        }
    }
}
