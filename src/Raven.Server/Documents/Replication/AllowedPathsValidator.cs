using System;
using System.Collections.Generic;
using Raven.Server.Documents.Replication.ReplicationItems;
using Sparrow.Json;
using Sparrow.Server.Utils;
using Voron;

namespace Raven.Server.Documents.Replication
{
    public class AllowedPathsValidator : IDisposable
    {
        private readonly JsonOperationContext _allowedPathsContext;
        private readonly List<LazyStringValue> _allowedPaths;
        private readonly List<LazyStringValue> _allowedPathsPrefixes;
        private LazyStringValue _tmpLazyStringInstance;

        private unsafe LazyStringValue GetDocumentId(Slice key)
        {
            var sepIdx = key.Content.IndexOf(SpecialChars.RecordSeparator);
            if (_tmpLazyStringInstance == null)
                _tmpLazyStringInstance = new LazyStringValue(null, null, 0, _allowedPathsContext);
            _tmpLazyStringInstance.Renew(null, key.Content.Ptr, sepIdx, _allowedPathsContext);
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

        public bool ShouldAllow(ReplicationBatchItem item)
        {
            return item switch
            {
                AttachmentReplicationItem a => AllowId(GetDocumentId(a.Key)),
                AttachmentTombstoneReplicationItem at => AllowId(GetDocumentId(at.Key)),
                CounterReplicationItem c => AllowId(c.Id),
                DocumentReplicationItem d => AllowId(d.Id),
                RevisionTombstoneReplicationItem r => AllowId(r.Id),
                TimeSeriesDeletedRangeItem td => AllowId(GetDocumentId(td.Key)),
                TimeSeriesReplicationItem t => AllowId(GetDocumentId(t.Key)),
                _ => throw new ArgumentOutOfRangeException($"{nameof(item)} - {item}")
            };
        }

        private bool AllowId(LazyStringValue id)
        {
            foreach (LazyStringValue path in _allowedPaths)
            {
                if (path.EqualsOrdinalIgnoreCase(id))
                {
                    return true;
                }
            }

            foreach (var prefix in _allowedPathsPrefixes)
            {
                if (id.StartsWithOrdinalIgnoreCase(prefix))
                    return true;
            }

            return false;
        }

        public AllowedPathsValidator(string[] allowedPaths)
        {
            {
                _allowedPathsContext = JsonOperationContext.ShortTermSingleUse();
                _allowedPaths = new List<LazyStringValue>();
                _allowedPathsPrefixes = new List<LazyStringValue>();
                foreach (var t in allowedPaths)
                {
                    var lazyStringValue = _allowedPathsContext.GetLazyString(t);
                    if (lazyStringValue.Size == 0)
                        continue;// shouldn't happen, but let's be safe
                    if (lazyStringValue[lazyStringValue.Size - 1] == '*')
                    {
                        lazyStringValue.Truncate(lazyStringValue.Size - 1);
                        _allowedPathsPrefixes.Add(lazyStringValue);
                    }
                    else
                    {
                        _allowedPaths.Add(lazyStringValue);
                    }
                }
            }
        }

        public void Dispose()
        {
            _allowedPathsContext?.Dispose();
        }
    }
}
