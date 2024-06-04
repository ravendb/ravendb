using System;
using System.Collections.Generic;
using Raven.Server.Documents.Replication.ReplicationItems;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Replication
{
    public class AllowedPathsValidator : IDisposable
    {
        private readonly JsonOperationContext _allowedPathsContext;
        private readonly List<LazyStringValue> _allowedPaths;
        private readonly List<LazyStringValue> _allowedPathsPrefixes;
        private DocumentInfoHelper _documentInfoHelper;
        private LazyStringValue GetDocumentId(Slice key) => _documentInfoHelper.GetDocumentId(key);
        public string GetItemInformation(ReplicationBatchItem item) => _documentInfoHelper.GetItemInformation(item);
        
        public bool ShouldAllow(ReplicationBatchItem item)
        {
            return item switch
            {
                AttachmentReplicationItem a => AllowId(GetDocumentId(a.Key)),
                AttachmentTombstoneReplicationItem at => AllowId(GetDocumentId(at.Key)),
                CounterReplicationItem c => AllowId(c.Id),
                DocumentReplicationItem d => AllowId(d.Id),
                RevisionTombstoneReplicationItem _ => true, // revision tombstones doesn't contain any info about the doc. The id here is the change-vector of the deleted revision
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
            _allowedPathsContext = JsonOperationContext.ShortTermSingleUse();
            _documentInfoHelper = new DocumentInfoHelper(_allowedPathsContext);
            _allowedPaths = new List<LazyStringValue>();
            _allowedPathsPrefixes = new List<LazyStringValue>();
            foreach (var t in allowedPaths)
            {
                var lazyStringValue = _allowedPathsContext.GetLazyString(t);
                if (lazyStringValue.Size == 0)
                    continue; // shouldn't happen, but let's be safe
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

        public void Dispose()
        {
            _allowedPathsContext?.Dispose();
            _documentInfoHelper?.Dispose();
        }
    }
}
