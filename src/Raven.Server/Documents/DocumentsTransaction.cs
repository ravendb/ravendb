using System;
using System.Collections.Generic;
using Amazon.Runtime.Internal.Transform;
using Raven.Client.Documents.Changes;
using Raven.Client.Extensions;
using Raven.Server.Documents.Changes;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents
{
    public sealed class DocumentsTransaction : RavenTransaction
    {
        private readonly DocumentsOperationContext _context;

        private readonly DocumentsChanges _changes;

        private List<DocumentChange> _documentNotifications;

        private List<CounterChange> _counterNotifications;

        private List<TimeSeriesChange> _timeSeriesNotifications;

        private Dictionary<bool, List<Slice>> _attachmentHashesToMaybeDelete;

        private bool _executeDocumentsMigrationAfterCommit;

        private bool _replaced;

        private Dictionary<string, CollectionName> _collectionCache;

        public DocumentsTransaction(DocumentsOperationContext context, Transaction transaction, DocumentsChanges changes)
            : base(transaction)
        {
            _context = context;
            _changes = changes;

            if (context.DocumentDatabase is ShardedDocumentDatabase sharded)
            {
                transaction.Owner = _context;
                transaction.OnBeforeCommit += sharded.ShardedDocumentsStorage.OnBeforeCommit;
                transaction.LowLevelTransaction.OnRollBack += sharded.ShardedDocumentsStorage.OnFailure;
            }
        }

        public override void BeforeCommit()
        {
            if (_attachmentHashesToMaybeDelete == null)
                return;

            _context.DocumentDatabase.DocumentsStorage.AttachmentsStorage.RemoveAttachmentStreamsWithoutReferences(_context, _attachmentHashesToMaybeDelete);
        }

        protected override void AfterCommit()
        {
            if (_executeDocumentsMigrationAfterCommit)
            {
                var shardedDatabase = ShardedDocumentDatabase.CastToShardedDocumentDatabase(_context.DocumentDatabase);
                shardedDatabase.DocumentsMigrator.ExecuteMoveDocumentsAsync().IgnoreUnobservedExceptions();
            }

            base.AfterCommit();
        }

        public DocumentsTransaction BeginAsyncCommitAndStartNewTransaction(DocumentsOperationContext context)
        {
            BeforeCommit();
            _replaced = true;
            var tx = InnerTransaction.BeginAsyncCommitAndStartNewTransaction(context.PersistentContext);
            return new DocumentsTransaction(context, tx, _changes);
        }

        public void AddAfterCommitNotification(DocumentChange change)
        {
            change.TriggeredByReplicationThread = IncomingReplicationHandler.IsIncomingInternalReplication;

            if (_documentNotifications == null)
                _documentNotifications = new List<DocumentChange>();
            _documentNotifications.Add(change);
        }

        public void AddAfterCommitNotification(CounterChange change)
        {
            change.TriggeredByReplicationThread = IncomingReplicationHandler.IsIncomingInternalReplication;

            if (_counterNotifications == null)
                _counterNotifications = new List<CounterChange>();
            _counterNotifications.Add(change);
        }

        public void AddAfterCommitNotification(TimeSeriesChange change)
        {
            change.TriggeredByReplicationThread = IncomingReplicationHandler.IsIncomingInternalReplication;

            if (_timeSeriesNotifications == null)
                _timeSeriesNotifications = new List<TimeSeriesChange>();
            _timeSeriesNotifications.Add(change);
        }

        private bool _isDisposed;

        public override void Dispose()
        {
            if (_isDisposed)
                return;
            _isDisposed = true;

            if (_replaced == false)
            {
                if (_context.Transaction != null && _context.Transaction != this)
                    ThrowInvalidTransactionUsage();

                _context.Transaction = null;
            }

            base.Dispose();
        }

        protected override void RaiseNotifications()
        {
            base.RaiseNotifications();

            if (_documentNotifications?.Count > 0)
            {
                foreach (var notification in _documentNotifications)
                {
                    _changes.RaiseNotifications(notification);
                }
            }

            if (_counterNotifications?.Count > 0)
            {
                foreach (var notification in _counterNotifications)
                {
                    _changes.RaiseNotifications(notification);
                }
            }

            if (_timeSeriesNotifications?.Count > 0)
            {
                foreach (var notification in _timeSeriesNotifications)
                {
                    _changes.RaiseNotifications(notification);
                }
            }
        }

        protected override bool ShouldRaiseNotifications()
        {
            return base.ShouldRaiseNotifications()
                || _documentNotifications != null
                || _counterNotifications != null
                || _timeSeriesNotifications != null;
        }

        public bool TryGetFromCache(string collectionName, out CollectionName name)
        {
            if (_collectionCache != null)
                return _collectionCache.TryGetValue(collectionName, out name);

            name = null;
            return false;
        }

        public void AddToCache(string collectionName, CollectionName name)
        {
            if (_collectionCache == null)
                _collectionCache = new Dictionary<string, CollectionName>(StringComparer.OrdinalIgnoreCase);

            _collectionCache.Add(collectionName, name);
        }

        public void ForgetAbout(Document doc)
        {
            if (doc == null)
                return;
            InnerTransaction.ForgetAbout(doc.StorageId);
        }

        internal void CheckIfShouldDeleteAttachmentStream(Slice hash, bool fromRetire)
        {
            var clone = hash.Clone(InnerTransaction.Allocator);
                _attachmentHashesToMaybeDelete ??= new();
            if (fromRetire)
            {
                if (_attachmentHashesToMaybeDelete.TryGetValue(true, out var val) == false)
                {
                    _attachmentHashesToMaybeDelete.Add(true, new List<Slice>(){ clone });
                    return;
                }

                val.Add(hash);
                _attachmentHashesToMaybeDelete[true] = val;
            }
            else
            {
                if (_attachmentHashesToMaybeDelete.TryGetValue(false, out var val) == false)
                {
                    _attachmentHashesToMaybeDelete.Add(false, new List<Slice>() { clone });
                    return;
                }

                val.Add(hash);
                _attachmentHashesToMaybeDelete[false] = val;
            }
        }

        internal void ExecuteDocumentsMigrationAfterCommit()
        {
            _executeDocumentsMigrationAfterCommit = true;
        }
    }
}
