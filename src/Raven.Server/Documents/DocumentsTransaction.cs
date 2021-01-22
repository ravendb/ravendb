using System;
using System.Collections.Generic;
using Raven.Client.Documents.Changes;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Voron.Impl;

namespace Raven.Server.Documents
{
    public class DocumentsTransaction : RavenTransaction
    {
        private readonly DocumentsOperationContext _context;

        private readonly DocumentsChanges _changes;

        private List<DocumentChange> _documentNotifications;

        private List<CounterChange> _counterNotifications;

        private List<TimeSeriesChange> _timeSeriesNotifications;

        private bool _replaced;

        private Dictionary<string, CollectionName> _collectionCache;

        public DocumentsTransaction(DocumentsOperationContext context, Transaction transaction, DocumentsChanges changes)
            : base(transaction)
        {
            _context = context;
            _changes = changes;
        }

        public DocumentsTransaction BeginAsyncCommitAndStartNewTransaction(DocumentsOperationContext context)
        {
            _replaced = true;
            var tx = InnerTransaction.BeginAsyncCommitAndStartNewTransaction(context.PersistentContext);
            return new DocumentsTransaction(context, tx, _changes);
        }

        public void AddAfterCommitNotification(DocumentChange change)
        {
            change.TriggeredByReplicationThread = IncomingReplicationHandler.IsIncomingReplication;

            if (_documentNotifications == null)
                _documentNotifications = new List<DocumentChange>();
            _documentNotifications.Add(change);
        }

        public void AddAfterCommitNotification(CounterChange change)
        {
            change.TriggeredByReplicationThread = IncomingReplicationHandler.IsIncomingReplication;

            if (_counterNotifications == null)
                _counterNotifications = new List<CounterChange>();
            _counterNotifications.Add(change);
        }

        public void AddAfterCommitNotification(TimeSeriesChange change)
        {
            change.TriggeredByReplicationThread = IncomingReplicationHandler.IsIncomingReplication;

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

        private static void ThrowInvalidTransactionUsage()
        {
            throw new InvalidOperationException("There is a different transaction in context.");
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
                _collectionCache = new Dictionary<string, CollectionName>(OrdinalIgnoreCaseStringStructComparer.Instance);

            _collectionCache.Add(collectionName, name);
        }

        public void ForgetAbout(Document doc)
        {
            if (doc == null)
                return;
            InnerTransaction.ForgetAbout(doc.StorageId);
        }
    }
}
