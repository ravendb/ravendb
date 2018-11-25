using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Client.Documents.Changes;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Voron.Impl;

namespace Raven.Server.Documents
{
    public class DocumentsTransaction : RavenTransaction
    {
        private readonly DocumentsOperationContext _context;

        private readonly DocumentsChanges _changes;

        private List<DocumentChange> _documentNotifications;

        private bool _replaced;

        public DocumentsTransaction(DocumentsOperationContext context, Transaction transaction, DocumentsChanges changes)
            : base(transaction)
        {
            _context = context;
            _changes = changes;
        }

        public DocumentsTransaction BeginAsyncCommitAndStartNewTransaction(DocumentsOperationContext context)
        {
            _replaced = true;
            var tx = InnerTransaction.BeginAsyncCommitAndStartNewTransaction();
            return new DocumentsTransaction(context, tx, _changes);
        }

        public void AddAfterCommitNotification(DocumentChange change)
        {
            change.TriggeredByReplicationThread = IncomingReplicationHandler.IsIncomingReplication;

            if (_documentNotifications == null)
                _documentNotifications = new List<DocumentChange>();
            _documentNotifications.Add(change);
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

            var committed = InnerTransaction.LowLevelTransaction.Committed;

            base.Dispose();

            if (committed)
                AfterCommit();
        }

        private static void ThrowInvalidTransactionUsage()
        {
            throw new InvalidOperationException("There is a different transaction in context.");
        }

        private void AfterCommit()
        {
            if (_documentNotifications == null)
                return;

            ThreadPool.QueueUserWorkItem(state => ((DocumentsTransaction)state).RaiseNotifications(), this);
        }

        private void RaiseNotifications()
        {
            foreach (var notification in _documentNotifications)
            {
                _changes.RaiseNotifications(notification);
            }
        }
    }
}
