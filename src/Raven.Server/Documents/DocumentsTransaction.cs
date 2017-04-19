using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Client.Documents.Changes;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Utils;
using Voron.Impl;

namespace Raven.Server.Documents
{
    public class DocumentsTransaction : RavenTransaction
    {
        private readonly DocumentsOperationContext _context;

        private readonly DocumentsChanges _changes;

        private List<DocumentChange> _documentNotifications;

        private List<DocumentChange> _systemDocumentChangeNotifications;
        private bool _replaced;

        public DocumentsTransaction(DocumentsOperationContext context, Transaction transaction, DocumentsChanges changes)
            : base(transaction)
        {
            _context = context;
            _changes = changes;
        }

        public DocumentsTransaction BeginAsyncCommitAndStartNewTransaction()
        {
            _replaced = true;
            var tx = InnerTransaction.BeginAsyncCommitAndStartNewTransaction();
            return new DocumentsTransaction(_context, tx, _changes);
        }

        public void AddAfterCommitNotification(DocumentChange change)
        {
            change.TriggeredByReplicationThread = IncomingReplicationHandler.IsIncomingReplicationThread;

            if (change.IsSystemDocument)
            {
                if (_systemDocumentChangeNotifications == null)
                    _systemDocumentChangeNotifications = new List<DocumentChange>();
                _systemDocumentChangeNotifications.Add(change);
            }
            else
            {
                if (_documentNotifications == null)
                    _documentNotifications = new List<DocumentChange>();
                _documentNotifications.Add(change);
            }
        }

        private bool _isDisposed = false;

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

            if (InnerTransaction.LowLevelTransaction.Committed)
                AfterCommit();
        }

        private static void ThrowInvalidTransactionUsage()
        {
            throw new InvalidOperationException("There is a different transaction in context.");
        }

        private void AfterCommit()
        {
            if (_systemDocumentChangeNotifications != null)
            {
                foreach (var notification in _systemDocumentChangeNotifications)
                {
                    _changes.RaiseSystemNotifications(notification);
                }
            }

            if (_documentNotifications == null)
                return;

            TaskExecuter.Execute(state => ((DocumentsTransaction)state).RaiseNotifications(), this);
        }

        public bool ModifiedSystemDocuments => _systemDocumentChangeNotifications?.Count > 0;

        private void RaiseNotifications()
        {
            foreach (var notification in _documentNotifications)
            {
                _changes.RaiseNotifications(notification);
            }
        }
    }
}