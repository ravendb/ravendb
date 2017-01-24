using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Voron.Impl;

namespace Raven.Server.Documents
{
    public class DocumentsTransaction : RavenTransaction
    {
        private readonly DocumentsOperationContext _context;

        private readonly DocumentsNotifications _notifications;

        private List<DocumentChangeNotification> _documentNotifications;

        private List<DocumentChangeNotification> _systemDocumentChangeNotifications;

        public DocumentsTransaction(DocumentsOperationContext context, Transaction transaction, DocumentsNotifications notifications)
            : base(transaction)
        {
            _context = context;
            _notifications = notifications;
        }

        public void AddAfterCommitNotification(DocumentChangeNotification notification)
        {
            notification.TriggeredByReplicationThread = IncomingReplicationHandler.IsIncomingReplicationThread;

            if (notification.IsSystemDocument)
            {
                if (_systemDocumentChangeNotifications == null)
                    _systemDocumentChangeNotifications = new List<DocumentChangeNotification>();
                _systemDocumentChangeNotifications.Add(notification);
            }
            else
            {
                if (_documentNotifications == null)
                    _documentNotifications = new List<DocumentChangeNotification>();
                _documentNotifications.Add(notification);
            }
        }

        public override void Dispose()
        {
            if (_context.Transaction != null && _context.Transaction != this)
                ThrowInvalidTransactionUsage();

            _context.Transaction = null;
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
                    _notifications.RaiseSystemNotifications(notification);
                }
            }

            if (_documentNotifications == null)
                return;

            if (ThreadPool.QueueUserWorkItem(state => ((DocumentsTransaction)state).RaiseNotifications(), this) == false)
            {
                RaiseNotifications(); // raise immediately
            }
        }

        private void RaiseNotifications()
        {
            foreach (var notification in _documentNotifications)
            {
                _notifications.RaiseNotifications(notification);
            }
        }
    }
}