using System;

using Raven.Abstractions.Data;
using Raven.Client.Data;

using Sparrow.Collections;

namespace Raven.Server.Documents
{
    public class DocumentsNotifications
    {
        private readonly ConcurrentSet<NotificationsClientConnection> _connections = new ConcurrentSet<NotificationsClientConnection>(new NotificationsClientConnectionComparer());

        public event Action<DocumentChangeNotification> OnDocumentChange;

        public void RaiseNotifications(Notification notification)
        {
            var documentChangeNotification = notification as DocumentChangeNotification;
            if (documentChangeNotification != null)
            {
                foreach (var connection in _connections)
                    connection.SendDocumentChanges(documentChangeNotification);

                OnDocumentChange?.Invoke(documentChangeNotification);
                return;
            }

            throw new NotSupportedException();
        }

        public void Connect(NotificationsClientConnection connection)
        {
            _connections.Add(connection);
        }

        public void Disconnect(NotificationsClientConnection connection)
        {
            _connections.TryRemove(connection);
            connection.Dispose();
        }
    }
}