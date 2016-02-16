using System;
using Raven.Abstractions.Data;
using Sparrow.Collections;

namespace Raven.Server.Documents
{
    public class DatabaseNotifications
    {
        readonly ConcurrentSet<NotificationsClientConnection> _connections = new ConcurrentSet<NotificationsClientConnection>(new NotificationsClientConnectionComparer());

        public event Action<DocumentChangeNotification> OnDocumentChange;

        public void RaiseNotifications(DocumentChangeNotification notification)
        {
            foreach (var connection in _connections)
            {
                connection.SendDocumentChanges(notification);
            }

            var onDocumentChange = OnDocumentChange;
            onDocumentChange?.Invoke(notification);
        }

        public void Connect(NotificationsClientConnection connection)
        {
            _connections.Add(connection);
        }

        public void Disconnect(NotificationsClientConnection connection)
        {
            _connections.TryRemove(connection);
        }
    }
}