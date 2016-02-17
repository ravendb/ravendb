using System;
using System.Threading.Tasks;
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

            OnDocumentChange?.Invoke(notification);
        }

        public void Connect(NotificationsClientConnection connection)
        {
            _connections.Add(connection);
            Task.Run(connection.StartSendingNotifications);
        }

        public void Disconnect(NotificationsClientConnection connection)
        {
            _connections.TryRemove(connection);
            connection.Dispose();
        }
    }
}