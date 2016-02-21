using System;

using Raven.Abstractions.Data;
using Raven.Client.Data;

using Sparrow.Collections;

namespace Raven.Server.Documents
{
    public class DocumentsNotifications
    {
        public readonly ConcurrentSet<NotificationsClientConnection> Connections = new ConcurrentSet<NotificationsClientConnection>(new NotificationsClientConnectionComparer());

        public event Action<DocumentChangeNotification> OnDocumentChange;

        public void RaiseNotifications(Notification notification)
        {
            var documentChangeNotification = notification as DocumentChangeNotification;
            if (documentChangeNotification != null)
            {
                foreach (var connection in Connections)
                    connection.SendDocumentChanges(documentChangeNotification);

                OnDocumentChange?.Invoke(documentChangeNotification);
                return;
            }

            throw new NotSupportedException();
        }

        public void Connect(NotificationsClientConnection connection)
        {
            Connections.Add(connection);
        }

        public void Disconnect(NotificationsClientConnection connection)
        {
            Connections.TryRemove(connection);
            connection.Dispose();
        }
    }
}