using System;
using System.Collections.Concurrent;
using Raven.Abstractions.Data;
using Raven.Client.Data;

using Sparrow.Collections;

namespace Raven.Server.Documents
{
    public class DocumentsNotifications
    {
        public readonly ConcurrentDictionary<long, NotificationsClientConnection> Connections = new ConcurrentDictionary<long, NotificationsClientConnection>();

        public event Action<DocumentChangeNotification> OnDocumentChange;

        public void RaiseNotifications(Notification notification)
        {
            var documentChangeNotification = notification as DocumentChangeNotification;
            if (documentChangeNotification != null)
            {
                foreach (var connection in Connections)
                    connection.Value.SendDocumentChanges(documentChangeNotification);

                OnDocumentChange?.Invoke(documentChangeNotification);
                return;
            }

            throw new NotSupportedException();
        }

        public void Connect(NotificationsClientConnection connection)
        {
            Connections.TryAdd(connection.Id, connection);
        }

        public void Disconnect(long id)
        {
            NotificationsClientConnection connection;
            if (Connections.TryRemove(id, out connection))
                connection.Dispose();
        }
    }
}