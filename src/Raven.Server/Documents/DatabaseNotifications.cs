using System;

using Raven.Abstractions.Data;

namespace Raven.Server.Documents
{
    public class DatabaseNotifications
    {
        public event Action<DocumentChangeNotification> OnDocumentChange;

        public void RaiseNotifications(DocumentChangeNotification notification)
        {
            var onDocumentChange = OnDocumentChange;
            onDocumentChange?.Invoke(notification);
        }
    }
}