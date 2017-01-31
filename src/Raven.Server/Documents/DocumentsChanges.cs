using System;
using System.Collections.Concurrent;
using Raven.Abstractions.Data;
using Raven.Client.Data;

namespace Raven.Server.Documents
{
    public class DocumentsChanges
    {
        public readonly ConcurrentDictionary<long, ChangesClientConnection> Connections = new ConcurrentDictionary<long, ChangesClientConnection>();

        public event Action<DocumentChange> OnSystemDocumentChange;

        public event Action<DocumentChange> OnDocumentChange;

        public event Action<IndexChange> OnIndexChange;

        public event Action<TransformerChange> OnTransformerChange;

        public event Action<OperationStatusChanged> OnOperationStatusChange;

        public void RaiseNotifications(IndexChange indexChange)
        {
            OnIndexChange?.Invoke(indexChange);

            foreach (var connection in Connections)
                connection.Value.SendIndexChanges(indexChange);
        }

        public void RaiseNotifications(TransformerChange transformerChange)
        {
            OnTransformerChange?.Invoke(transformerChange);

            foreach (var connection in Connections)
                connection.Value.SendTransformerChanges(transformerChange);
        }

        public void RaiseSystemNotifications(DocumentChange documentChange)
        {
            OnSystemDocumentChange?.Invoke(documentChange);

            foreach (var connection in Connections)
                connection.Value.SendDocumentChanges(documentChange);
        }

        public void RaiseNotifications(DocumentChange documentChange)
        {
            OnDocumentChange?.Invoke(documentChange);

            foreach (var connection in Connections)
            {
                if (connection.Value.IsDisposed == false)
                    connection.Value.SendDocumentChanges(documentChange);
            }
                
        }

        public void RaiseNotifications(OperationStatusChanged operationStatusChange)
        {
            OnOperationStatusChange?.Invoke(operationStatusChange);

            foreach (var connection in Connections)
            {
                connection.Value.SendOperationStatusChangeNotification(operationStatusChange);
            }
        }

        public void Connect(ChangesClientConnection connection)
        {
            Connections.TryAdd(connection.Id, connection);
        }

        public void Disconnect(long id)
        {
            ChangesClientConnection connection;
            if (Connections.TryRemove(id, out connection))
                connection.Dispose();
        }
    }
}