using System;
using System.Collections.Concurrent;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations;

namespace Raven.Server.Documents
{
    public class DocumentsChanges
    {
        public readonly ConcurrentDictionary<long, ChangesClientConnection> Connections = new ConcurrentDictionary<long, ChangesClientConnection>();

        public event Action<DocumentChange> OnDocumentChange;

        public event Action<CounterChange> OnCounterChange;

        public event Action<IndexChange> OnIndexChange;

        public event Action<OperationStatusChange> OnOperationStatusChange;

        public event Action<TopologyChange> OnTopologyChange;

        public void RaiseNotifications(TopologyChange topologyChange)
        {
            OnTopologyChange?.Invoke(topologyChange);

            foreach (var connection in Connections)
                connection.Value.SendTopologyChanges(topologyChange);
        }

        public void RaiseNotifications(IndexChange indexChange)
        {
            OnIndexChange?.Invoke(indexChange);

            foreach (var connection in Connections)
                connection.Value.SendIndexChanges(indexChange);
        }

        public void RaiseNotifications(DocumentChange documentChange)
        {
            OnDocumentChange?.Invoke(documentChange);

            foreach (var connection in Connections)
            {
                if (!connection.Value.IsDisposed)
                    connection.Value.SendDocumentChanges(documentChange);
            }
        }

        public void RaiseNotifications(CounterChange counterChange)
        {
            OnCounterChange?.Invoke(counterChange);

            foreach (var connection in Connections)
            {
                if (!connection.Value.IsDisposed)
                    connection.Value.SendCounterChanges(counterChange);
            }
        }

        public void RaiseNotifications(OperationStatusChange operationStatusChange)
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
            if (Connections.TryRemove(id, out ChangesClientConnection connection))
                connection.Dispose();
        }
    }
}
