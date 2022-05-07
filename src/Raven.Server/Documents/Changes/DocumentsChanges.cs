using System;
using System.Collections.Concurrent;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Changes
{
    public class DocumentsChanges : DocumentsChangesBase<ChangesClientConnection, DocumentsOperationContext>
    {
        public event Action<DocumentChange> OnDocumentChange;

        public event Action<CounterChange> OnCounterChange;

        public event Action<TimeSeriesChange> OnTimeSeriesChange;

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

        public void RaiseNotifications(TimeSeriesChange timeSeriesChange)
        {
            OnTimeSeriesChange?.Invoke(timeSeriesChange);

            foreach (var connection in Connections)
            {
                if (!connection.Value.IsDisposed)
                    connection.Value.SendTimeSeriesChanges(timeSeriesChange);
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
    }

    public abstract class DocumentsChangesBase<TChangesClientConnection, TOperationContext>
        where TChangesClientConnection : ChangesClientConnectionBase<TOperationContext>
        where TOperationContext : JsonOperationContext
    {
        public readonly ConcurrentDictionary<long, TChangesClientConnection> Connections = new ConcurrentDictionary<long, TChangesClientConnection>();

        public void Connect(TChangesClientConnection connection)
        {
            Connections.TryAdd(connection.Id, connection);
        }

        public void Disconnect(long id)
        {
            if (Connections.TryRemove(id, out TChangesClientConnection connection))
                connection.Dispose();
        }
    }
}
