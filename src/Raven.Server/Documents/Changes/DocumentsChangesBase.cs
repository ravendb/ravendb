using System;
using System.Collections.Concurrent;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations;
using Sparrow.Json;

namespace Raven.Server.Documents.Changes;

public abstract class DocumentsChangesBase<TChangesClientConnection, TOperationContext> : IDocumentsChanges
    where TChangesClientConnection : AbstractChangesClientConnection<TOperationContext>
    where TOperationContext : JsonOperationContext
{
    public readonly ConcurrentDictionary<long, TChangesClientConnection> Connections = new();

    public event Action<OperationStatusChange> OnOperationStatusChange;

    public event Action<TopologyChange> OnTopologyChange;

    public void Connect(TChangesClientConnection connection)
    {
        Connections.TryAdd(connection.Id, connection);
    }

    public void Disconnect(long id)
    {
        if (Connections.TryRemove(id, out TChangesClientConnection connection))
            connection.Dispose();
    }

    public void RaiseNotifications(TopologyChange topologyChange)
    {
        OnTopologyChange?.Invoke(topologyChange);

        foreach (var connection in Connections)
            connection.Value.SendTopologyChanges(topologyChange);
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
