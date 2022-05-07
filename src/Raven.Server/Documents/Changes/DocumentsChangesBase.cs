using System.Collections.Concurrent;
using Sparrow.Json;

namespace Raven.Server.Documents.Changes;

public abstract class DocumentsChangesBase<TChangesClientConnection, TOperationContext>
    where TChangesClientConnection : AbstractChangesClientConnection<TOperationContext>
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
