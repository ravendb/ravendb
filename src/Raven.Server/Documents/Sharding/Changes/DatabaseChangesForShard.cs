using System;
using System.Net.WebSockets;
using Raven.Client.Documents.Changes;
using Raven.Client.Http;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Sharding.Changes;

internal class DatabaseChangesForShard : DatabaseChanges
{
    private readonly ServerStore _server;

    public DatabaseChangesForShard(ServerStore server, RequestExecutor requestExecutor, string databaseName, Action onDispose, string nodeTag)
        : base(requestExecutor, databaseName, onDispose, nodeTag)
    {
        _server = server;
    }

    protected override ClientWebSocket CreateClientWebSocket(RequestExecutor requestExecutor) => 
        ShardedDatabaseChanges.CreateClientWebSocket(_server, requestExecutor);
}
