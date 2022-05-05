using System;
using System.Net.WebSockets;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.NotificationCenter.Handlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors;

internal abstract class AbstractHandlerWebSocketProxyProcessor<TRequestHandler, TOperationContext> : AbstractHandlerProxyProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractHandlerWebSocketProxyProcessor([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract ValueTask HandleCurrentNodeAsync(WebSocket webSocket, OperationCancelToken token);

    protected virtual string GetDatabaseName() => RequestHandler.DatabaseName;

    protected virtual string GetRemoteEndpointUrl(string databaseName) => throw new NotSupportedException($"Processor '{GetType().Name}' does not support creating URLs.");

    public override async ValueTask ExecuteAsync()
    {
        using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
        using (var token = RequestHandler.CreateOperationToken())
        {
            if (IsCurrentNode(out var nodeTag))
            {
                await HandleCurrentNodeAsync(webSocket, token);
            }
            else
            {
                var databaseName = GetDatabaseName();
                var remoteNodeUrl = GetRemoteNodeUrl(nodeTag);
                var remoteEndpointUrl = GetRemoteEndpointUrl(databaseName);

                await HandleRemoteNodeAsync(webSocket, remoteNodeUrl, remoteEndpointUrl, token);
            }
        }
    }

    private async ValueTask HandleRemoteNodeAsync(WebSocket webSocket, string remoteNodeUrl, string remoteEndpointUrl, OperationCancelToken token)
    {
        using (var connection = new ProxyWebSocketConnection(webSocket, remoteNodeUrl, remoteEndpointUrl, ServerStore.ContextPool, token.Token))
        {
            await connection.Establish(RequestHandler.Server.Certificate?.Certificate);

            await connection.RelayData();
        }
    }

    private string GetRemoteNodeUrl(string nodeTag)
    {
        ClusterTopology topology;

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext txOperationContext))
        using (txOperationContext.OpenReadTransaction())
        {
            topology = ServerStore.GetClusterTopology(txOperationContext);
        }

        var remoteNodeUrl = topology.GetUrlFromTag(nodeTag);

        if (string.IsNullOrEmpty(remoteNodeUrl))
        {
            throw new InvalidOperationException($"Could not find node url for node tag '{nodeTag}'");
        }

        return remoteNodeUrl;
    }
}
