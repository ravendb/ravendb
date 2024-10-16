using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.NotificationCenter.Handlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;

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
        using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
        {
            if (IsCurrentNode(out var nodeTag))
            {
                try
                {
                    await HandleCurrentNodeAsync(webSocket, token);
                }
                catch (OperationCanceledException)
                {
                    // ignored
                }
                catch (ObjectDisposedException)
                {
                    // ignored
                }
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
        try
        {
            using (var connection = new ProxyWebSocketConnection(webSocket, remoteNodeUrl, remoteEndpointUrl, ServerStore.ContextPool, token.Token))
            {
                await connection.Establish(RequestHandler.Server.Certificate?.Certificate);

                await connection.RelayData();
            }
        }
        catch (OperationCanceledException)
        {
            // ignored 
        }
        catch (ObjectDisposedException)
        {
            // ignored
        }
        catch (Exception ex)
        {
            await HandleExceptionAsync(ex, webSocket);
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

    private async ValueTask HandleExceptionAsync(Exception ex, WebSocket webSocket)
    {
        try
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var ms = RecyclableMemoryStreamFactory.GetRecyclableStream())
            {
                using (var writer = new BlittableJsonTextWriter(context, ms))
                {
                    context.Write(writer, new DynamicJsonValue { ["Type"] = "Error", ["Exception"] = ex.ToString() });
                }

                ms.TryGetBuffer(out ArraySegment<byte> bytes);
                await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, ServerStore.ServerShutdown);
            }
        }
        catch (Exception)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info("Failed to send the error in cluster dashboard handler to the client", ex);
        }
    }
}
