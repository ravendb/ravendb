using System;
using System.Net.WebSockets;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
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
                var remoteNodeUrl = WebSocketProxy.GetRemoteNodeUrl(ServerStore, nodeTag);
                var remoteEndpointUrl = GetRemoteEndpointUrl(databaseName);

                await WebSocketProxy.RelayAsync(ServerStore, webSocket, remoteNodeUrl, remoteEndpointUrl, RequestHandler.Server.Certificate?.ClientCertificate, Logger, token.Token);
            }
        }
    }
}
