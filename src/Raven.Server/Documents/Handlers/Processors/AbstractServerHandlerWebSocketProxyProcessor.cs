using System;
using System.Net.WebSockets;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Logging;
using Raven.Server.ServerWide;
using Raven.Server.Web;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.Handlers.Processors;

internal abstract class AbstractServerHandlerWebSocketProxyProcessor : AbstractServerHandlerProxyProcessor
{
    private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer<AbstractServerHandlerWebSocketProxyProcessor>();

    protected AbstractServerHandlerWebSocketProxyProcessor([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected abstract ValueTask HandleCurrentNodeAsync(WebSocket webSocket, OperationCancelToken token);

    protected abstract string GetRemoteEndpointUrl();

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
                }
                catch (ObjectDisposedException)
                {
                }
                catch (WebSocketException)
                {
                }
                catch (Exception e)
                {
                    await WebSocketProxy.SendErrorAsync(ServerStore, webSocket, e, Logger);
                }
            }
            else
            {
                string remoteNodeUrl;

                try
                {
                    remoteNodeUrl = WebSocketProxy.GetRemoteNodeUrl(ServerStore, nodeTag);
                }
                catch (Exception e)
                {
                    await WebSocketProxy.SendErrorAsync(ServerStore, webSocket, e, Logger);
                    return;
                }

                await WebSocketProxy.RelayAsync(ServerStore, webSocket, remoteNodeUrl, GetRemoteEndpointUrl(), RequestHandler.Server.Certificate?.ClientCertificate, Logger, token.Token);
            }
        }
    }
}
