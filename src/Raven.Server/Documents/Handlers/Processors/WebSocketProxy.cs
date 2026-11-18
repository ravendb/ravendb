using System;
using System.Net.WebSockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Server.NotificationCenter.Handlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.Handlers.Processors;

internal static class WebSocketProxy
{
    public static string GetRemoteNodeUrl(ServerStore serverStore, string nodeTag)
    {
        ClusterTopology topology;

        using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            topology = serverStore.GetClusterTopology(context);
        }

        var remoteNodeUrl = topology.GetUrlFromTag(nodeTag);
        if (string.IsNullOrEmpty(remoteNodeUrl))
            throw new InvalidOperationException($"Could not find node url for node tag '{nodeTag}'");

        return remoteNodeUrl;
    }

    public static async ValueTask RelayAsync(ServerStore serverStore, WebSocket webSocket, string remoteNodeUrl, string remoteEndpointUrl, X509Certificate2 certificate,
        RavenLogger logger, CancellationToken token)
    {
        try
        {
            using (var connection = new ProxyWebSocketConnection(webSocket, remoteNodeUrl, remoteEndpointUrl, serverStore.ContextPool, token))
            {
                await connection.Establish(certificate);

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
        catch (Exception e)
        {
            await SendErrorAsync(serverStore, webSocket, e, logger);
        }
    }

    public static async ValueTask SendErrorAsync(ServerStore serverStore, WebSocket webSocket, Exception exception, RavenLogger logger)
    {
        try
        {
            using (serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var ms = RecyclableMemoryStreamFactory.GetRecyclableStream())
            {
                using (var writer = new BlittableJsonTextWriter(context, ms))
                {
                    context.Write(writer, new DynamicJsonValue { ["Type"] = "Error", ["Exception"] = exception.ToString() });
                }

                ms.TryGetBuffer(out ArraySegment<byte> bytes);
                await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, serverStore.ServerShutdown);
            }
        }
        catch (Exception)
        {
            if (logger.IsInfoEnabled)
                logger.Info("Failed to send the error of the web socket to the client", exception);
        }
    }
}
