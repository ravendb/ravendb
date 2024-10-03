// -----------------------------------------------------------------------
//  <copyright file="ClusterDashboardHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Dashboard;
using Raven.Server.Dashboard.Cluster;
using Raven.Server.Json;
using Raven.Server.Logging;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter.Handlers
{
    public sealed class ClusterDashboardHandler : ServerNotificationHandlerBase
    {
        private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer<ClusterDashboardHandler>();

        [RavenAction("/cluster-dashboard/watch", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true)]
        public async Task Watch()
        {
            var nodeTag = GetStringQueryString("node", required: true);

            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                try
                {
                    if (nodeTag.Equals(ServerStore.NodeTag, StringComparison.OrdinalIgnoreCase))
                    {
                        var canAccessDatabase = GetDatabaseAccessValidationFunc();

                        await SendNotifications(canAccessDatabase, webSocket);
                    }
                    else
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

                        var currentCert = GetCurrentCertificate();

                        using (var connection = new ProxyWebSocketConnection(webSocket, remoteNodeUrl, $"/admin/cluster-dashboard/remote/watch?thumbprint={currentCert?.Thumbprint}", ServerStore.ContextPool, ServerStore.ServerShutdown))
                        {
                            await connection.Establish(Server.Certificate?.Certificate);

                            await connection.RelayData();
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // ignored 
                }
                catch (Exception ex)
                {
                    await HandleException(ex, webSocket);
                }
            }
        }

        [RavenAction("/admin/cluster-dashboard/remote/watch", "GET", AuthorizationStatus.ClusterAdmin, SkipUsagesCount = true)]
        public async Task RemoteWatch()
        {
            var thumbprint = GetStringQueryString("thumbprint", required: false);

            CertificateDefinition clientConnectedCertificate = null;

            var canAccessDatabase = GetDatabaseAccessValidationFunc();

            var currentCertificate = GetCurrentCertificate();

            if (string.IsNullOrEmpty(thumbprint) == false && currentCertificate != null && thumbprint != currentCertificate.Thumbprint)
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                {
                    using (ctx.OpenReadTransaction())
                    {
                        var certByThumbprint = ServerStore.Cluster.GetCertificateByThumbprint(ctx, thumbprint) ?? ServerStore.Cluster.GetLocalStateByThumbprint(ctx, thumbprint);

                        if (certByThumbprint != null)
                        {
                            clientConnectedCertificate = JsonDeserializationServer.CertificateDefinition(certByThumbprint);
                        }
                    }

                    if (clientConnectedCertificate != null)
                    {
                        // we're already connected as ClusterAdmin, here we're just limiting the access to databases based on the thumbprint of the originally connected certificated
                        // so we'll send notifications only about relevant databases

                        var authenticationStatus = new RavenServer.AuthenticateConnection(Server.TwoFactor);

                        authenticationStatus.SetBasedOnCertificateDefinition(clientConnectedCertificate);

                        canAccessDatabase = GetDatabaseAccessValidationFunc(authenticationStatus);
                    }
                }
            }

            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                try
                {
                    await SendNotifications(canAccessDatabase, webSocket);
                }
                catch (OperationCanceledException)
                {
                    // ignored 
                }
                catch (Exception ex)
                {
                    await HandleException(ex, webSocket);
                }
            }
        }

        private async Task SendNotifications(CanAccessDatabase canAccessDatabase, WebSocket webSocket)
        {
            using (var notifications = new ClusterDashboardNotifications(Server, canAccessDatabase, ServerStore.ServerShutdown))
            using (var connection = new ClusterDashboardConnection<TransactionOperationContext>(webSocket, canAccessDatabase, notifications, ServerStore.ContextPool, ServerStore.ServerShutdown))
            {
                await connection.Handle();
            }
        }

        private async Task HandleException(Exception ex, WebSocket webSocket)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info("Error encountered in cluster dashboard handler", ex);

            try
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                using (var ms = new MemoryStream())
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
}
