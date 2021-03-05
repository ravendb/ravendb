// -----------------------------------------------------------------------
//  <copyright file="ClusterDashboardHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter.Handlers
{
    public class ClusterDashboardHandler : ServerNotificationHandlerBase
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<ClusterDashboardHandler>("Server");

        [RavenAction("/cluster-dashboard/watch", "GET", AuthorizationStatus.ValidUser, SkipUsagesCount = true, DisableOnCpuCreditsExhaustion = true)]
        public async Task Get()
        {
            //TODO: access control
            //TODO: check for param - withProxy?
            
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext writeContext))
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext readContext))
                {
                    try
                    {
                        using (var connection = new ClusterDashboardConnection(webSocket, writeContext, readContext, ServerStore.ServerShutdown))
                        {
                            await connection.Handle();
                        }
                    }
                    catch (OperationCanceledException)
                    {

                    }
                    catch (Exception ex)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Error encountered in cluster dashboard handler", ex);

                        try
                        {
                            using (var ms = new MemoryStream())
                            {
                                using (var writer = new BlittableJsonTextWriter(writeContext, ms))
                                {
                                    writeContext.Write(writer, new DynamicJsonValue
                                    {
                                        ["Type"] = "Error",
                                        ["Exception"] = ex.ToString()
                                    });
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
        }
    }
}
