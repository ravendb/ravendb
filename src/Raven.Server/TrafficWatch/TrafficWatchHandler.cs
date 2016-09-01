// ------------------------------------------------------------[-----------
//  <copyright file="ChangesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading.Tasks;
using Raven.Abstractions.Logging;
using Raven.Server.Routing;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.TrafficWatch
{
   public class TrafficWatchHandler : RequestHandler
    {
        private static readonly ILog Logger = LogManager.GetLogger(typeof(TrafficWatchHandler));

        [RavenAction("/traffic-watch/websockets", "GET", "/traffic-watch/websockets")]
        public async Task TrafficWatchWebsockets()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                JsonOperationContext context;
                using (ServerStore.ContextPool.AllocateOperationContext(out context))
                {
                    try
                    {
                        var resourceName = GetStringQueryString("resourceName", required: false);
                        var connection = new TrafficWatchConnection(webSocket, ServerStore.ServerShutdown,
                            "db/" + resourceName);
                        TrafficWatchManager.AddConnection(connection);
                        await connection.StartSendingNotifications();
                    }
                    catch (IOException)
                    {
                        // nothing to do - connection closed
                    }
                    catch (Exception ex)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Error encountered in TrafficWatch handler", ex);

                        try
                        {
                            using (var ms = new MemoryStream())
                            {
                                using (var writer = new BlittableJsonTextWriter(context, ms))
                                {
                                    context.Write(writer, new DynamicJsonValue
                                    {
                                        ["Exception"] = ex
                                    });
                                }

                                ArraySegment<byte> bytes;
                                ms.TryGetBuffer(out bytes);
                                await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, ServerStore.ServerShutdown);
                            }
                        }
                        catch (Exception)
                        {
                            if (Logger.IsInfoEnabled)
                                Logger.Info("Failed to send the error in TrafficWatch handler to the client", ex);
                        }
                    }
                }
            }
        }
    }
}