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
                        await HandleConnection(webSocket, context);
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

        private async Task HandleConnection(WebSocket webSocket, JsonOperationContext context)
        {
            var debugTag = "traffic-watch/websocket";
            string id = "N/A";
            TrafficWatchConnection connection = null;
            try
            {
                var buffer = context.GetManagedBuffer();
                var segments = new[]
                {
                    new ArraySegment<byte>(buffer, 0, buffer.Length/2),
                    new ArraySegment<byte>(buffer, buffer.Length/2, buffer.Length/2)
                };
                int index = 0;
                var receiveAsync = webSocket.ReceiveAsync(segments[index], ServerStore.ServerShutdown);
                var jsonParserState = new JsonParserState();
                using (var parser = new UnmanagedJsonParser(context, jsonParserState, debugTag))
                {
                    var result = await receiveAsync;
                    parser.SetBuffer(new ArraySegment<byte>(segments[index].Array, segments[index].Offset, result.Count));
                    index++;
                    receiveAsync = webSocket.ReceiveAsync(segments[index], ServerStore.ServerShutdown);

                    while (true)
                    {
                        using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, debugTag, parser, jsonParserState))
                        {
                            builder.ReadObject();

                            while (builder.Read() == false)
                            {
                                result = await receiveAsync;

                                parser.SetBuffer(new ArraySegment<byte>(segments[index].Array, segments[index].Offset, result.Count));
                                if (++index >= segments.Length)
                                    index = 0;
                                receiveAsync = webSocket.ReceiveAsync(segments[index], ServerStore.ServerShutdown);
                            }

                            builder.FinalizeDocument();

                            using (var reader = builder.CreateReader())
                            {
                                string token;
                                if (reader.TryGet("Token", out token) == false)
                                    throw new ArgumentNullException(nameof(token), "Token argument is mandatory");
                                if (reader.TryGet("Id", out id) == false)
                                    throw new ArgumentNullException(nameof(id), "Id argument is mandatory");
                                int timeout;
                                if (reader.TryGet("Timeout", out timeout) == false)
                                    throw new ArgumentNullException(nameof(id), "Timeout argument is mandatory");

                                // TODO (TrafficWatch) : Validate Token, (Uri?, ActiveSource?, User/ApiKey?).  

                                string resourceName;
                                if (reader.TryGet("ResourceName", out resourceName) == false ||
                                    resourceName.Equals("N/A"))
                                {
                                    resourceName = null;
                                }

                                connection = new TrafficWatchConnection(webSocket, id, ServerStore.ServerShutdown, resourceName, timeout);
                                TrafficWatchManager.AddConnection(connection);
                                await connection.StartSendingNotifications();
                            }
                        }
                    }
                }
            }
            catch (IOException ex)
            {
                /* Client was disconnected, write to log */
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Client was disconnected during TrafficWatch session Id={id}", ex);
                if (connection != null)
                    TrafficWatchManager.Disconnect(connection);
            }
        }
    }
}