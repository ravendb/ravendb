// ------------------------------------------------------------[-----------
//  <copyright file="ChangesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Globalization;
using System.IO;
using System.Net.WebSockets;
using System.Threading.Tasks;
using Raven.Abstractions.Logging;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers
{
    public class ChangesHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/changes", "GET", "/databases/{databaseName:string}/changes")]
        public async Task GetChanges()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                // this flag can be used to detect if server was restarted between changes connections on client side
                var sendStartTime = GetBoolValueQueryString("sendServerStartTime", false).GetValueOrDefault(false);

                //TODO: select small context size (maybe pool just for them?)
                JsonOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                {
                    try
                    {
                        await HandleConnection(webSocket, context, sendStartTime);
                    }
                    catch (OperationCanceledException)
                    {

                    }
                    catch (Exception ex)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Error encountered in changes handler", ex);

                        try
                        {
                            using (var ms = new MemoryStream())
                            {
                                using (var writer = new BlittableJsonTextWriter(context, ms))
                                {
                                    context.Write(writer, new DynamicJsonValue
                                    {
                                        ["Type"] = "Error",
                                        ["Exception"] = ex.ToString(),
                                    });
                                }

                                ArraySegment<byte> bytes;
                                ms.TryGetBuffer(out bytes);
                                await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, Database.DatabaseShutdown);
                            }
                        }
                        catch (Exception)
                        {
                            if (Logger.IsInfoEnabled)
                                Logger.Info("Failed to send the error in changes handler to the client", ex);
                        }
                    }
                }
            }
        }

        [RavenAction("/databases/*/changes/debug", "GET", "/databases/{databaseName:string}/changes/debug")]
        public Task GetConnectionsDebugInfo()
        {
            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartArray();
                var first = true;
                foreach (var connection in Database.Notifications.Connections)
                {
                    if (first == false)
                        writer.WriteComma();
                    first = false;
                    context.Write(writer, connection.Value.GetDebugInfo());
                }
                writer.WriteEndArray();
            }
            return Task.CompletedTask;
        }

        private async Task HandleConnection(WebSocket webSocket, JsonOperationContext context, bool sendStartTime)
        {
            var connection = new NotificationsClientConnection(webSocket, Database);
            Database.Notifications.Connect(connection);
            var sendTask = connection.StartSendingNotifications(sendStartTime);
            var debugTag = "changes/" + connection.Id;
            JsonOperationContext.ManagedPinnedBuffer segment1,segment2;
            using (context.GetManagedBuffer(out segment1))
            using (context.GetManagedBuffer(out segment2))
            {
                try
                {
                    var segments = new[]{segment1,segment2};
                    int index = 0;
                    var receiveAsync = webSocket.ReceiveAsync(segments[index].Buffer, Database.DatabaseShutdown);
                    var jsonParserState = new JsonParserState();
                    using (var parser = new UnmanagedJsonParser(context, jsonParserState, debugTag))
                    {
                        var result = await receiveAsync;
                        parser.SetBuffer(segments[index], result.Count);
                        index++;
                        receiveAsync = webSocket.ReceiveAsync(segments[index].Buffer, Database.DatabaseShutdown);

                        while (true)
                        {
                            using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, debugTag, parser, jsonParserState))
                            {
                                parser.NewDocument();
                                builder.ReadObject();

                                while (builder.Read() == false)
                                {
                                    result = await receiveAsync;

                                    parser.SetBuffer(segments[index], result.Count);
                                    if (++index >= segments.Length)
                                        index = 0;
                                    receiveAsync = webSocket.ReceiveAsync(segments[index].Buffer, Database.DatabaseShutdown);
                                }

                                builder.FinalizeDocument();

                                using (var reader = builder.CreateReader())
                                {
                                    string command, commandParameter;
                                    if (reader.TryGet("Command", out command) == false)
                                        throw new ArgumentNullException(nameof(command), "Command argument is mandatory");

                                    reader.TryGet("Param", out commandParameter);
                                    connection.HandleCommand(command, commandParameter);

                                    int commandId;
                                    if (reader.TryGet("CommandId", out commandId))
                                    {
                                        connection.Confirm(commandId);
                                    }
                                }
                            }
                        }
                    }
                }
                catch (IOException ex)
                {
                    /* Client was disconnected, write to log */
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Client was disconnected", ex);
                }
                finally
                {
                    Database.Notifications.Disconnect(connection.Id);
                }
            }
            await sendTask;
        }

        [RavenAction("/databases/*/changes", "DELETE", "/databases/{databaseName:string}/changes?id={connectionId:long|multiple}")]
        public Task DeleteConnections()
        {
            var ids = HttpContext.Request.Query["id"];
            if (ids.Count == 0)
                throw new ArgumentException($"Query string 'id' is mandatory, but wasn't specified");

            foreach (var idStr in ids)
            {
                long id;
                if (long.TryParse(idStr, NumberStyles.Any, CultureInfo.InvariantCulture, out id) == false)
                    throw new ArgumentException($"Could not parse query string 'id' header as int64, value was: {idStr}");

                Database.Notifications.Disconnect(id);
            }

            return Task.CompletedTask;
        }
    }
}