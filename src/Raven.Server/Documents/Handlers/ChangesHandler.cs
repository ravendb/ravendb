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
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class ChangesHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/changes", "GET", "/databases/{databaseName:string}/changes", SkipUsagesCount = true)]
        public async Task GetChanges()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                //TODO: select small context size (maybe pool just for them?)
                JsonOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                {
                    try
                    {
                        await HandleConnection(webSocket, context);
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
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Connections");

                writer.WriteStartArray();
                var first = true;
                foreach (var connection in Database.Changes.Connections)
                {
                    if (first == false)
                        writer.WriteComma();
                    first = false;
                    context.Write(writer, connection.Value.GetDebugInfo());
                }
                writer.WriteEndArray();

                writer.WriteEndObject();
            }
            return Task.CompletedTask;
        }

        private async Task HandleConnection(WebSocket webSocket, JsonOperationContext context)
        {
            var throttleConnection = GetBoolValueQueryString("throttleConnection", false).GetValueOrDefault(false);

            var connection = new ChangesClientConnection(webSocket, Database);
            Database.Changes.Connect(connection);
            var sendTask = connection.StartSendingNotifications(throttleConnection);
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
                        Database.DatabaseShutdown.ThrowIfCancellationRequested();

                        parser.SetBuffer(segments[index], 0, result.Count);
                        index++;
                        receiveAsync = webSocket.ReceiveAsync(segments[index].Buffer, Database.DatabaseShutdown);

                        while (true)
                        {
                            using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, debugTag, parser, jsonParserState))
                            {
                                parser.NewDocument();
                                builder.ReadObjectDocument();

                                while (builder.Read() == false)
                                {
                                    result = await receiveAsync;
                                    Database.DatabaseShutdown.ThrowIfCancellationRequested();

                                    parser.SetBuffer(segments[index], 0, result.Count);
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
                    Database.Changes.Disconnect(connection.Id);
                }
            }
            await sendTask;
        }

        [RavenAction("/databases/*/changes", "DELETE", "/databases/{databaseName:string}/changes?id={connectionId:long|multiple}")]
        public Task DeleteConnections()
        {
            var ids = GetStringValuesQueryString("id");

            foreach (var idStr in ids)
            {
                long id;
                if (long.TryParse(idStr, NumberStyles.Any, CultureInfo.InvariantCulture, out id) == false)
                    throw new ArgumentException($"Could not parse query string 'id' header as int64, value was: {idStr}");

                Database.Changes.Disconnect(id);
            }

            return NoContent();
        }
    }
}