// ------------------------------------------------------------[-----------
//  <copyright file="ChangesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Server.Documents.Sharding.Changes;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedChangesHandler : ShardedDatabaseRequestHandler
    {
        private static readonly string StudioMarker = "fromStudio";

        [RavenShardedAction("/databases/*/changes", "GET")]
        public async Task GetChanges()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                using (var token = CreateOperationToken())
                using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    try
                    {
                        await HandleConnectionAsync(webSocket, context, token);
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
                            await using (var ms = new MemoryStream())
                            {
                                await using (var writer = new AsyncBlittableJsonTextWriter(context, ms))
                                {
                                    context.Write(writer, new DynamicJsonValue
                                    {
                                        ["Type"] = "Error",
                                        ["Exception"] = ex.ToString()
                                    });
                                }

                                ms.TryGetBuffer(out ArraySegment<byte> bytes);
                                await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, token.Token);
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

        private async ValueTask HandleConnectionAsync(WebSocket webSocket, JsonOperationContext context, OperationCancelToken token)
        {
            var fromStudio = GetBoolValueQueryString(StudioMarker, false) ?? false;
            var throttleConnection = GetBoolValueQueryString("throttleConnection", false).GetValueOrDefault(false);

            var connection = new ShardedChangesClientConnection(webSocket, ServerStore, DatabaseContext, fromStudio);
            DatabaseContext.Changes.Connect(connection);
            var sendTask = connection.StartSendingNotificationsAsync(throttleConnection);
            var debugTag = "changes/" + connection.Id;
            using (context.GetMemoryBuffer(out JsonOperationContext.MemoryBuffer segment1))
            using (context.GetMemoryBuffer(out JsonOperationContext.MemoryBuffer segment2))
            {
                try
                {
                    var segments = new[] { segment1, segment2 };
                    int index = 0;
                    var receiveAsync = webSocket.ReceiveAsync(segments[index].Memory.Memory, token.Token);
                    var jsonParserState = new JsonParserState();
                    using (var parser = new UnmanagedJsonParser(context, jsonParserState, debugTag))
                    {
                        connection.SendSupportedFeatures();

                        var result = await receiveAsync;
                        token.Token.ThrowIfCancellationRequested();

                        parser.SetBuffer(segments[index], 0, result.Count);
                        index++;
                        receiveAsync = webSocket.ReceiveAsync(segments[index].Memory.Memory, token.Token);

                        while (true)
                        {
                            using (var builder =
                                new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, debugTag, parser, jsonParserState))
                            {
                                parser.NewDocument();
                                builder.ReadObjectDocument();

                                while (builder.Read() == false)
                                {
                                    result = await receiveAsync;
                                    token.Token.ThrowIfCancellationRequested();

                                    parser.SetBuffer(segments[index], 0, result.Count);
                                    if (++index >= segments.Length)
                                        index = 0;
                                    receiveAsync = webSocket.ReceiveAsync(segments[index].Memory.Memory, token.Token);
                                }

                                builder.FinalizeDocument();

                                using (var reader = builder.CreateReader())
                                {
                                    if (reader.TryGet("Command", out string command) == false)
                                        throw new ArgumentNullException(nameof(command), "Command argument is mandatory");

                                    reader.TryGet("Param", out string commandParameter);
                                    reader.TryGet("Params", out BlittableJsonReaderArray commandParameters);

                                    await connection.HandleCommandAsync(command, commandParameter, commandParameters);

                                    if (reader.TryGet("CommandId", out int commandId))
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
                catch (Exception ex)
                {
#pragma warning disable 4014
                    sendTask.IgnoreUnobservedExceptions();
#pragma warning restore 4014

                    // if we received close from the client, we want to ignore it and close the websocket (dispose does it)
                    if (ex is WebSocketException webSocketException
                        && webSocketException.WebSocketErrorCode == WebSocketError.InvalidState
                        && webSocket.State == WebSocketState.CloseReceived)
                    {
                        // ignore
                    }
                    else
                    {
                        throw;
                    }
                }
                finally
                {
                    DatabaseContext.Changes.Disconnect(connection.Id);
                }
            }

            token.Token.ThrowIfCancellationRequested();

            await sendTask;
        }
    }
}
