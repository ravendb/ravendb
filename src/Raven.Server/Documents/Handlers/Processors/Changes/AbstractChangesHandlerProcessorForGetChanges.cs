using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Extensions;
using Raven.Server.Documents.Changes;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Changes;

internal abstract class AbstractChangesHandlerProcessorForGetChanges<TRequestHandler, TOperationContext, TChangesClientConnection> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TChangesClientConnection : AbstractChangesClientConnection<TOperationContext>
{
    private const string StudioMarker = "fromStudio";

    protected AbstractChangesHandlerProcessorForGetChanges([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract TChangesClientConnection CreateChangesClientConnection(WebSocket webSocket, bool fromStudio);

    protected abstract void Connect(TChangesClientConnection connection);

    protected abstract void Disconnect(TChangesClientConnection connection);

    public override async ValueTask ExecuteAsync()
    {
        using (var token = RequestHandler.CreateOperationToken())
        using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
        {
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

    private async Task HandleConnectionAsync(WebSocket webSocket, JsonOperationContext context, OperationCancelToken token)
    {
        var fromStudio = RequestHandler.GetBoolValueQueryString(StudioMarker, false) ?? false;
        var throttleConnection = RequestHandler.GetBoolValueQueryString("throttleConnection", false).GetValueOrDefault(false);

        var connection = CreateChangesClientConnection(webSocket, fromStudio);
        Connect(connection);

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
                        using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, debugTag, parser, jsonParserState))
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
                Disconnect(connection);
            }
        }

        token.Token.ThrowIfCancellationRequested();

        await sendTask;
    }

}
