using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Extensions;
using Raven.Server.Documents.Changes;
using Raven.Server.Extensions;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Changes;

internal abstract class AbstractChangesHandlerProcessorForGetChanges<TRequestHandler, TOperationContext, TChangesClientConnection> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TChangesClientConnection : AbstractChangesClientConnection<TOperationContext>
{
    protected AbstractChangesHandlerProcessorForGetChanges([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract TChangesClientConnection CreateChangesClientConnection(WebSocket webSocket, bool throttleConnection, bool fromStudio);

    protected abstract void Connect(TChangesClientConnection connection);

    protected abstract void Disconnect(TChangesClientConnection connection);

    public override async ValueTask ExecuteAsync()
    {
        using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
        {
            var fromStudio = RequestHandler.HttpContext.Request.IsFromStudio();
            var throttleConnection = RequestHandler.GetBoolValueQueryString("throttleConnection", false).GetValueOrDefault(false);

            var connection = CreateChangesClientConnection(webSocket, throttleConnection, fromStudio);
            
            using (var token = RequestHandler.CreateHttpRequestBoundOperationToken(connection.DisposeToken))
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                try
                {
                    await HandleConnectionAsync(webSocket, connection, context, token);
                }
                catch (OperationCanceledException)
                {
                }
                catch (TimeoutException e)
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations("Timeout in changes handler", e);
                }
                catch (Exception ex)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Error encountered in changes handler", ex);

                    try
                    {
                        if (webSocket.State != WebSocketState.Open)
                            return;

                        await using (var ms = RecyclableMemoryStreamFactory.GetRecyclableStream())
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
                    catch (ObjectDisposedException)
                    {
                        // disposing
                    }
                    catch (Exception exception)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Failed to send the error in changes handler to the client", exception);
                    }
                }
            }
        }
    }

    private async Task HandleConnectionAsync(
        WebSocket webSocket, 
        TChangesClientConnection connection, 
        JsonOperationContext context,
        OperationCancelToken token)
    {
       
        Connect(connection);

        var sendTask = connection.StartSendingNotificationsAsync();
        var debugTag = "changes/" + connection.Id;
        using (context.GetMemoryBuffer(out JsonOperationContext.MemoryBuffer segment1))
        using (context.GetMemoryBuffer(out JsonOperationContext.MemoryBuffer segment2))
        {
            try
            {
                var segments = new[] { segment1, segment2 };
                int index = 0;
#pragma warning disable CA2012
                var receiveAsync = webSocket.ReceiveAsync(segments[index].Memory.Memory, token.Token);
#pragma warning restore CA2012
                var jsonParserState = new JsonParserState();
                using (var parser = new UnmanagedJsonParser(context, jsonParserState, debugTag))
                {
                    connection.SendSupportedFeatures();

                    var result = await receiveAsync;
                    token.Token.ThrowIfCancellationRequested();

                    parser.SetBuffer(segments[index], 0, result.Count);
                    index++;
#pragma warning disable CA2012
                    receiveAsync = webSocket.ReceiveAsync(segments[index].Memory.Memory, token.Token);
#pragma warning restore CA2012

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
#pragma warning disable CA2012
                                receiveAsync = webSocket.ReceiveAsync(segments[index].Memory.Memory, token.Token);
#pragma warning restore CA2012
                            }

                            builder.FinalizeDocument();

                            using (var reader = builder.CreateReader())
                            {
                                if (reader.TryGet("Command", out string command) == false)
                                    throw new ArgumentNullException(nameof(command), "Command argument is mandatory");

                                reader.TryGet("Param", out string commandParameter);
                                reader.TryGet("Params", out BlittableJsonReaderArray commandParameters);

                                using (var commandToken = RequestHandler.CreateHttpRequestBoundTimeLimitedOperationToken(TimeSpan.FromSeconds(30)))
                                    await connection.HandleCommandAsync(command, commandParameter, commandParameters, commandToken.Token);

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
                else if (ex is OperationCanceledException)
                {
                    await sendTask; // will throw if the task is faulted
                    throw;
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
