using System;
using System.Linq;
using System.Net.WebSockets;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Logging;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Backups;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Web.System.Processors.Backups;

internal sealed class BackupDatabaseHandlerProcessorForWatchDecisionLog : AbstractHandlerProcessor<RequestHandler>
{
    private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer<BackupDatabaseHandlerProcessorForWatchDecisionLog>();

    private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(5);

    private const int DefaultPageSize = 512;

    private const int MaxPageSize = 4096;

    public BackupDatabaseHandlerProcessorForWatchDecisionLog([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
        using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
        using (ServerStore.ServerBackupRunner.Decisions.StartWatching())
        {
            try
            {
                await WatchAsync(webSocket, token);
            }
            catch (OperationCanceledException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
            catch (WebSocketException)
            {
            }
            catch (Exception e)
            {
                await SendErrorAsync(webSocket, e);
            }
        }
    }

    private async ValueTask WatchAsync(WebSocket webSocket, OperationCancelToken token)
    {
        var decisions = ServerStore.ServerBackupRunner.Decisions;
        var filter = GetFilter();
        var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
        var receive = webSocket.ReceiveAsync(receiveBuffer, token.Token);

        var lastSummaryVersion = decisions.SummaryVersion;

        var snapshot = decisions.GetDecisionLogDetails(filter, afterEtag: null);
        var lastEtag = snapshot.LastEtag;
        var lastId = snapshot.LastId;
        var totalResults = snapshot.TotalResults;

        await SendAsync(webSocket, "Snapshot", snapshot, token);

        while (token.Token.IsCancellationRequested == false)
        {
            var changed = decisions.WaitForChangeAsync(lastEtag, lastSummaryVersion);
            await Task.WhenAny(receive, changed, Task.Delay(HeartbeatInterval, token.Token));

            if (receive.IsCompleted)
                break;

            if (changed.IsCompleted == false)
            {
                await webSocket.SendAsync(WebSocketHelper.Heartbeat, WebSocketMessageType.Text, true, token.Token);
                continue;
            }

            var currentSummaryVersion = decisions.SummaryVersion;
            var details = decisions.GetDecisionLogDetails(filter, afterEtag: lastEtag, afterId: lastId);

            var summaryChanged = currentSummaryVersion != lastSummaryVersion;
            lastEtag = details.LastEtag;
            lastId = details.LastId;
            lastSummaryVersion = currentSummaryVersion;
            totalResults += details.TotalResults;

            if (details.Entries.Count == 0 && summaryChanged == false)
                continue;

            details.TotalResults = totalResults;

            await SendAsync(webSocket, "Update", details, token);
        }
    }

    private BackupDecisionLogFilter GetFilter()
    {
        return new BackupDecisionLogFilter
        {
            Databases = RequestHandler.GetStringValuesQueryString("database", required: false).ToHashSet(StringComparer.OrdinalIgnoreCase),
            TaskIds = RequestHandler.GetStringValuesQueryString("taskId", required: false).Select(BackupDecisionLogFilter.ParseTaskId).ToHashSet(),
            Categories = RequestHandler.GetStringValuesQueryString("category", required: false).Select(BackupDecisionLogFilter.ParseCategory).ToHashSet(),
            Reason = RequestHandler.GetStringQueryString("reason", required: false),
            From = RequestHandler.GetDateTimeQueryString("from", required: false),
            To = RequestHandler.GetDateTimeQueryString("to", required: false),
            Take = Math.Clamp(RequestHandler.GetIntValueQueryString("take", required: false) ?? DefaultPageSize, 0, MaxPageSize)
        };
    }

    private async Task SendAsync(WebSocket webSocket, string type, BackupDecisionLogDetails details, OperationCancelToken token)
    {
        using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var ms = RecyclableMemoryStreamFactory.GetRecyclableStream())
        {
            var json = details.ToJson();
            json["Type"] = type;

            await using (var writer = new AsyncBlittableJsonTextWriter(context, ms))
            {
                context.Write(writer, json);
            }

            ms.TryGetBuffer(out ArraySegment<byte> bytes);
            await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, token.Token);
        }
    }

    private async ValueTask SendErrorAsync(WebSocket webSocket, Exception exception)
    {
        try
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var ms = RecyclableMemoryStreamFactory.GetRecyclableStream())
            {
                using (var writer = new BlittableJsonTextWriter(context, ms))
                {
                    context.Write(writer, new DynamicJsonValue { ["Type"] = "Error", ["Exception"] = exception.ToString() });
                }

                ms.TryGetBuffer(out ArraySegment<byte> bytes);
                await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, ServerStore.ServerShutdown);
            }
        }
        catch (Exception)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info("Failed to send the error of the backup decision log watch to the client", exception);
        }
    }
}
