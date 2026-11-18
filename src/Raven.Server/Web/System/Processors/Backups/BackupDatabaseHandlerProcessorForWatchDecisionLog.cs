using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Backups;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.Backups;

internal sealed class BackupDatabaseHandlerProcessorForWatchDecisionLog : AbstractServerHandlerWebSocketProxyProcessor
{
    private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(5);

    private const int DefaultPageSize = 512;

    private const int MaxPageSize = 4096;

    public BackupDatabaseHandlerProcessorForWatchDecisionLog([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override string GetRemoteEndpointUrl()
    {
        var queryString = QueryString.Create(HttpContext.Request.Query.Where(x => string.Equals(x.Key, Constants.QueryString.NodeTag, StringComparison.OrdinalIgnoreCase) == false));

        return $"/admin/periodic-backup/decision-log/watch{queryString.ToUriComponent()}";
    }

    protected override async ValueTask HandleCurrentNodeAsync(WebSocket webSocket, OperationCancelToken token)
    {
        var decisions = ServerStore.ServerBackupRunner.Decisions;
        var filter = GetFilter();
        var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
        var receive = webSocket.ReceiveAsync(receiveBuffer, token.Token);

        var lastEtag = decisions.LastDecisionEtag;
        var lastSummaryVersion = decisions.SummaryVersion;
        var lastDecisionId = decisions.LastDecisionId;

        var snapshot = decisions.GetDecisionLogDetails(filter, afterEtag: null);
        var totalResults = snapshot.TotalResults;
        lastDecisionId = Math.Max(lastDecisionId, GetMaxId(snapshot.Entries));

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

            var currentEtag = decisions.LastDecisionEtag;
            var currentSummaryVersion = decisions.SummaryVersion;
            var details = decisions.GetDecisionLogDetails(filter, afterEtag: lastEtag);

            var summaryChanged = currentSummaryVersion != lastSummaryVersion;
            lastEtag = currentEtag;
            lastSummaryVersion = currentSummaryVersion;

            if (details.Entries.Count == 0 && summaryChanged == false)
                continue;

            var maxId = lastDecisionId;
            foreach (var entry in details.Entries)
            {
                if (entry.Id > lastDecisionId)
                    totalResults++;

                maxId = Math.Max(maxId, entry.Id);
            }

            lastDecisionId = maxId;
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

    private static long GetMaxId(List<BackupDecisionLogEntry> entries)
    {
        long maxId = 0;

        foreach (var entry in entries)
            maxId = Math.Max(maxId, entry.Id);

        return maxId;
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
}
