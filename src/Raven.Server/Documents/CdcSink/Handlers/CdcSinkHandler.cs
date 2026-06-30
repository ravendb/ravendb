using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents.CdcSink.Handlers.Processors;
using Raven.Server.Documents.CdcSink.Stats.Performance;
using Raven.Server.Json;
using Raven.Server.Routing;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.CdcSink.Handlers;

public class CdcSinkHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/admin/cdc-sink/test", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task PostScriptTest()
    {
        using (var processor = new CdcSinkHandlerProcessorForTest(this))
            await processor.ExecuteAsync();
    }

    [RavenAction("/databases/*/admin/cdc-sink/schema", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task PostSchema()
    {
        using (var processor = new CdcSinkHandlerProcessorForSchema(this))
            await processor.ExecuteAsync();
    }

    [RavenAction("/databases/*/cdc-sink/performance", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task Performance()
    {
        var sinks = GetProcessesToReportOn();

        var stats = new List<CdcSinkTaskPerformanceStats>(sinks.Count);
        foreach (var kvp in sinks)
        {
            var processPerformanceStats = new List<CdcSinkProcessPerformanceStats>(kvp.Value.Count);
            foreach (var process in kvp.Value)
            {
                processPerformanceStats.Add(new CdcSinkProcessPerformanceStats
                {
                    Performance = process.GetPerformanceStats()
                });
            }

            stats.Add(new CdcSinkTaskPerformanceStats
            {
                TaskId = kvp.Value[0].TaskId,
                TaskName = kvp.Key,
                Stats = processPerformanceStats.ToArray()
            });
        }

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
        {
            writer.WriteCdcSinkTaskPerformanceStats(context, stats);
        }
    }

    [RavenAction("/databases/*/cdc-sink/performance/live", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true)]
    public async Task PerformanceLive()
    {
        using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
        {
            var sinks = GetProcessesToReportOn();

            var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
            var receive = webSocket.ReceiveAsync(receiveBuffer, Database.DatabaseShutdown);

            await using (var ms = RecyclableMemoryStreamFactory.GetRecyclableStream())
            using (var collector = new LiveCdcSinkPerformanceCollector(Database, sinks))
            {
                await collector.SendStatsOrHeartbeatToWebSocket(receive, webSocket, ContextPool, ms, 100);

                while (Database.DatabaseShutdown.IsCancellationRequested == false)
                {
                    if (await collector.SendStatsOrHeartbeatToWebSocket(receive, webSocket, ContextPool, ms, 4000) == false)
                    {
                        break;
                    }
                }
            }
        }
    }

    private SortedDictionary<string, List<CdcSinkProcess>> GetProcessesToReportOn()
    {
        var names = GetStringValuesQueryString("name", required: false);
        var sinks = new SortedDictionary<string, List<CdcSinkProcess>>(StringComparer.OrdinalIgnoreCase);

        foreach (var process in Database.CdcSinkLoader.Processes)
        {
            if (names.Count > 0 &&
                names.Contains(process.Configuration.Name, StringComparer.OrdinalIgnoreCase) == false &&
                names.Contains(process.Name, StringComparer.OrdinalIgnoreCase) == false)
            {
                continue;
            }

            if (sinks.TryGetValue(process.Configuration.Name, out var list) == false)
            {
                list = new List<CdcSinkProcess>();
                sinks[process.Configuration.Name] = list;
            }

            list.Add(process);
        }

        foreach (var list in sinks.Values)
        {
            list.Sort((x, y) => string.Compare(x.Name, y.Name, StringComparison.Ordinal));
        }

        return sinks;
    }
}
