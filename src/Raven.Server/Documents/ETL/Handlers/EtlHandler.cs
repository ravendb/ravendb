using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Handlers
{
    public class EtlHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/etl/stats", "GET", AuthorizationStatus.ValidUser)]
        public Task GetStats()
        {
            var etlStats = GetProcessesToReportOn().Select(x => new EtlTaskStats
            {
                TaskName = x.Key,
                Stats = x.Value.Select(y => new EtlProcessTransformationStats
                {
                    TransformationName = y.TransformationName,
                    Statistics = y.Statistics
                }).ToArray()
            }).ToArray();

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WriteArray(context, "Results", etlStats, (w, c, stats) =>
                    {
                        w.WriteObject(context.ReadObject(stats.ToJson(), "etl/stats"));
                    });
                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/etl/debug/stats", "GET", AuthorizationStatus.ValidUser)]
        public Task GetDebugStats()
        {
            var debugStats = GetProcessesToReportOn().Select(x => new DynamicJsonValue()
            {
                ["TaskName"] = x.Key,
                ["Stats"] = x.Value.Select(y =>
                {
                    var stats = new EtlProcessTransformationStats
                    {
                        TransformationName = y.TransformationName,
                        Statistics = y.Statistics
                    }.ToJson();

                    stats[nameof(y.Metrics)] = y.Metrics.ToJson();

                    return stats;
                }).ToArray()
            }).ToArray();

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WriteArray(context, "Results", debugStats, (w, c, stats) =>
                    {
                        w.WriteObject(context.ReadObject(stats, "etl/debug/stats"));
                    });
                    writer.WriteEndObject();
                }
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/etl/performance", "GET", AuthorizationStatus.ValidUser)]
        public Task Performance()
        {
            var stats = GetProcessesToReportOn().Select(x => new EtlTaskPerformanceStats
            {
                TaskName = x.Key,
                TaskId = x.Value.First().TaskId, // since we grouped by task name it implies each task id inside group is the same
                EtlType = x.Value.First().EtlType,
                Stats = x.Value.Select(y => new EtlProcessPerformanceStats
                {
                    TransformationName = y.TransformationName,
                    Performance = y.GetPerformanceStats()
                }).ToArray()
            }).ToArray();

            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteEtlTaskPerformanceStats(context, stats);
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/etl/performance/live", "GET", AuthorizationStatus.ValidUser, SkipUsagesCount = true)]
        public async Task PerformanceLive()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                var etls = GetProcessesToReportOn();

                var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
                var receive = webSocket.ReceiveAsync(receiveBuffer, Database.DatabaseShutdown);

                using (var ms = new MemoryStream())
                using (var collector = new LiveEtlPerformanceCollector(Database, etls))
                {
                    // 1. Send data to webSocket without making UI wait upon opening webSocket
                    await collector.SendStatsOrHeartbeatToWebSocket(receive, webSocket, ContextPool, ms, 100);

                    // 2. Send data to webSocket when available
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

        [RavenAction("/databases/*/etl/progress", "GET", AuthorizationStatus.ValidUser)]
        public Task Progress()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                var performance = GetProcessesToReportOn().Select(x => new EtlTaskProgress
                {
                    TaskName = x.Key,
                    EtlType = x.Value.First().EtlType,
                    ProcessesProgress = x.Value.Select(y => y.GetProgress(context)).ToArray()
                }).ToArray();

                writer.WriteEtlTaskProgress(context, performance);
            }

            return Task.CompletedTask;
        }

        private Dictionary<string, List<EtlProcess>> GetProcessesToReportOn()
        {
            Dictionary<string, List<EtlProcess>> etls;
            var names = HttpContext.Request.Query["name"];

            if (names.Count == 0)
                etls = Database.EtlLoader.Processes
                    .GroupBy(x => x.ConfigurationName)
                    .OrderBy(x => x.Key)
                    .ToDictionary(x => x.Key, x => x.OrderBy(y => y.TransformationName).ToList());
            else
            {
                etls = Database.EtlLoader.Processes
                    .Where(x => names.Contains(x.ConfigurationName, StringComparer.OrdinalIgnoreCase) || names.Contains(x.Name, StringComparer.OrdinalIgnoreCase))
                    .GroupBy(x => x.ConfigurationName)
                    .OrderBy(x => x.Key)
                    .ToDictionary(x => x.Key, x => x.OrderBy(y => y.TransformationName).ToList());
            }

            return etls;
        }
    }
}
