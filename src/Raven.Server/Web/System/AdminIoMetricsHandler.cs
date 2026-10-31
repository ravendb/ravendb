using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Utils.IoMetrics;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Platform;
using Voron;
using Voron.Impl.Paging;

namespace Raven.Server.Web.System
{
    public sealed class AdminIoMetricsHandler : ServerRequestHandler
    {
        [RavenAction("/admin/debug/io-metrics", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task IoMetrics()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
            {
                var result = IoMetricsUtil.GetIoMetricsResponse(GetSystemEnvironment(ServerStore), null);
                context.Write(writer, result.ToJson());
            }
        }

        [RavenAction("/admin/debug/io-metrics/prefetch", "GET", AuthorizationStatus.Operator, IsDebugInformationEndpoint = true)]
        public async Task PrefetchStats()
        {
            // the counters of the process wide prefetch ring (page hints and segment read-ahead), since the server started
            var ring = PrefetchRing.Default;
            var stats = ring.GetStats();
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(PrefetchRing.IsAvailable)] = ring.IsAvailable,
                    [nameof(PrefetchRing.MaxFiles)] = ring.MaxFiles,
                    [nameof(PrefetchRingStats.Submitted)] = stats?.Submitted,
                    [nameof(PrefetchRingStats.DroppedFull)] = stats?.DroppedFull,
                    [nameof(PrefetchRingStats.DroppedStale)] = stats?.DroppedStale,
                    [nameof(PrefetchRingStats.Failed)] = stats?.Failed,
                    [nameof(PrefetchRingStats.ReadGroups)] = stats?.ReadGroups,
                    [nameof(PrefetchRingStats.Reads)] = stats?.Reads,
                    [nameof(PrefetchRingStats.UsesIoUring)] = stats?.UsesIoUring,
                    ["SegmentReadAheadWarmUp"] = PrefetchPolicy.Global.Generation
                });
            }
        }

        [RavenAction("/admin/debug/io-metrics/live", "GET", AuthorizationStatus.Operator, SkipUsagesCount = true)]
        public async Task IoMetricsLive()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
                var receive = webSocket.ReceiveAsync(receiveBuffer, ServerStore.ServerShutdown);

                await using (var ms = RecyclableMemoryStreamFactory.GetRecyclableStream())
                using (var collector = new ServerStoreLiveIoStatsCollector(ServerStore))
                {
                    // 1. Send data to webSocket without making UI wait upon opening webSocket
                    await collector.SendDataOrHeartbeatToWebSocket(receive, webSocket, ms, 100);

                    // 2. Send data to webSocket when available
                    while (ServerStore.ServerShutdown.IsCancellationRequested == false)
                    {
                        if (await collector.SendDataOrHeartbeatToWebSocket(receive, webSocket, ms, 4000) == false)
                        {
                            break;
                        }
                    }
                }
            }
        }

        private sealed class ServerStoreLiveIoStatsCollector : LiveIoStatsCollector<TransactionOperationContext>
        {
            public ServerStoreLiveIoStatsCollector(ServerStore serverStore) : base(serverStore.IoChanges, GetSystemEnvironment(serverStore), null,
                serverStore.ContextPool,
                serverStore.ServerShutdown)
            {
            }
        }

        private static List<StorageEnvironmentWithType> GetSystemEnvironment(ServerStore serverStore)
        {
            return new List<StorageEnvironmentWithType>
            {
                new StorageEnvironmentWithType("<System>", StorageEnvironmentWithType.StorageEnvironmentType.System, serverStore._env)
            };
        }
    }
}
