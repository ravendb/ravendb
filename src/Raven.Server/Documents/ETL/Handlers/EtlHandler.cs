﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Handlers.Processors;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Json;
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Handlers
{
    public class EtlHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/etl/stats", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task Stats()
        {
            using (var processor = new EtlHandlerProcessorForStats(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/etl/debug/stats", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task DebugStats()
        {
            using (var processor = new EtlHandlerProcessorForDebugStats(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/etl/performance", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Performance()
        {
            using (var processor = new EtlHandlerProcessorForPerformance(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/etl/performance/live", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true)]
        public async Task PerformanceLive()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                var etls = GetProcessesToReportOn();

                var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
                var receive = webSocket.ReceiveAsync(receiveBuffer, Database.DatabaseShutdown);

                await using (var ms = new MemoryStream())
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

        [RavenAction("/databases/*/etl/progress", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task Progress()
        {
            using (var processor = new EtlHandlerProcessorForProgress(this))
                await processor.ExecuteAsync();
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
