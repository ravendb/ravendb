using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Handlers.Processors;
using Raven.Server.Routing;

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
            using (var processor = new EtlHandlerProcessorForPerformanceLive(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/etl/progress", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task Progress()
        {
            using (var processor = new EtlHandlerProcessorForProgress(this))
                await processor.ExecuteAsync();
        }
    }
}
