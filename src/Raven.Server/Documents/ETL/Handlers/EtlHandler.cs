using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.ETL.Handlers
{
    public sealed class EtlHandler : DatabaseRequestHandler
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
        
        [RavenAction("/databases/*/etl/errors", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetErrors()
        {
            using (var processor = new EtlHandlerProcessorForGetErrors(this))
                await processor.ExecuteAsync();
        }
        
        [RavenAction("/databases/*/etl/errors", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task DeleteErrors()
        {
            using (var processor = new EtlHandlerProcessorForDeleteErrors(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/etl/retry-batch", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task RetryBatch()
        {
            using (var processor = new EtlHandlerProcessorForRetryBatch(this))
                await processor.ExecuteAsync();
        }
    }
}
