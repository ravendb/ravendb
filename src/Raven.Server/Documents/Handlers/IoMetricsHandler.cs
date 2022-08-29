using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.IoMetrics;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Utils.IoMetrics;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class IoMetricsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/io-metrics", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task Get()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
            {
                var result = IoMetricsUtil.GetIoMetricsResponse(Database.GetAllStoragesEnvironment(), Database.GetAllPerformanceMetrics());
                context.Write(writer, result.ToJson());
            }
        }

        [RavenAction("/databases/*/debug/io-metrics/live", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true)]
        public async Task Live()
        {
            using (var processor = new IoMetricsHandlerProcessorForLive(this))
                await processor.ExecuteAsync();
        }
    }
}
