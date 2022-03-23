using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Batches;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers.Batches
{
    public class BatchHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/bulk_docs", "POST", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task BulkDocs()
        {
            using (var processor = new BatchHandlerProcessorForBulkDocs(this))
                await processor.ExecuteAsync();
        }
    }
}
