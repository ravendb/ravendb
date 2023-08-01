using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.Queue.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.ETL.Providers.Queue.Handlers
{
    public sealed class QueueEtlHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/etl/queue/test", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task PostScriptTest()
        {
            using (var processor = new QueueEtlHandlerProcessorForPostScriptTest(this))
                await processor.ExecuteAsync();
        }
    }
}
