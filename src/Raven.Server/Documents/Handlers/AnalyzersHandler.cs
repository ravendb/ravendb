using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Analyzers;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers
{
    public class AnalyzersHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/analyzers", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Get()
        {
            using (var processor = new AnalyzersHandlerProcessorForGet<DocumentsOperationContext>(this))
                await processor.ExecuteAsync();
        }
    }
}
