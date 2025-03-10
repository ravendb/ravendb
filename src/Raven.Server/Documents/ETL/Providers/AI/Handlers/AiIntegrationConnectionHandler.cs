using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.AI.Handlers.Processors;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.AI.Handlers;

public sealed class AiIntegrationConnectionHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/admin/ai/test-connection", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task TestAiConnection()
    {
        using (var processor = new AiIntegrationHandlerProcessorForTestAiConnection<DatabaseRequestHandler, DocumentsOperationContext>(this))
            await processor.ExecuteAsync();
    }
}
