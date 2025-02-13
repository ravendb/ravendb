using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.AI.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.ETL.Providers.AI.Handlers;

public sealed class AiIntegrationHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/admin/ai/test", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task PostScriptTest()
    {
        using (var processor = new AiIntegrationHandlerProcessorForPostScriptTest(this))
            await processor.ExecuteAsync();
    }
}
