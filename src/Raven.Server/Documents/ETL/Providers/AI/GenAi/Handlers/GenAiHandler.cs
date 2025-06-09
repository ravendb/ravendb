using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi.Handlers;

public sealed class GenAiHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/admin/ai/gen-ai/test", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task PostScriptTest()
    {
        using (var processor = new GenAiHandlerProcessorForPostScriptTest(this))
            await processor.ExecuteAsync();
    }
}
