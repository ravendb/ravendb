using System.Threading.Tasks;
using Raven.Server.Documents.AI.AiAssistant.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.AI.AiAssistant.Handlers;

public class AiAssistantHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/assistant/assist", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task Assist()
    {
        using (var processor = new AiAssistantAssistProcessor(this))
            await processor.ExecuteAsync();
    }
    
    [RavenAction("/databases/*/assistant/give-consent", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GiveConsent()
    {
        using (var processor = new AiAssistantGiveConsentProcessor(this))
            await processor.ExecuteAsync();
    }
    
    [RavenAction("/databases/*/assistant/check-consent", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task CheckConsent()
    {
        using (var processor = new AiAssistantCheckConsentProcessor(this))
            await processor.ExecuteAsync();
    }
    
    [RavenAction("/databases/*/assistant/check-usage", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task CheckUsage()
    {
        using (var processor = new AiAssistantCheckUsageProcessor(this))
            await processor.ExecuteAsync();
    }
}
