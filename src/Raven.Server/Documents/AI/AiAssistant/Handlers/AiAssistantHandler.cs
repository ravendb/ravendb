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
    
    [RavenAction("/databases/*/assistant/consent", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task SignConsent()
    {
        using (var processor = new AiAssistantConsentProcessor(this))
            await processor.ExecuteAsync();
    }
    
    [RavenAction("/databases/*/assistant/quota", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task Quota()
    {
        using (var processor = new AiAssistantQuotaProcessor(this))
            await processor.ExecuteAsync();
    }
}
