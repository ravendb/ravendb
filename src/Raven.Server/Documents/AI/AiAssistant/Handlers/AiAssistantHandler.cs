using System.Threading.Tasks;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.AI.AiAssistant.Handlers.Processors;
using Raven.Server.Routing;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.AI.AiAssistant.Handlers;

public class AiAssistantHandler : ServerRequestHandler
{
    [RavenAction("/assistant/assist", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task Assist()
    {
        using (var processor = new AiAssistantAssistProcessor(this))
            await processor.ExecuteAsync();
    }
    
    [RavenAction("/assistant/give-consent", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GiveConsent()
    {
        using (var processor = new AiAssistantGiveConsentProcessor(this))
            await processor.ExecuteAsync();
    }
    
    [RavenAction("/assistant/check-consent", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task CheckConsent()
    {
        using (var processor = new AiAssistantCheckConsentProcessor(this))
            await processor.ExecuteAsync();
    }
    
    [RavenAction("/assistant/check-usage", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task CheckUsage()
    {
        using (var processor = new AiAssistantCheckUsageProcessor(this))
            await processor.ExecuteAsync();
    }

    [RavenAction("/assistant/settings", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetSettings()
    {
        var aiSettings = ServerStore.Configuration.Ai;
        using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        using (var token = CreateHttpRequestBoundOperationToken())
        await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream(), token.Token))
        {
            writer.WriteObject(context.ReadObject(new DynamicJsonValue
            {
                [nameof(AiConfiguration.DisableAiAssistant)] = aiSettings.DisableAiAssistant,
                [nameof(AiConfiguration.DisableDataSubmission)] = aiSettings.DisableDataSubmission
            }, "ai-settings"));
        }
    }
}
