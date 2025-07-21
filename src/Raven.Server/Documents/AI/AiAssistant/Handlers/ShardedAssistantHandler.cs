using System.Threading.Tasks;
using Raven.Server.Documents.AI.AiAssistant.Handlers.Processors;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Routing;

namespace Raven.Server.Documents.AI.AiAssistant.Handlers;

public class ShardedAssistantHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/assistant/assist", "POST")]
    public async Task Assist()
    {
        using (var processor = new AiAssistantAssistProcessor(this))
            await processor.ExecuteAsync();
    }
    
    [RavenShardedAction("/databases/*/assistant/give-consent", "POST")]
    public async Task GiveConsent()
    {
        using (var processor = new AiAssistantGiveConsentProcessor(this))
            await processor.ExecuteAsync();
    }
    
    [RavenShardedAction("/databases/*/assistant/check-consent", "GET")]
    public async Task CheckConsent()
    {
        using (var processor = new AiAssistantCheckConsentProcessor(this))
            await processor.ExecuteAsync();
    }
    
    [RavenShardedAction("/databases/*/assistant/check-usage", "GET")]
    public async Task CheckUsage()
    {
        using (var processor = new AiAssistantCheckUsageProcessor(this))
            await processor.ExecuteAsync();
    }
}
