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
    
    [RavenShardedAction("/databases/*/assistant/consent", "POST")]
    public async Task SignConsent()
    {
        using (var processor = new AiAssistantConsentProcessor(this))
            await processor.ExecuteAsync();
    }
    
    [RavenShardedAction("/databases/*/assistant/quota", "GET")]
    public async Task Quota()
    {
        using (var processor = new AiAssistantQuotaProcessor(this))
            await processor.ExecuteAsync();
    }
}
