using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.AI.Agents;

public class AiAgentHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/admin/ai/agent", "PUT", AuthorizationStatus.DatabaseAdmin)]
    public async Task AddOrModifyAiAgent()
    {
        using (var processor = new AiAgentProcessorForAddOrUpdateAiAgent<DatabaseRequestHandler, DocumentsOperationContext>(this))
        {
            await processor.ExecuteAsync();
        }
    }

    [RavenAction("/databases/*/admin/ai/agent", "DELETE", AuthorizationStatus.DatabaseAdmin)]
    public async Task DeleteAiAgent()
    {
        using (var processor = new AiAgentProcessorForDeleteAiAgent<DatabaseRequestHandler, DocumentsOperationContext>(this))
        {
            await processor.ExecuteAsync();
        }
    }

    [RavenAction("/databases/*/admin/ai/agent", "GET", AuthorizationStatus.DatabaseAdmin)]
    public async Task GetAiAgentConfiguration()
    {
        using (var processor = new AiAgentProcessorForGetAiAgent<DatabaseRequestHandler, DocumentsOperationContext>(this))
        {
            await processor.ExecuteAsync();
        }
    }

    [RavenAction("/databases/*/ai/agent", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
    public async Task RunAiAgent()
    {
        using (var processor = new AiAgentProcessor(this))
        {
            await processor.ExecuteAsync();
        }
    }

    [RavenAction("/databases/*/ai/agent/test", "POST", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
    public async Task AiAgentTest()
    {
        using (var processor = new AiAgentProcessorForTestConversation(this))
        {
            await processor.ExecuteAsync();
        }
    }


    [RavenAction("/databases/*/admin/ai/agent/generate-code", "GET", AuthorizationStatus.DatabaseAdmin)]
    public async Task AiAgentGenerateCode()
    {
        using (var processor = new AiAgentProcessorForGenerateCode<DatabaseRequestHandler, DocumentsOperationContext>(this))
        {
            await processor.ExecuteAsync();
        }
    }

    [RavenAction("/databases/*/ai/agent/conversation/messages", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetConversationMessages()
    {
        using (var processor = new AiAgentProcessorForGetConversationMessages(this))
        {
            await processor.ExecuteAsync();
        }
    }
}
