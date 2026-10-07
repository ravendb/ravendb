using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.ETL.Providers.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal class Talker(ConversationHandler handler, JsonOperationContext context, AiAgentConfiguration configuration, string schema, ConversationDocument document, string firstStreamPropertyPath, Func<Memory<byte>, Task> streaming) : IDisposable
{
    private List<BlittableJsonReaderObject> _preparedTools;

    public AiUsage AiUsage;
    public ChatCompletionClient Client;
    public ConversationDocument Document => document;

    public void Init()
    {
        document.EnsureInitialized();

        Client = handler.CreateClient();
        _preparedTools = Client.PrepareTools(context, handler.BuildToolDescriptors(context, configuration));
    }

    public AiChatRequest CreateRequest(List<AiAttachment> attachments)
    {
        AiUsage = new();
        return new AiChatRequest
        {
            Messages = document.Messages,
            Attachments = attachments,
            PreparedTools = _preparedTools,
            UseTools = document.RemainingToolIterations-- > 0,
            Schema = schema,
            PromptCacheKey = handler.GetPromptCacheKey(document.Id)
        };
    }

    public async Task<AiResponse> RunAsync(IMemoryContextPool contextPool, AiChatRequest request, AiDebugTrace trace, CancellationToken token)
    {
        if (streaming is null)
        {
            return await Client.CompleteAsync(
                context,
                request,
                AiUsage,
                trace,
                token
            );
        }

        return await Client.StreamingCompleteAsync(
            context,
            contextPool,
            firstStreamPropertyPath,
            request,
            streaming,
            AiUsage,
            trace,
            token
        );
    }

    public void Dispose()
    {
        Client?.Dispose();
    }
}
