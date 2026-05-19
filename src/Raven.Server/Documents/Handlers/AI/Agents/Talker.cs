using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.ETL.Providers.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal class Talker(ConversationHandler handler, JsonOperationContext context, AiAgentConfiguration configuration, ConversationDocument document, string firstStreamPropertyPath, Func<Memory<byte>, Task> streaming) : IDisposable
{
    private string _schema;
    private List<BlittableJsonReaderObject> _tools;

    public AiUsage AiUsage;
    public ChatCompletionClient Client;
    public ConversationDocument Document => document;

    public void Init()
    {
        document.EnsureInitialized();

        _schema = ChatCompletionClient.GetSchemaForRequest(configuration.OutputSchema, configuration.SampleObject);
        Client = handler.CreateClient();
        _tools = Client.GenerateTools(context, configuration, handler);
    }

    public HttpRequestMessage CreateCompletionRequest(List<AiAttachment> attachments, AiDebugTrace trace)
    {
        AiUsage = new();
        return Client.CreateCompletionRequest(context, document.Messages, attachments, _tools, useTools: document.RemainingToolIterations-- > 0, streaming != null, _schema, promptCacheKey: document.Id, trace: trace);
    }

    public async Task<AiResponse> RunAsync(IMemoryContextPool contextPool, HttpRequestMessage request, AiDebugTrace trace, CancellationToken token)
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
