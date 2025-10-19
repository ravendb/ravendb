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
    private const int DefaultMaxModelIterationsPerCall = 16;

    private int _maxModelIterationsPerCall;
    private string _schema;
    private List<BlittableJsonReaderObject> _tools;
    private int _remainingToolIterations;

    public AiUsage AiUsage;
    public ChatCompletionClient Client;

    public void Init()
    {
        document.EnsureInitialized();

        _schema = ChatCompletionClient.GetSchemaForRequest(configuration.OutputSchema, configuration.SampleObject);
        _tools = ConversationDocument.GenerateTools(context, configuration);
        
        _maxModelIterationsPerCall = configuration.MaxModelIterationsPerCall ?? DefaultMaxModelIterationsPerCall;
        _remainingToolIterations = _maxModelIterationsPerCall - document.NumberOfRepeatedToolCalls;

        Client = handler.CreateClient();
    }

    public HttpRequestMessage CreateCompletionRequest(List<AiAttachment> attachments)
    {
        AiUsage = new();
        return Client.CreateCompletionRequest(context, document.Messages, attachments, _tools, useTools: _remainingToolIterations-- > 0, streaming != null, _schema);
    }

    public async Task<AiResponse> RunAsync(IMemoryContextPool contextPool, HttpRequestMessage request, CancellationToken token)
    {
        if (streaming is null)
        {
            return await Client.CompleteAsync(
                context,
                request,
                AiUsage,
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
            token
        );
    }

    public void Dispose()
    {
        Client?.Dispose();
    }
}
