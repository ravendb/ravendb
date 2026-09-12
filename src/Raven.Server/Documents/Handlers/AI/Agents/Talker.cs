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

internal class Talker(ConversationHandler handler, JsonOperationContext context, AiAgentConfiguration configuration, string schema, ConversationDocument document, string firstStreamPropertyPath, Func<Memory<byte>, Task> streaming) : IDisposable
{
    private List<BlittableJsonReaderObject> _tools;
    private string _turnSchema;
    private bool _turnStreaming;

    public AiUsage AiUsage;
    public ChatCompletionClient Client;
    public ConversationDocument Document => document;
    public bool RequiresStructuredFollowUp { get; private set; }

    private bool SplitToolsAndSchema => schema != null && Client.SupportsToolsWithStructuredOutput == false;

    public void Init()
    {
        document.EnsureInitialized();

        Client = handler.CreateClient();
        _tools = Client.GenerateTools(context, configuration, handler);
    }

    public HttpRequestMessage CreateCompletionRequest(List<AiAttachment> attachments, AiDebugTrace trace, bool finalStructuredTurn = false)
    {
        AiUsage = new();

        var useTools = finalStructuredTurn == false && document.RemainingToolIterations-- > 0;
        var toolTurnWithoutSchema = SplitToolsAndSchema && useTools;

        _turnSchema = toolTurnWithoutSchema ? null : schema;
        _turnStreaming = streaming != null && toolTurnWithoutSchema == false;
        RequiresStructuredFollowUp = toolTurnWithoutSchema;

        var turnTools = SplitToolsAndSchema && useTools == false ? null : _tools;

        return Client.CreateCompletionRequest(context, document.Messages, attachments, turnTools, useTools, _turnStreaming, _turnSchema, promptCacheKey: document.Id, trace: trace);
    }

    public async Task<AiResponse> RunAsync(IMemoryContextPool contextPool, HttpRequestMessage request, AiDebugTrace trace, CancellationToken token)
    {
        if (_turnStreaming == false)
        {
            return await Client.CompleteAsync(
                context,
                request,
                AiUsage,
                _turnSchema,
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
            _turnSchema,
            trace,
            token
        );
    }

    public void Dispose()
    {
        Client?.Dispose();
    }
}
