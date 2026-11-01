using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.ETL.Providers.AI;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal class Talker(ConversationHandler handler, JsonOperationContext context, AiAgentConfiguration configuration, string schema, ConversationDocument document, string firstStreamPropertyPath, Func<Memory<byte>, Task> streaming) : IDisposable
{
    // Ollama can't call tools when the request has response_format, so its requests get the schema as text too.
    // The text asks the model to use its tools and actions before it answers: without that, small models answered
    // right away and skipped the client actions.
    private const string SchemaInstructions = "\nUse the tools and actions you need first, then reply. Your final answer must be JSON matching this schema: ";

    private List<BlittableJsonReaderObject> _preparedTools;
    private ParsedJsonCache _parsedJson;
    private BlittableJsonReaderObject _systemMessage;
    private BlittableJsonReaderObject _systemMessageWithSchema;

    public AiUsage AiUsage;
    public ChatCompletionClient Client;
    public ConversationDocument Document => document;

    public void Init()
    {
        document.EnsureInitialized();

        Client = handler.CreateClient();
        _preparedTools = Client.PrepareTools(context, handler.BuildToolDescriptors(context, configuration));
        _parsedJson = new ParsedJsonCache(context);
    }

    public AiChatRequest CreateRequest(List<AiAttachment> attachments, bool finalStructuredTurn)
    {
        AiUsage = new();

        // the final turn doesn't use up a tool iteration
        var useTools = finalStructuredTurn == false && document.RemainingToolIterations-- > 0;
        // can't send the tools with the schema, so every request carries the schema as text
        var splitsTools = schema != null && _preparedTools != null && Client.SupportsToolsWithStructuredOutput == false;
        var answerIsDropped = splitsTools && useTools;

        return new AiChatRequest
        {
            Messages = splitsTools ? WithSchemaInstructions() : document.Messages,
            Attachments = attachments,
            Tools = _preparedTools,
            UseTools = useTools,
            Schema = answerIsDropped ? null : schema,
            AnswerIsDropped = answerIsDropped,
            PromptCacheKey = document.Id,
            MayRepeatPrompt = handler.MayRepeatPrompt(_preparedTools),
            ParsedJson = _parsedJson
        };
    }

    // a copy for the requests only, the stored conversation keeps the agent's prompt
    private IEnumerable<BlittableJsonReaderObject> WithSchemaInstructions()
    {
        // built once per call, and again if the summarization replaced the system message
        var systemMessage = document.Messages[0];
        if (ReferenceEquals(systemMessage, _systemMessage) == false)
        {
            _systemMessage = systemMessage;
            _systemMessageWithSchema = CreateSystemMessageWithSchema(systemMessage);
        }

        return _systemMessageWithSchema == null ? document.Messages : document.Messages.Skip(1).Prepend(_systemMessageWithSchema);
    }

    private BlittableJsonReaderObject CreateSystemMessageWithSchema(BlittableJsonReaderObject systemMessage)
    {
        if (systemMessage.TryGet(ChatCompletionClient.Constants.RequestFields.Role, out string role) == false ||
            role != ChatCompletionClient.Constants.RequestFields.RoleSystemValue ||
            systemMessage.TryGet(ChatCompletionClient.Constants.RequestFields.Content, out string prompt) == false)
            return null;

        var parsedSchema = _parsedJson.GetOrParseSchema(schema);
        var schemaText = parsedSchema.TryGet(ChatCompletionClient.Constants.JsonSchemaFields.Schema, out BlittableJsonReaderObject inner) ? inner.ToString() : parsedSchema.ToString();

        return context.ReadObject(new DynamicJsonValue
        {
            [ChatCompletionClient.Constants.RequestFields.Role] = ChatCompletionClient.Constants.RequestFields.RoleSystemValue,
            [ChatCompletionClient.Constants.RequestFields.Content] = prompt + SchemaInstructions + schemaText
        }, "system/schema");
    }

    public async Task<AiResponse> RunAsync(IMemoryContextPool contextPool, AiChatRequest request, AiDebugTrace trace, CancellationToken token)
    {
        // its answer may be dropped, so it isn't streamed
        if (streaming is null || request.AnswerIsDropped)
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
