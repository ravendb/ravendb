using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Json;
using Raven.Server.ServerWide.Commands.AI;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal class AiAgentProcessorForTestConversation : AbstractAiAgentProcessor
{
    public AiAgentProcessorForTestConversation([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using var _ = ContextPool.AllocateOperationContext(out JsonOperationContext context);
        using var token = RequestHandler.CreateHttpRequestBoundOperationToken();
        var options = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "ai-agent", token.Token);
        
        var body = JsonDeserializationServer.AiAgentTestRequest(options);
        var req = body.RequestBody;

        AiAgentHelpers.AddDefaultValues(body.Configuration, RequestHandler.Configuration.Ai);
        AddOrUpdateAiAgentCommand.ValidateConfiguration(context, body.Configuration);

        ConversationDocument conversation = null;
        if (body.Document != null)
        {
            conversation = ConversationDocument.ToDocument("test", body.Document);
        }

        if (conversation == null)
        {
            conversation = new ConversationDocument("test", req.Parameters);
            conversation.Initialize(context, body.Configuration);
        }

        await HandleRequest(context, body.Configuration, "test", conversation, req, token.Token);
    }

    public override Task<string> TryPersistAsync(JsonOperationContext context, AiAgentConfiguration configuration, string conversationId, ConversationDocument conversation,
        BlittableJsonReaderObject history)
    {
        // In test mode, we don't persist the conversation document
        return Task.FromResult("test");
    }

    public override void WriteResponse(JsonOperationContext context, AsyncBlittableJsonTextWriter writer,
        string conversationId, ConversationDocument document,
        BlittableJsonReaderObject response)
    {
        context.Write(writer, new DynamicJsonValue
        {
            [nameof(ConversationResult<object>.ConversationId)] = conversationId,
            [nameof(ConversationResult<object>.ChangeVector)] = document.ChangeVector,
            [nameof(ConversationResult<object>.Response)] = response,
            [nameof(ConversationResult<object>.ActionRequests)] = new DynamicJsonArray(document.OpenActionCalls.Select(t => t.Value.ToJson())),
            [nameof(ConversationResult<object>.TotalUsage)] = document.TotalUsage.ToJson(),
            ["Document"] = document.ToBlittable(context)
        });
    }

    public class AiAgentTestResult
    {
        public BlittableJsonReaderObject Document;
        public BlittableJsonReaderObject Response;
        public BlittableJsonReaderArray ActionRequests;
        public AiUsage Usage;
    }

    public class AiAgentTestRequest
    {
        public string UserPrompt;
        public BlittableJsonReaderObject CreationOptions;
        public AiAgentConfiguration Configuration;
        public BlittableJsonReaderArray ActionResponses;
        public BlittableJsonReaderObject Document;
        
        public RequestBody RequestBody
        {
            get
            {
                CreationOptions.TryGet(nameof(AiConversationCreationOptions.Parameters), out BlittableJsonReaderObject param);
                return new RequestBody
                {
                    UserPrompt = UserPrompt, 
                    Parameters = param, 
                    ActionResponses = ActionResponses
                };
            }
        }
    }
}
