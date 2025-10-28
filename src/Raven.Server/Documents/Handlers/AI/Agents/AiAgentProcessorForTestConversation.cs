using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.AI;
using Raven.Server.ServerWide.Context;
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
        using var _ = ContextPool.AllocateOperationContext(out DocumentsOperationContext context);
        using var token = RequestHandler.CreateHttpRequestBoundOperationToken();
        var streaming = RequestHandler.GetBoolValueQueryString("streaming", required: false) ?? false;
        var options = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "ai-agent", token.Token);
        
        var body = JsonDeserializationServer.AiAgentTestRequest(options);
        var req = body.RequestBody;

        AiAgentHelpers.AddDefaultValues(body.Configuration, RequestHandler.Configuration.Ai);
        AddOrUpdateAiAgentCommand.ValidateConfiguration(context, body.Configuration);

        var handler = new TestConversationHandler(ServerStore, RequestHandler.Database, body)
        {
            Authentication = RequestHandler.HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection
        };
        await ExecuteInternalAsync(handler, context, body.Configuration, "TestConversation", req, changeVector: null, streaming: streaming, token: token);
    }

    public class TestConversationHandler(ServerStore server, DocumentDatabase database, AiAgentTestRequest request) : ConversationHandler(server, database)
    {
        protected override Task<string> TryPersistAsync(JsonOperationContext context, List<BlittableJsonReaderObject> historyDocs)
        {
            // In test mode, we don't persist the conversation document
            return Task.FromResult("test");
        }

        public override DynamicJsonValue GetConversationResponse(JsonOperationContext context, BlittableJsonReaderObject response)
        {
            var r = base.GetConversationResponse(context, response);
            r["Document"] = Document.ToBlittable(context);
            return r;
        }

        protected override async Task InitializeDocument(DocumentsOperationContext context)
        {
            if (request.Document == null)
            {
                await base.InitializeDocument(context);
                return;
            }

            Document = ConversationDocument.ToDocument("TestConversation", request.Document, request.Configuration);
        }
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
        public object UserPrompt;
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
                    ActionResponses = ActionResponses,
                    CreationOptions = new AiConversationCreationOptions()
                };
            }
        }
    }
}
