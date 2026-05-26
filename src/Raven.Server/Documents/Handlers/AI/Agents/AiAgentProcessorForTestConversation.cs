using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Exceptions.Commercial;
using Raven.Server.Documents.AI;
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
        var conversationId = RequestHandler.GetStringQueryString("conversationId", required: false) ?? "TestConversation";
        var options = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "ai-agent", token.Token);
        
        var request = JsonDeserializationServer.AiAgentTestRequest(options);
        var body = request.RequestBody;

        AiAgentHelpers.AddDefaultValues(request.Configuration, RequestHandler.Configuration.Ai);
        AddOrUpdateAiAgentCommand.ValidateConfiguration(context, request.Configuration);

        var handler = new TestConversationHandler(ServerStore, RequestHandler.Database, request)
        {
            Authentication = RequestHandler.HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection
        };

        if (ServerStore.LicenseManager.LicenseStatus.HasAiAgent == false)
            throw new LicenseLimitException(LimitType.AiAgent, "Your license doesn't support using the AI Agent feature.");

        await ExecuteInternalAsync(handler, context, request.Configuration, conversationId, body, changeVector: null, streaming: streaming, debugOverride: null, token: token);
    }

    public class TestConversationHandler(ServerStore server, DocumentDatabase database, AiAgentTestRequest request) : ConversationHandler(server, database)
    {
        private Dictionary<string, BlittableJsonReaderObject> _documents;
        private DocumentsOperationContext _documentsContext;
        private readonly DocumentDatabase _database = database;

        protected override DynamicJsonValue CreateAgentRequest(string agent, string conversationId, string prompt, IEnumerable<object> actionResponses, DynamicJsonValue creationOptions)
        {
            // We send only documents from the current conversation subtree (self + descendants).
            // This gives the sub-agent full context for its scope,
            // and allows us to safely update only this scope in `UpdateDocuments`.
            var relevantDocuments = _documents.Where(kvp => IsSelfOrChild(conversationId, kvp.Key)).ToDictionary();

            var baseJson = base.CreateAgentRequest(agent, conversationId, prompt, actionResponses, creationOptions);
            var content = (DynamicJsonValue)baseJson[nameof(GetRequest.Content)];
            content[nameof(AiAgentTestRequest.Configuration)] = GetAiAgentConfiguration(agent).ToJson();
            content[nameof(AiAgentTestRequest.Documents)] = DynamicJsonValue.Convert(relevantDocuments);
            return new DynamicJsonValue
            {
                [nameof(GetRequest.Url)] = $"/databases/{_database.Name}/ai/agent/test",
                [nameof(GetRequest.Query)] = new StringBuilder("?").Append("conversationId=").Append(Uri.EscapeDataString(conversationId)).ToString(),
                [nameof(GetRequest.Method)] = "POST",
                [nameof(GetRequest.Content)] = content
            };
        }

        protected override void ProcessSubConversationResult(JsonOperationContext context, SubConversationResult r)
        {
            base.ProcessSubConversationResult(context, r);

            foreach (var response in r.SubAgentsResponses ?? [])
            {
                if (response.TryGet(nameof(AiAgentTestResult.Documents), out BlittableJsonReaderObject docsBjro) == false)
                    throw new InvalidOperationException($"Missing '{nameof(AiAgentTestResult.Documents)}' field on sub-agent test response");

                // We update only documents from the current sub-conversation subtree (its 'scope'),
                // since `CreateAgentRequest` sends only this scoped data to the sub-agent.
                // This prevents overriding newer data with stale data from other sub-agents.
                // Without scoping:
                // Sub-agent A returns updated docs for A but stale docs for B.
                // Later, sub-agent B returns updated docs for B but stale docs for A.
                // B could override A with stale data.
                UpdateDocuments(docsBjro);
            }
        }

        private void UpdateDocuments(BlittableJsonReaderObject documents)
        {
            foreach (var subId in documents.GetPropertyNames())
            {
                if (_documents.TryGetValue(subId, out BlittableJsonReaderObject oldDoc))
                    oldDoc.Dispose();

                var newDoc = documents[subId] as BlittableJsonReaderObject
                             ?? throw new InvalidOperationException(
                                 $"Expected '{subId}' to be of type {nameof(BlittableJsonReaderObject)}, " +
                                 $"but got '{documents[subId]?.GetType().Name ?? "null"}'.");

                _documents[subId] = newDoc.Clone(_documentsContext);
            }
        }

        protected override bool TryCloseSubAgentCall(JsonOperationContext context, string subConversationId, BlittableJsonReaderObject response, 
            AiToolCall currentCall,
            SubConversationResult result)
        {
            result.SubAgentsResponses ??= [];
            result.SubAgentsResponses.Add(response.CloneOnTheSameContext());
            return base.TryCloseSubAgentCall(context, subConversationId, response, currentCall, result);
        }

        private bool IsSelfOrChild(string parentId, string candidateId) => candidateId == parentId || candidateId.StartsWith(parentId + "/");

        protected override Task<string> TryPersistAsync(JsonOperationContext context, List<BlittableJsonReaderObject> historyDocs)
        {
            // In test mode, we don't persist the conversation document, we just save it in _documents
            _documents[_document.Id] = _document.ToBlittable(_documentsContext);
            return Task.FromResult(_document.Id);
        }

        public override DynamicJsonValue GetConversationResponse(JsonOperationContext context, BlittableJsonReaderObject response, int toolsIterations)
        {
            var r = base.GetConversationResponse(context, response, toolsIterations);
            r[nameof(AiAgentTestResult.Documents)] = DynamicJsonValue.Convert(_documents);
            return r;
        }

        protected override async Task InitializeDocument(DocumentsOperationContext context)
        {
            _documentsContext = context;

            if (request.Documents == null || request.Documents.TryGetValue(_conversationId, out BlittableJsonReaderObject docBjro) == false)
                await base.InitializeDocument(context);
            else
                _document = ConversationDocument.ToDocument(_conversationId, docBjro, _maxModelIterationsPerCall); // document exists, we initialize from it instead of creating a new one

            _documents = request.Documents ?? new();
        }
    }

    public class AiAgentTestResult
    {
        public Dictionary<string, BlittableJsonReaderObject> Documents;
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
        public Dictionary<string, BlittableJsonReaderObject> Documents;
        
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
