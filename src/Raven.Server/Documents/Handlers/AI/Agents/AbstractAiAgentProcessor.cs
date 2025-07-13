using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.AiGen;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Handlers.Processors.MultiGet;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.Handlers.AI.Agents
{
    internal abstract class AbstractAiAgentProcessor : AbstractDatabaseHandlerProcessor<DatabaseRequestHandler, DocumentsOperationContext>
    {
        protected AbstractAiAgentProcessor([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public async Task HandleRequest(
            JsonOperationContext context, 
            AiAgentConfiguration configuration, 
            string conversationId, 
            ConversationDocument document, 
            RequestBody body,
            CancellationToken token)
        {
            var hasActionResponse = body.ActionResponses is { Length: > 0 };
            var hasUserPrompt = string.IsNullOrEmpty(body.UserPrompt) == false;

            if (hasActionResponse && hasUserPrompt)
                throw new InvalidOperationException($"Cannot have a conversation '{conversationId}' with open action calls and user prompt.");

            if (body.ActionResponses != null)
            {
                foreach (BlittableJsonReaderObject tool in body.ActionResponses)
                {
                    var t = JsonDeserializationClient.ActionResponse(tool);
                    if (document.OpenActionCalls.Remove(t.ToolId) == false)
                        throw new InvalidOperationException($"{t.ToolId} is an unknown action ID for conversation '{conversationId}'");

                    document.Messages.Add(context.ReadObject(new DynamicJsonValue { ["tool_call_id"] = t.ToolId, ["role"] = "tool", ["content"] = t.Content },
                        "user/tool"));
                }
            }

            if (document.OpenActionCalls.Count > 0)
            {
                await TryPersistAsync(context, configuration, conversationId, document);
                await WriteResponseAsync(context, conversationId, (Response: null, document));
                return;
            }

            if (hasActionResponse == false && hasUserPrompt == false)
                throw new InvalidOperationException($"Cannot have a conversation '{conversationId}' without open action calls or user prompt.");

            if (string.IsNullOrEmpty(body.UserPrompt) == false)
            {
                document.AddMessage(context, context.ReadObject(new DynamicJsonValue { ["role"] = "user", ["content"] = body.UserPrompt }, "user/msg"));
            }

            var r = await TalkAsync(context, configuration, document, token: token);

            conversationId = await TryPersistAsync(context, configuration, conversationId, r.Document);
            await WriteResponseAsync(context, conversationId, r);
        }

        public override async ValueTask ExecuteAsync()
        {
            using var token = RequestHandler.CreateHttpRequestBoundOperationToken();
            var conversationId = RequestHandler.GetStringQueryString("conversationId", required: false);
            var agentId = RequestHandler.GetStringQueryString("agentId", required: false);
            var changeVector = RequestHandler.GetStringQueryString("changeVector", required: false);

            if (string.IsNullOrEmpty(conversationId) && string.IsNullOrEmpty(agentId))
                throw new ArgumentException("conversation ID or agent name must be provided.");

            if (string.IsNullOrEmpty(conversationId) == false && string.IsNullOrEmpty(agentId) == false)
                throw new ArgumentException($"conversation '{conversationId}' and agent '{agentId}' can't be provided together.");

            using var _ = ContextPool.AllocateOperationContext(out DocumentsOperationContext context);
            var body = await ReadRequestBodyAsync(context, token.Token);

            ConversationDocument conversationDocument = null;
            AiAgentConfiguration configuration = null;

            if (string.IsNullOrEmpty(conversationId) == false)
            {
                using var __ = context.OpenReadTransaction();
                var conversation = RequestHandler.Database.DocumentsStorage.Get(context, conversationId);
                if (conversation == null)
                    throw new DocumentDoesNotExistException(conversationId);

                conversationDocument = ConversationDocument.ToDocument(conversationId, conversation.Data);

                if (changeVector != null)
                {
                    if (conversation.ChangeVector != changeVector)
                        throw new ConcurrencyException($"The conversation '{conversationId}' was changed, please try again")
                        {
                            ExpectedChangeVector = changeVector,
                            ActualChangeVector = conversation.ChangeVector,
                            Id = conversationId
                        };

                    conversationDocument.ChangeVector = conversation.ChangeVector;
                }
               
                configuration = GetAiAgentConfiguration(conversationDocument.Agent);
            }

            if (string.IsNullOrEmpty(agentId) == false)
            {
                configuration = GetAiAgentConfiguration(agentId);
                conversationDocument = new ConversationDocument(agentId, body.Parameters);
                conversationDocument.Initialize(context, configuration, body.UserPrompt);
                conversationId = BuildId(configuration);
            }

            await HandleRequest(context, configuration, conversationId, conversationDocument, body, token.Token);
        }

        private string BuildId(AiAgentConfiguration configuration)
        {
            var agentPrefix = $"{configuration.Identifier}{RequestHandler.IdentityPartsSeparator}";
            var collection = configuration.Persistence?.Collection ?? Constants.Documents.Collections.AiAgentConversationCollection;

            return $"{agentPrefix}{collection}{RequestHandler.IdentityPartsSeparator}";
        }

        public async Task<RequestBody> ReadRequestBodyAsync(JsonOperationContext context, CancellationToken token)
        {
            var body = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "ai-agent", token);
            body.TryGet(nameof(ConversionRequestBody.ActionResponses), out BlittableJsonReaderArray actionResponses);
            body.TryGet(nameof(ConversionRequestBody.UserPrompt), out string userPrompt);
            body.TryGet(nameof(ConversionRequestBody.Parameters), out BlittableJsonReaderObject parameters);

            return new RequestBody
            {
                ActionResponses = actionResponses, 
                UserPrompt = userPrompt, 
                Parameters = parameters
            };
        }

        public class RequestBody
        {
            public BlittableJsonReaderObject Parameters { get; set; }
            public string UserPrompt { get; set; }
            public BlittableJsonReaderArray ActionResponses { get; set; }

            public void ValidateForStart()
            {
                if (string.IsNullOrEmpty(UserPrompt))
                    throw new ArgumentException("User prompt is missing.");

                if (Parameters == null)
                    throw new ArgumentException(nameof(Parameters));
            }

            public void ValidateForResume()
            {
                if (string.IsNullOrEmpty(UserPrompt))
                    throw new ArgumentException("User prompt is missing.");

                if (ActionResponses == null)
                    throw new ArgumentException(nameof(ActionResponses));
            }
        }

        public async Task<(BlittableJsonReaderObject Response, ConversationDocument Document)> TalkAsync(JsonOperationContext context, AiAgentConfiguration configuration,
            ConversationDocument document, CancellationToken token)
        {
            document.EnsureInitialized();

            var conStr = GetAiConnectionString(configuration.ConnectionStringName);

            var schema = ChatCompletionClient.GetSchemaForRequest(configuration.OutputSchema, configuration.SampleObject);

            using var client = ChatCompletionClient.CreateChatCompletionClient(ContextPool, conStr, schema);

            var tools = document.GenerateTools(context, configuration);

            AiResponse aiResponse;

            while (true)
            {
                aiResponse = await client.CompleteAsync(
                    context,
                    document.Messages,
                    tools,
                    document.TotalUsage,
                    token
                );
                if (aiResponse.Type is AiResponseType.Result)
                    break;

                await HandleQueryToolCallsAsync(context, configuration, document, aiResponse);

                if (TryGetUserTools(context, document, configuration, aiResponse))
                    break; // we need to return the user tool requests to the client, so we can continue the conversation
            }

            return (aiResponse.Result, document);
        }

        private bool TryGetUserTools(JsonOperationContext context, ConversationDocument document, AiAgentConfiguration configuration, AiResponse result)
        {
            foreach (var call in result.ToolCalls)
            {
                if (configuration.FindAction(call.Name) == null)
                    continue;

                document.OpenActionCalls.Add(call.Id,
                    new AiAgentActionRequest { ToolId = call.Id, Name = call.Name, Arguments = CreateParameters(context, call, document.Parameters).ToString() });
            }

            return document.OpenActionCalls.Count > 0;
        }

        public virtual async Task WriteResponseAsync(JsonOperationContext context, string conversationId, (BlittableJsonReaderObject Response, ConversationDocument Document) r)
        {
            var output = new DynamicJsonValue
            {
                [nameof(ConversationResult<object>.ConversationId)] = conversationId,
                [nameof(ConversationResult<object>.ChangeVector)] = r.Document.ChangeVector,
                [nameof(ConversationResult<object>.Response)] = r.Response,
                [nameof(ConversationResult<object>.ActionRequests)] = new DynamicJsonArray(r.Document.OpenActionCalls.Select(t => t.Value.ToJson())),
                [nameof(ConversationResult<object>.Usage)] = r.Document.TotalUsage.ToJson()
            };

            await using var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream());
            context.Write(writer, output);
        }

        public static BlittableJsonReaderObject CreateParameters(JsonOperationContext context, AiToolCall call, BlittableJsonReaderObject parameters)
        {
            var args = context.Sync.ReadForMemory(call.Arguments, "call/args");
            if (parameters is null)
                return args;

            args.Modifications = new DynamicJsonValue();
            BlittableJsonReaderObject.PropertyDetails prop = default;
            for (int i = 0; i < parameters.Count; i++)
            {
                // Important: we *override* any parameter from the model with the user provided values
                // to ensure the safety & security of this feature. Model cannot override those values, period.
                parameters.GetPropertyByIndex(i, ref prop);
                args.Modifications[prop.Name] = prop.Value;
            }

            return args;
        }

        public AiAgentConfiguration GetAiAgentConfiguration(string identifier)
        {
            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
            using (ctx.OpenReadTransaction())
            using (var record = ServerStore.Cluster.ReadRawDatabaseRecord(ctx, RequestHandler.DatabaseName))
            {
                if (record.TryGetAiAgent(identifier, out var configuration) == false)
                    throw new ArgumentException($"AI Agent '{identifier}' doesn't exists");

                return configuration;
            }
        }

        private AiConnectionString GetAiConnectionString(string name)
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverCtx))
            using (serverCtx.OpenReadTransaction())
            {
                return ServerStore.Cluster.ReadRawDatabaseRecord(serverCtx, RequestHandler.DatabaseName).GetAiConnectionString(name)
                       ?? throw new InvalidOperationException("Cannot find connection string: " + name);
            }
        }

        public async Task HandleQueryToolCallsAsync(JsonOperationContext context, AiAgentConfiguration cfg, ConversationDocument document, AiResponse result)
        {
            // TODO: handle a response that does both query & action
            DynamicJsonArray reqs = [];
            List<string> toolCallsIds = [];
            var queryUrl = $"/databases/{RequestHandler.DatabaseName}/queries";
            foreach (var call in result.ToolCalls)
            {
                var q = cfg.FindQuery(call.Name);
                if (q is null)
                    continue;

                toolCallsIds.Add(call.Id);
                reqs.Add(new DynamicJsonValue
                {
                    ["Url"] = queryUrl,
                    ["Query"] = null,
                    ["Method"] = "POST",
                    ["Content"] = new DynamicJsonValue
                    {
                        ["Query"] = q.Query,
                        // TODO: need to dispose this? Or maybe use a dedicated context per each tool call to avoid high memory?
                        ["QueryParameters"] = CreateParameters(context, call, document.Parameters)
                    }
                });
            }

            using (var reqsBlittable = context.ReadObject(new DynamicJsonValue { ["Requests"] = reqs }, "ai-agent/multi-query"))
            using (var handler = new MultiGetHandlerProcessorForPost(RequestHandler))
            using (var memoryStream = RecyclableMemoryStreamFactory.GetRecyclableStream())
            {
                await handler.ExecuteMultiGetAsync(context, reqsBlittable, memoryStream);
                memoryStream.Position = 0;
                // TODO: have to verify that we got a successful result here!
                using var resp = context.Sync.ReadForMemory(memoryStream, "query/response");
                if (resp.TryGet("Results", out BlittableJsonReaderArray results) is false) // TODO: shouldn't happen, but add error handling
                    throw new InvalidOperationException("Missing Results from multi-get reply");

                for (int i = 0; i < results.Length; i++)
                {
                    var queryResponse = (BlittableJsonReaderObject)results[i];
                    if (queryResponse.TryGet("StatusCode", out int statusCode) == false)
                        throw new InvalidOperationException("Missing status code"); // TODO: shouldn't happen, but add error handling
                    if (queryResponse.TryGet("Result", out BlittableJsonReaderObject queryResponseResult) is false)
                        throw new InvalidOperationException("Missing Result from query request output"); // TODO: shouldn't happen, but add error handling

                    if (statusCode != 200)
                        throw ExceptionDispatcher.Get(queryResponseResult, (HttpStatusCode)statusCode);

                    if (queryResponseResult.TryGet("Results", out BlittableJsonReaderArray queryResult) is false)
                        throw new InvalidOperationException("Missing Results from query output"); // TODO: shouldn't happen, but add error handling

                    document.Messages.Add(context.ReadObject(
                        new DynamicJsonValue { ["tool_call_id"] = toolCallsIds[i], ["role"] = "tool", ["content"] = queryResult.ToString() }, "tool-call/response"));
                }
            }
        }
        public async Task<string> TryPersistAsync(JsonOperationContext context, AiAgentConfiguration configuration, string conversationId, ConversationDocument conversation)
        {
            var changeVectorLsv = context.GetLazyString(conversation.ChangeVector);
            if (configuration.Persistence is not null)
            {
                // we don't pass change vector here, so last write wins
                MergedPutCommand putCmd = new(conversation.ToBlittable(context, configuration), conversationId, changeVectorLsv, RequestHandler.Database);
                await RequestHandler.Database.TxMerger.Enqueue(putCmd);
                conversation.ChangeVector = putCmd.PutResult.ChangeVector;
                return putCmd.PutResult.Id;
            }

            return null;
        }
    }
}
