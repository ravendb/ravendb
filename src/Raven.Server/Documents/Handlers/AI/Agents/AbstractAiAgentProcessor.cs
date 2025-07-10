using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.AiGen;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;
using System.Net;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.Handlers.Processors.MultiGet;
using Sparrow;

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
            string chatId, 
            ChatDocument chatDocument, 
            RequestBody body,
            CancellationToken token)
        {
            var hasActionResponse = body.ActionResponses is { Length: > 0 };
            var hasUserPrompt = string.IsNullOrEmpty(body.UserPrompt) == false;

            if (hasActionResponse && hasUserPrompt)
                throw new InvalidOperationException($"Cannot have a chat '{chatId}' with open tool calls and user prompt.");

            if (body.ActionResponses != null)
            {
                foreach (BlittableJsonReaderObject tool in body.ActionResponses)
                {
                    var t = JsonDeserializationClient.ToolResponse(tool);
                    if (chatDocument.OpenToolCalls.Remove(t.ToolId) == false)
                        throw new InvalidOperationException($"{t.ToolId} is an unknown tool ID for chat '{chatId}'");

                    chatDocument.Messages.Add(context.ReadObject(new DynamicJsonValue { ["tool_call_id"] = t.ToolId, ["role"] = "tool", ["content"] = t.Content },
                        "user/tool"));
                }
            }

            if (chatDocument.OpenToolCalls.Count > 0)
            {
                await TryPersistAsync(context, configuration, chatId, chatDocument);
                await WriteResponseAsync(context, chatId, (Response: null, chatDocument));
                return;
            }

            if (hasActionResponse == false && hasUserPrompt == false)
                throw new InvalidOperationException($"Cannot have a chat '{chatId}' without open tool calls or user prompt.");

            if (string.IsNullOrEmpty(body.UserPrompt) == false)
            {
                chatDocument.AddMessage(context, context.ReadObject(new DynamicJsonValue { ["role"] = "user", ["content"] = body.UserPrompt }, "user/msg"));
            }

            var r = await TalkAsync(context, configuration, chatDocument, token: token);

            chatId = await TryPersistAsync(context, configuration, chatId, r.Document);
            await WriteResponseAsync(context, chatId, r);
        }

        public override async ValueTask ExecuteAsync()
        {
            using var token = RequestHandler.CreateHttpRequestBoundOperationToken();
            var chatId = RequestHandler.GetStringQueryString("chatId", required: false);
            var agent = RequestHandler.GetStringQueryString("id", required: false);

            if (string.IsNullOrEmpty(chatId) && string.IsNullOrEmpty(agent))
                throw new ArgumentException("Chat ID or agent name must be provided.");

            if (string.IsNullOrEmpty(chatId) == false && string.IsNullOrEmpty(agent) == false)
                throw new ArgumentException($"Chat '{chatId}' and agent '{agent}' can't be provided together.");

            using var _ = ContextPool.AllocateOperationContext(out DocumentsOperationContext context);
            var body = await ReadRequestBodyAsync(context, token.Token);

            ChatDocument chatDocument = null;
            AiAgentConfiguration configuration = null;

            if (string.IsNullOrEmpty(chatId) == false)
            {
                using var __ = context.OpenReadTransaction();
                var chat = RequestHandler.Database.DocumentsStorage.Get(context, chatId);
                if (chat == null)
                    throw new DocumentDoesNotExistException(chatId);

                chatDocument = ChatDocument.ToDocument(chatId, chat.Data);
                configuration = GetAiAgentConfiguration(chatDocument.Agent);
            }

            if (string.IsNullOrEmpty(agent) == false)
            {
                configuration = GetAiAgentConfiguration(agent);
                chatDocument = new ChatDocument(agent, body.Parameters);
                chatDocument.Initialize(context, configuration, body.UserPrompt);
                chatId = BuildChatId(configuration);
            }

            await HandleRequest(context, configuration, chatId, chatDocument, body, token.Token);
        }

        private string BuildChatId(AiAgentConfiguration configuration)
        {
            var agentPrefix = $"{configuration.Identifier}{RequestHandler.IdentityPartsSeparator}";
            var collection = configuration.Persistence?.Collection ?? Constants.Documents.Collections.AiAgentChatCollection;

            return $"{agentPrefix}{collection}{RequestHandler.IdentityPartsSeparator}";
        }

        public async Task<RequestBody> ReadRequestBodyAsync(JsonOperationContext context, CancellationToken token)
        {
            var body = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "ai-agent", token);
            body.TryGet(nameof(ChatRequestBody.ToolResponses), out BlittableJsonReaderArray actionResponses);
            body.TryGet(nameof(ChatRequestBody.UserPrompt), out string userPrompt);
            body.TryGet(nameof(ChatRequestBody.Parameters), out BlittableJsonReaderObject parameters);

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

        public async Task<(BlittableJsonReaderObject Response, ChatDocument Document)> TalkAsync(JsonOperationContext context, AiAgentConfiguration configuration,
            ChatDocument document, CancellationToken token)
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

        private bool TryGetUserTools(JsonOperationContext context, ChatDocument document, AiAgentConfiguration configuration, AiResponse result)
        {
            foreach (var call in result.ToolCalls)
            {
                if (configuration.FindAction(call.Name) == null)
                    continue;

                document.OpenToolCalls.Add(call.Id,
                    new ToolRequest { ToolId = call.Id, Name = call.Name, Arguments = CreateParameters(context, call, document.Parameters).ToString() });
            }

            return document.OpenToolCalls.Count > 0;
        }

        public virtual async Task WriteResponseAsync(JsonOperationContext context, string conversationId, (BlittableJsonReaderObject Response, ChatDocument Document) r)
        {
            var output = new DynamicJsonValue
            {
                [nameof(ChatResult<object>.ChatId)] = conversationId,
                [nameof(ChatResult<object>.Response)] = r.Response,
                [nameof(ChatResult<object>.ToolRequests)] = new DynamicJsonArray(r.Document.OpenToolCalls.Select(t => t.Value.ToJson())),
                [nameof(ChatResult<object>.Usage)] = r.Document.TotalUsage.ToJson()
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

        public async Task HandleQueryToolCallsAsync(JsonOperationContext context, AiAgentConfiguration cfg, ChatDocument document, AiResponse result)
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
        public async Task<string> TryPersistAsync(JsonOperationContext context, AiAgentConfiguration configuration, string chatId, ChatDocument chat)
        {
            if (configuration.Persistence is not null)
            {
                // we don't pass change vector here, so last write wins
                MergedPutCommand putCmd = new(chat.ToBlittable(context, configuration), chatId, changeVector: null, RequestHandler.Database);
                await RequestHandler.Database.TxMerger.Enqueue(putCmd);
                return putCmd.PutResult.Id;
            }

            return null;
        }
    }
}
