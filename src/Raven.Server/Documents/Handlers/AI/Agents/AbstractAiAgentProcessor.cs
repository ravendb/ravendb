using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
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
using Newtonsoft.Json;
using Raven.Client.Documents.AI;
using ChatConstants = Raven.Server.Documents.AI.ChatCompletionClient.Constants;

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

                    document.AddMessage(context,context.ReadObject(
                        new DynamicJsonValue
                        {
                            ["tool_call_id"] = t.ToolId, 
                            ["role"] = "tool", 
                            ["content"] = t.Content
                        },
                        "user/tool"), usage: null);
                }
            }

            if (document.OpenActionCalls.Count > 0)
            {
                // We have pending tool-call results from the user;
                // skip reduction - persist the document now without history,
                // ensuring we can recover if TalkAsync fails.
                await TryPersistAsync(context, configuration, conversationId, document, history: null);
                await WriteResponseAsync(context, conversationId, response: null, document);
                return;
            }

            if (hasActionResponse == false && hasUserPrompt == false)
                throw new InvalidOperationException($"Cannot have a conversation '{conversationId}' without open action calls or user prompt.");

            if (string.IsNullOrEmpty(body.UserPrompt) == false)
            {
                document.AddMessage(context, context.ReadObject(new DynamicJsonValue
                {
                    ["role"] = "user", 
                    ["content"] = body.UserPrompt
                }, "user/msg"), usage: null);
            }

            (BlittableJsonReaderObject Response, ConversationDocument Document, BlittableJsonReaderObject History) r;
            try
            {
                r = await TalkAsync(context, configuration, document, token: token);
            }
            catch (Exception e)
            {
                throw new AiException($"Failed to 'talk' with the agent '{configuration.Identifier}', conversation: '{conversationId}'.", e) { RequestId = null };
            }

            conversationId = await TryPersistAsync(context, configuration, conversationId, r.Document, r.History);
            await WriteResponseAsync(context, conversationId, r.Response, r.Document);
        }

        public override async ValueTask ExecuteAsync()
        {
            using var token = RequestHandler.CreateHttpRequestBoundOperationToken();
            var conversationId = RequestHandler.GetStringQueryString("conversationId");
            var agentId = RequestHandler.GetStringQueryString("agentId");
            var changeVector = RequestHandler.GetStringQueryString("changeVector", required: false);

            using var _ = ContextPool.AllocateOperationContext(out DocumentsOperationContext context);
            var body = await ReadRequestBodyAsync(context, token.Token);

            ConversationDocument conversationDocument = null;
            AiAgentConfiguration configuration = null;
            
            using(context.OpenReadTransaction())
            {
                var conversation = RequestHandler.Database.DocumentsStorage.Get(context, conversationId);
                if (conversation == null)
                {
                    if (string.IsNullOrEmpty(changeVector) == false)
                    {
                        throw new ConcurrencyException(
                            $"The conversation '{conversationId}' doesn't exists.")
                        {
                            ExpectedChangeVector = changeVector, 
                            ActualChangeVector = string.Empty, 
                            Id = conversationId
                        };
                    }

                    if (string.IsNullOrEmpty(body.UserPrompt))
                    {
                        throw new InvalidOperationException(
                            $"Cannot start a new conversation '{conversationId}' without a user prompt.");
                    }

                    conversationDocument = new ConversationDocument(agentId, body.Parameters);
                    configuration = GetAiAgentConfiguration(agentId);

                    if (body.Options.ConversationExpirationInSec.HasValue)
                    {
                        conversationDocument.Expires = TimeSpan.FromSeconds(body.Options.ConversationExpirationInSec.Value);
                    }
                
                    conversationDocument.Initialize(context, configuration);
                }
                else
                {
                    conversationDocument = ConversationDocument.ToDocument(conversationId, conversation.Data);
                    if (conversationDocument.Agent != agentId)
                        throw new InvalidOperationException(
                            $"The conversation '{conversationId}' is assigned to agent '{conversationDocument.Agent}', " +
                            $"but the request is for agent '{agentId}'.");

                    configuration = GetAiAgentConfiguration(conversationDocument.Agent);

                    if (changeVector != null)
                    {
                        if (conversation.ChangeVector != changeVector)
                            throw new ConcurrencyException(
                                $"The conversation '{conversationId}' was updated and doesn't match the expected change vector. Reload the conversation and try again.")
                            {
                                ExpectedChangeVector = changeVector, 
                                ActualChangeVector = conversation.ChangeVector, 
                                Id = conversationId
                            };

                        conversationDocument.ChangeVector = conversation.ChangeVector;
                    }
                }
            }

            await HandleRequest(context, configuration, conversationId, conversationDocument, body, token.Token);
        }

        public async Task<RequestBody> ReadRequestBodyAsync(JsonOperationContext context, CancellationToken token)
        {
            var body = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "ai-agent", token);
            body.TryGet(nameof(ConversionRequestBody.ActionResponses), out BlittableJsonReaderArray actionResponses);
            body.TryGet(nameof(ConversionRequestBody.UserPrompt), out string userPrompt);
            body.TryGet(nameof(ConversionRequestBody.Parameters), out BlittableJsonReaderObject parameters);
            body.TryGet(nameof(ConversionRequestBody.Options), out BlittableJsonReaderObject options);

            return new RequestBody
            {
                ActionResponses = actionResponses, 
                UserPrompt = userPrompt, 
                Parameters = parameters,
                Options = options != null ? JsonDeserializationClient.ConversationCreationOptions(options) : new AiConversationCreationOptions()
            };
        }

        public class RequestBody
        {
            public BlittableJsonReaderObject Parameters { get; set; }
            public string UserPrompt { get; set; }
            public BlittableJsonReaderArray ActionResponses { get; set; }
            public AiConversationCreationOptions Options { get; set; }

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

        private const int DefaultMaxModelIterationsPerCall = 16;
        private const int DefaultMaxTokensBeforeSummarization = 32 * 1024;
        private const int DefaultMaxTokensAfterSummarization = 1024;

        public async Task<(BlittableJsonReaderObject Response, ConversationDocument Document, BlittableJsonReaderObject History)> TalkAsync(JsonOperationContext context, AiAgentConfiguration configuration,
            ConversationDocument document, CancellationToken token)
        {
            document.EnsureInitialized();

            var conStr = GetAiConnectionString(configuration.ConnectionStringName);

            var schema = ChatCompletionClient.GetSchemaForRequest(configuration.OutputSchema, configuration.SampleObject);

            var tools = ConversationDocument.GenerateTools(context, configuration);

            AiResponse aiResponse;
            AiUsage aiUsage;
            using var client = ChatCompletionClient.CreateChatCompletionClient(ContextPool, conStr);
            var count = configuration.MaxModelIterationsPerCall ?? DefaultMaxModelIterationsPerCall;
            
            while (true)
            {
                aiUsage = new();
                using var request = client.CreateCompletionRequest(context, document.Messages, tools, useTools: count-- > 0, schema);

                aiResponse = await client.CompleteAsync(
                    context,
                    request,
                    aiUsage,
                    token
                );

                document.AddMessage(context, aiResponse.Message, aiUsage);
                document.UpdateUsage(aiUsage);
                if (aiResponse.Type is AiResponseType.Result)
                    break;

                await HandleQueryToolCallsAsync(context, configuration, document, aiResponse);

                if (TryGetUserTools(context, document, configuration, aiResponse))
                    break; // we need to return the user tool requests to the client, so we can continue the conversation
            }

            var history = await TryReduceChatSize();

            return (aiResponse.Result, document, history);


            async Task<BlittableJsonReaderObject> TryReduceChatSize()
            {
                var reduction = configuration.ChatTrimming;
                if (reduction == null || document.OpenActionCalls.Count > 0)
                    return null;

                TimeSpan? historyExpiration = reduction.History?.HistoryExpirationInSec == null
                    ? null
                    : TimeSpan.FromSeconds(reduction.History.HistoryExpirationInSec.Value);

                if (reduction.Truncate != null)
                {
                    if (document.Messages.Count > reduction.Truncate.MessagesLengthBeforeTruncate)
                    {
                        var truncateCount = document.Messages.Count - reduction.Truncate.MessagesLengthAfterTruncate;
                        truncateCount = int.Min(truncateCount, document.Messages.Count - 1); // prevent System.ArgumentException (out of bounds)
                        if (truncateCount > 0)
                        {
                            var chatBefore = reduction.History == null ? null : document.ToHistoryBlittable(context, configuration, historyExpiration);
                            document.Messages.RemoveRange(1, truncateCount);
                            return chatBefore;
                        }
                    }
                }
                else if (reduction.Tokens != null)
                {
                    reduction.Tokens.MaxTokensBeforeSummarization = configuration.ChatTrimming.Tokens.MaxTokensBeforeSummarization ?? 
                                                                    DefaultMaxTokensBeforeSummarization;
                    reduction.Tokens.MaxTokensAfterSummarization = configuration.ChatTrimming.Tokens.MaxTokensAfterSummarization ?? 
                                                                   DefaultMaxTokensAfterSummarization;

                    if (aiUsage.TotalTokens > reduction.Tokens.MaxTokensBeforeSummarization)
                    {
                        var chatBefore = reduction.History == null ? null : document.ToHistoryBlittable(context, configuration, historyExpiration);
                        await SummarizeAsync(context, client, configuration, document, token);
                        return chatBefore;
                    }
                }

                return null; // if reduction wasn't executed -> no history to persist (return null)
            }
        }

        private async Task SummarizeAsync(JsonOperationContext context, ChatCompletionClient client, AiAgentConfiguration configuration, ConversationDocument oldChat, CancellationToken token)
        {
            var summarization = configuration.ChatTrimming.Tokens;
            var systemPrompt = oldChat.Messages.FirstOrDefault();
            if (systemPrompt == null)
                throw new InvalidOperationException("Cannot perform summarization: the conversation's original system prompt is null.");

            if (systemPrompt.TryGet(ChatConstants.RequestFields.Content, out string _) == false)
                throw new InvalidOperationException($"Cannot perform summarization: the conversation's original system prompt has no '{ChatConstants.RequestFields.Content}' field.");

            var beginningPrompt = string.IsNullOrEmpty(summarization.SummarizationTaskBeginningPrompt)
                ? RequestHandler.Database.Configuration.Ai.SummarizationTaskBeginningPrompt
                : summarization.SummarizationTaskBeginningPrompt;
            beginningPrompt += $" The original system prompt was: {systemPrompt}, the rest of follows";

            var messages = new List<BlittableJsonReaderObject>()
            {
                context.ReadObject(
                    new DynamicJsonValue
                    {
                        [ChatConstants.RequestFields.Role] = ChatConstants.RequestFields.RoleSystemValue,
                        [ChatConstants.RequestFields.Content] = beginningPrompt,
                    }, "system/summary/msg"),
            };
            messages.AddRange(oldChat.Messages.Skip(1));

            var endPrompt = string.IsNullOrEmpty(summarization.SummarizationTaskEndPrompt)
                ? RequestHandler.Database.Configuration.Ai.SummarizationTaskEndPrompt
                : summarization.SummarizationTaskEndPrompt;
            messages.Add(context.ReadObject(
                new DynamicJsonValue
                {
                    [ChatConstants.RequestFields.Role] = ChatConstants.RequestFields.RoleUserValue,
                    [ChatConstants.RequestFields.Content] = endPrompt,
                    [ChatConstants.RequestFields.MaxCompletionToken] = summarization.MaxTokensAfterSummarization
                }, "system/summary/final/msg"));


            var usage = new AiUsage();
            var tools = ConversationDocument.GenerateTools(context, configuration);
            using var request = client.CreateCompletionRequest(context, messages, tools, useTools: false, SummarizationOutputSchema);
            var result = await client.CompleteAsync(context, request, usage, token);

            if (result.Result.TryGet(nameof(SummarizationSampleObject.Answer), out string messagesSummary) == false)
                throw new UnexpectedResponseException($"Unable to get a summary from response of agent '{oldChat.Agent}'.") { RequestId = null };

            oldChat.Messages.Clear();

            oldChat.Initialize(context, configuration);
            oldChat.AddMessage(context,
                context.ReadObject(
                    new DynamicJsonValue
                    {
                        [ChatConstants.RequestFields.Role] = ChatConstants.RequestFields.RoleAssistantValue,
                        [ChatConstants.RequestFields.Content] = summarization.ResultPrefix + messagesSummary
                    },
                    "system/msg"), usage);

            oldChat.UpdateUsage(usage);
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

        public virtual async Task WriteResponseAsync(JsonOperationContext context, string conversationId, BlittableJsonReaderObject response, ConversationDocument document)
        {
            var output = new DynamicJsonValue
            {
                [nameof(ConversationResult<object>.ConversationId)] = conversationId,
                [nameof(ConversationResult<object>.ChangeVector)] = document.ChangeVector,
                [nameof(ConversationResult<object>.Response)] = response,
                [nameof(ConversationResult<object>.ActionRequests)] = new DynamicJsonArray(document.OpenActionCalls.Select(t => t.Value.ToJson())),
                [nameof(ConversationResult<object>.TotalUsage)] = document.TotalUsage.ToJson()
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

                    document.AddMessage(context, context.ReadObject(
                        new DynamicJsonValue
                        {
                            ["tool_call_id"] = toolCallsIds[i], 
                            ["role"] = "tool", 
                            ["content"] = queryResult.ToString()
                        }, "tool-call/response"), usage: null);
                }
            }
        }

        public virtual async Task<string> TryPersistAsync(JsonOperationContext context, AiAgentConfiguration configuration, string conversationId, ConversationDocument conversation, BlittableJsonReaderObject history)
        {
            var changeVectorLsv = context.GetLazyString(conversation.ChangeVector);

            var cmd = new PutChatCommand(conversationId, conversation, history, changeVectorLsv, configuration, RequestHandler.Database);
            await RequestHandler.Database.TxMerger.Enqueue(cmd);
            conversation.ChangeVector = cmd.PutResult.Conversation.ChangeVector;
            return cmd.PutResult.Conversation.Id;
        }

        private static readonly string SummarizationOutputSchema = ChatCompletionClient.GetSchemaFromSampleObject(JsonConvert.SerializeObject(new SummarizationSampleObject()));

        private class SummarizationSampleObject
        {
            public string Answer = "Summary of the following chat messages history";
        }
    }
}
