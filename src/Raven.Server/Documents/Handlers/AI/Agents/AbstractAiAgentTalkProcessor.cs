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
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.Handlers.AI.Agents
{
    internal abstract class AbstractAiAgentTalkProcessor<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TOperationContext : JsonOperationContext
    {

        private IMemoryContextPool _contextPool;

        public AbstractAiAgentTalkProcessor([NotNull] TRequestHandler requestHandler, IMemoryContextPool contextPool) : base(requestHandler)
        {
            _contextPool = contextPool;
        }

        public (BlittableJsonReaderObject Parameter, string UserPrompt) GetStartChatOptions(BlittableJsonReaderObject obj)
        {
            if (obj.TryGet(nameof(StartChatBody.Parameters), out BlittableJsonReaderObject parameters) == false)
                throw new ArgumentException(nameof(StartChatBody.Parameters));
            if (obj.TryGet(nameof(StartChatBody.Prompt), out string userPrompt) == false)
                throw new ArgumentException("User prompt is missing");

            return (parameters, userPrompt);
        }

        public async Task<(BlittableJsonReaderArray ActionResponse, string UserPrompt)> ReadResumeChatBodyAsync(JsonOperationContext context, CancellationToken token)
        {
            var body = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "ai-agent", token);
            if (body.TryGet(nameof(ResumeChatBody.ToolResponse), out BlittableJsonReaderArray actionResponse) == false)
                throw new ArgumentException(nameof(ResumeChatBody.ToolResponse));
            if (body.TryGet(nameof(ResumeChatBody.UserPrompt), out string userPrompt) == false)
                throw new ArgumentException($"User prompt is missing");

            return (actionResponse, userPrompt);
        }

        public async Task<(AiUsage Usage, List<ToolRequest> userToolRequests, BlittableJsonReaderObject Response, BlittableJsonReaderObject Document)> TalkAsync(JsonOperationContext context, AiAgentConfiguration configuration, ChatDocument document, OperationCancelToken token)
        {
            document.EnsureInitialized();

            var conStr = GetAiConnectionString(configuration.ConnectionStringName);

            string schema;
            if (string.IsNullOrWhiteSpace(configuration.OutputSchema) == false)
            {
                schema = configuration.OutputSchema;
            }
            else if (string.IsNullOrWhiteSpace(configuration.SampleObject) == false)
            {
                schema = ChatCompletionClient.GetSchemaFromSampleObject(configuration.SampleObject);
            }
            else
                throw new InvalidOperationException("Missing output schema and sample object in configuration (there must be at least one of them)");

            using var client = ChatCompletionClient.CreateChatCompletionClient(_contextPool, conStr, schema);

            var tools = document.GenerateTools(context, configuration);

            AiUsage usage = new();
            AiResponse aiResponse;
            List<ToolRequest> userToolRequests = null;

            while (true)
            {
                aiResponse = await client.CompleteAsync(
                    context,
                    document.Messages,
                    tools,
                    usage,
                    token.Token
                );
                if (aiResponse.Type is AiResponseType.Result)
                    break;

                await HandleQueryToolCallsAsync(context, configuration, document, aiResponse);

                if (TryGetUserTools(configuration, aiResponse, out userToolRequests))
                    break; // we need to return the user tool requests to the client, so we can continue the conversation
            }

            document.UpdateUsage(usage);
            return (usage, userToolRequests, aiResponse.Result, document.ToBlittable(context, configuration));
        }

        // there’s a difference between sharded and single
        protected abstract Task HandleQueryToolCallsAsync(JsonOperationContext context, AiAgentConfiguration cfg, ChatDocument document, AiResponse result);

        private bool TryGetUserTools(AiAgentConfiguration configuration, AiResponse result, out List<ToolRequest> userTools)
        {
            userTools = [];
            foreach (var call in result.ToolCalls)
            {
                if (configuration.FindAction(call.Name) == null)
                    continue;

                userTools ??= [];
                userTools.Add(new ToolRequest
                {
                    ToolId = call.Id,
                    Name = call.Name,
                    Arguments = call.Arguments
                });
            }
            return userTools.Count > 0;
        }

        public async Task WriteResponseAsync(JsonOperationContext context, string conversationId, (AiUsage Usage, List<ToolRequest> UserToolRequests, BlittableJsonReaderObject Response, BlittableJsonReaderObject Dcoument) r)
        {
            var output = new DynamicJsonValue
            {
                [nameof(ChatResult<object>.ChatId)] = conversationId,
                [nameof(ChatResult<object>.Response)] = r.Response,
                [nameof(ChatResult<object>.ToolRequests)] = r.UserToolRequests == null ? null : new DynamicJsonArray(r.UserToolRequests.Select(t => t.ToJson())),
                [nameof(ChatResult<object>.Usage)] = r.Usage.ToJson()
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

        public AiAgentConfiguration GetAiAgentConfiguration(string name)
        {
            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
            using (ctx.OpenReadTransaction())
            using (var record = ServerStore.Cluster.ReadRawDatabaseRecord(ctx, RequestHandler.DatabaseName))
            {
                if (record.TryGetAiAgent(name, out var configuration) == false)
                    throw new ArgumentException($"AI Agent '{name}' doesn't exists");

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
    }
}
