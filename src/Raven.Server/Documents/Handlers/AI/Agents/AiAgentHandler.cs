using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.AiGen;
using Raven.Server.Documents.Handlers.Processors.MultiGet;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.Handlers.AI.Agents;

public class AiAgentHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/admin/ai/agent", "PUT", AuthorizationStatus.DatabaseAdmin)]
    public async Task AddOrModifyAiAgent()
    {
        using (var process = new AiAgentProcessorForAddOrUpdateAiAgent<DatabaseRequestHandler, DocumentsOperationContext>(this))
        {
            await process.ExecuteAsync();
        }
    }


    [RavenAction("/databases/*/admin/ai/agent", "DELETE", AuthorizationStatus.DatabaseAdmin)]
    public async Task DeleteAiAgent()
    {
        using (var process = new AiAgentProcessorForDeleteAiAgent<DatabaseRequestHandler, DocumentsOperationContext>(this))
        {
            await process.ExecuteAsync();
        }
    }

    [RavenAction("/databases/*/ai/agent/start", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
    public async Task StartChat()
    {
        using var token = CreateHttpRequestBoundOperationToken();
        var name = GetStringQueryString("name", required: true);

        var configuration = GetAiAgentConfiguration(name);

        using var _ = ContextPool.AllocateOperationContext(out JsonOperationContext context);
        var options = await context.ReadForMemoryAsync(RequestBodyStream(), "ai-agent", token.Token);
        var body = GetStartChatOptions(options);
        var chat = new ChatDocument(name, body.Parameter);
        
        chat.Initialize(context, configuration.SystemPrompt, body.UserPrompt);
        var r = await Talk(context, configuration, chat, token);

        string conversationId = null;
        if (configuration.Persistence is not null)
        {
            MergedPutCommand putCmd = new(r.Docoument, $"{configuration.Persistence.Collection}{Database.IdentityPartsSeparator}", null, Database);
            await Database.TxMerger.Enqueue(putCmd);
            conversationId = putCmd.PutResult.Id;
        }

        await WriteResponseAsync(context, conversationId, r);
    }

    [RavenAction("/databases/*/ai/agent/resume", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
    public async Task ResumeChat()
    {
        using var token = CreateHttpRequestBoundOperationToken();
        var chatId = GetStringQueryString("chatId", required: true);

        using var _ = Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context);
        using var __ = context.OpenReadTransaction();

        var chat = Database.DocumentsStorage.Get(context, chatId);
        if (chat == null)
            throw new DocumentDoesNotExistException(chatId);

        var chatDocument = ChatDocument.ToDocument(chatId, chat.Data);
        var body = await ReadResumeChatBodyAsync(context, token.Token);

        var configuration = GetAiAgentConfiguration(chatDocument.Agent);

        AddNewMessages();

        var r = await Talk(context, configuration, chatDocument, token: token);

        if (configuration.Persistence is not null)
        {
            // we don't pass change vector here, so last write wins
            MergedPutCommand putCmd = new(r.Docoument, chatId, changeVector: null, Database);
            await Database.TxMerger.Enqueue(putCmd);
        }

        await WriteResponseAsync(context, chatId, r);

        void AddNewMessages()
        {
            if (string.IsNullOrEmpty(body.UserPrompt) == false)
            {
                chatDocument.AddMessage(context, context.ReadObject(new DynamicJsonValue
                {
                    ["role"] = "user",
                    ["content"] = body.UserPrompt
                }, "user/msg"));
            }

            if (body.ActionResponse != null)
            {
                foreach (BlittableJsonReaderObject tool in body.ActionResponse)
                {
                    var t = JsonDeserializationClient.ToolResponse(tool);
                    chatDocument.Messages.Add(context.ReadObject(new DynamicJsonValue
                    {
                        ["tool_call_id"] = t.ToolId,
                        ["role"] = "tool",
                        ["content"] = t.Content
                    }, "user/tool"));
                }
            }
        }
    }

    [RavenAction("/databases/*/admin/ai/agent", "GET", AuthorizationStatus.DatabaseAdmin)]
    public async Task GetAiAgentConfiguration()
    {
        using var token = CreateHttpRequestBoundOperationToken();
        var name = GetStringQueryString("name");

        Dictionary<string, AiAgentConfiguration> agents;
        using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
        using (ctx.OpenReadTransaction())
        using (var record = ServerStore.Cluster.ReadRawDatabaseRecord(ctx, DatabaseName))
        {
            agents = record.AiAgents;
        }

        if (string.IsNullOrEmpty(name) == false)
        {
            if (agents.TryGetValue(name, out var configuration) == false)
                throw new ArgumentException($"AI Agent '{name}' doesn't exists");

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream(), token.Token))
            {
                var obj = context.ReadObject(configuration.ToJson(), "get-ai-agent");
                writer.WriteObject(obj);
            }

            return;
        }

        using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream(), token.Token))
        {
            writer.WriteStartObject();

            var count = agents.Count;
            var current = 0;
            foreach (var agent in agents)
            {
                current++;
                writer.WritePropertyName(agent.Key);
                writer.WriteObject(context.ReadObject(agent.Value.ToJson(), "ai-agent"));
                if (current < count)
                {
                    writer.WriteComma();
                }
            }
            writer.WriteEndObject();
        }
    }

    [RavenAction("/databases/*/ai/agent/test", "POST", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
    public async Task AiAgentTest()
    {
        using var _ = ContextPool.AllocateOperationContext(out DocumentsOperationContext context);
        using var token = CreateHttpRequestBoundOperationToken();
        var options = await context.ReadForMemoryAsync(RequestBodyStream(), "ai-agent", token.Token);
        var cfg = JsonDeserializationClient.AiAgentConfiguration(options);

        var body = GetStartChatOptions(options);
        var chat = new ChatDocument("test", body.Parameter);
        chat.Initialize(context, cfg.SystemPrompt, body.UserPrompt);

        var r = await Talk(context, cfg, chat, token);

        await WriteResponseAsync(context, "test", r);
    }
    private (BlittableJsonReaderObject Parameter, string UserPrompt) GetStartChatOptions(BlittableJsonReaderObject obj)
    {
        if (obj.TryGet(nameof(StartChatBody.Parameters), out BlittableJsonReaderObject parameters) == false)
            throw new ArgumentException(nameof(StartChatBody.Parameters));
        if (obj.TryGet(nameof(StartChatBody.Prompt), out string userPrompt) == false)
            throw new ArgumentException($"User prompt is missing");

        return (parameters, userPrompt);
    }

    private async Task<(BlittableJsonReaderArray ActionResponse, string UserPrompt)> ReadResumeChatBodyAsync(JsonOperationContext context, CancellationToken token)
    {
        var body = await context.ReadForMemoryAsync(RequestBodyStream(), "ai-agent", token);
        if (body.TryGet(nameof(ResumeChatBody.ToolResponse), out BlittableJsonReaderArray actionResponse) == false)
            throw new ArgumentException(nameof(ResumeChatBody.ToolResponse));
        if (body.TryGet(nameof(ResumeChatBody.UserPrompt), out string userPrompt) == false)
            throw new ArgumentException($"User prompt is missing");

        return (actionResponse, userPrompt);
    }

    private async Task<(AiUsage Usage, List<ToolRequest> userToolRequests, BlittableJsonReaderObject Response, BlittableJsonReaderObject Docoument)> Talk(JsonOperationContext context, AiAgentConfiguration configuration, ChatDocument document, OperationCancelToken token)
    {
        document.EnsureInitialized();

        var conStr = GetAiConnectionString(configuration.ConnectionStringName);

        string schemaOrSampleObject = configuration.OutputSchema ?? throw new InvalidOperationException("Missing output schema in configuration");
        string schema = ChatCompletionClient.GetSchemaFor(schemaOrSampleObject);
        using var client = ChatCompletionClient.CreateChatCompletionClient(Database.ServerStore.ContextPool, conStr, schema);

        var tools = document.GenerateTools(context, configuration);

        AiUsage usage = new();
        AiResponse aiResponse;
        List<ToolRequest> userToolRequests = null;

        while (true)
        {
            aiResponse = await client.CompleteAsync2(
                context,
                document.Messages,
                tools,
                usage,
                token.Token
            );
            if (aiResponse.Type is AiResponseType.Result)
                break;
            
            await HandleQueryToolCalls(context, configuration, document, aiResponse);

            if (TryGetUserTools(configuration, aiResponse, out userToolRequests))
                break; // we need to return the user tool requests to the client, so we can continue the conversation
        }

        document.UpdateUsage(usage);
        return (usage, userToolRequests, aiResponse.Result,document.ToBlittable(context, configuration));
    }

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

    private async Task HandleQueryToolCalls(JsonOperationContext context, AiAgentConfiguration cfg, ChatDocument document, AiResponse result)
    {
        // TODO: handle a response that does both query & action
        DynamicJsonArray reqs = [];
        List<string> toolCallsIds = [];
        var queryUrl = $"/databases/{DatabaseName}/queries";
        foreach (var call in result.ToolCalls)
        {
            var q = cfg.FindQuery(call.Name);
            if(q is null)
                continue;
            
            toolCallsIds .Add(call.Id);
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

        using var reqsBlittable = context.ReadObject(new DynamicJsonValue { ["Requests"] = reqs }, "ai-agent/multi-query");
        using MultiGetHandlerProcessorForPost handler = new(this);
        using (var memoryStream = RecyclableMemoryStreamFactory.GetRecyclableStream())
        {
            await handler.ExecuteMultiGetAsync(context, reqsBlittable, memoryStream);
            memoryStream.Position = 0;
            // TODO: have to verify that we got a successful result here!
            using var resp = context.Sync.ReadForMemory(memoryStream, "query/response");
            if (resp.TryGet("Results", out BlittableJsonReaderArray results) is false)// TODO: shouldn't happen, but add error handling
                throw new InvalidOperationException("Missing Results from multi-get reply");
            for (int i = 0; i < results.Length; i++)
            {
                var queryResponse = (BlittableJsonReaderObject)results[i];
                if (queryResponse.TryGet("StatusCode", out int statusCode) == false)
                    throw new InvalidOperationException("Missing status code"); // TODO: shouldn't happen, but add error handling
                if(queryResponse.TryGet("Result", out BlittableJsonReaderObject queryResponseResult) is false)
                    throw new InvalidOperationException("Missing Result from query request output"); // TODO: shouldn't happen, but add error handling

                if (statusCode != 200)
                    throw ExceptionDispatcher.Get(queryResponseResult, (HttpStatusCode)statusCode);

                if (queryResponseResult.TryGet("Results", out BlittableJsonReaderArray queryResult) is false)
                    throw new InvalidOperationException("Missing Results from query output"); // TODO: shouldn't happen, but add error handling

                document.Messages.Add(context.ReadObject(new DynamicJsonValue
                {
                    ["tool_call_id"] = toolCallsIds[i],
                    ["role"] = "tool",
                    ["content"] = queryResult.ToString()
                },"tool-call/response"));
            }
        }
    }

    private async Task WriteResponseAsync(JsonOperationContext context, string conversationId, (AiUsage Usage, List<ToolRequest> UserToolRequests, BlittableJsonReaderObject Response, BlittableJsonReaderObject Dcoument) r)
    {
        var output = new DynamicJsonValue
        {
            [nameof(ChatResult<object>.ChatId)] = conversationId,
            [nameof(ChatResult<object>.Response)] = r.Response,
            [nameof(ChatResult<object>.ToolRequests)] = r.UserToolRequests == null ? null : new DynamicJsonArray(r.UserToolRequests.Select(t => t.ToJson())),
            [nameof(ChatResult<object>.Usage)] = r.Usage.ToJson()
        };

        await using var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream());
        context.Write(writer, output);
    }

    private static BlittableJsonReaderObject CreateParameters(JsonOperationContext context, AiToolCall call, BlittableJsonReaderObject parameters)
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
    
    private AiAgentConfiguration GetAiAgentConfiguration(string name)
    {
        using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
        using (ctx.OpenReadTransaction())
        using (var record = ServerStore.Cluster.ReadRawDatabaseRecord(ctx, DatabaseName))
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
            return ServerStore.Cluster.ReadRawDatabaseRecord(serverCtx, DatabaseName).GetAiConnectionString(name)
                   ?? throw new InvalidOperationException("Cannot find connection string: " + name);
        }
    }
}
