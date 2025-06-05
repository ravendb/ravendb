using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.AI.AiGen;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Handlers.Processors.MultiGet;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.AI;

public class AiRagHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/ai/rag/test", "POST", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
    public async Task Rag()
    {
        using var _ = ContextPool.AllocateOperationContext(out DocumentsOperationContext context);
        using var token = CreateHttpRequestBoundOperationToken();
        var options = await context.ReadForMemoryAsync(RequestBodyStream(), "ai/rag", token.Token);
        var cfg = JsonDeserializationClient.AiRagConfiguration(options);
        var conStr = GetAiConnectionString(cfg.ConnectionStringName);
        options.TryGet("Parameters", out BlittableJsonReaderObject parameters);

        string schemaOrSampleObject = cfg.OutputSchema ?? throw new InvalidOperationException("Missing output schema in configuration");
        string schema = ChatCompletionClient.GetSchemaFor(schemaOrSampleObject);
        using var client = ChatCompletionClient.CreateChatCompletionClient(Database.ServerStore.ContextPool, conStr, schema);

        string userPrompt = GetStringQueryString("prompt");
        string id = GetStringQueryString("id");

        List<BlittableJsonReaderObject> msgs =
        [
            context.ReadObject(new DynamicJsonValue
            {
                ["role"] = "system",
                ["content"] = cfg.SystemPrompt
            }, "system/msg"),
            context.ReadObject(new DynamicJsonValue
            {
                ["role"] = "user",
                ["content"] = userPrompt
            }, "user/msg"),
        ];
        var tools = GenerateTools(cfg, context);

        AiUsage usage = new();
        AiResponse result;
        while (true)
        {
            result = await client.CompleteAsync2(
                context,
                msgs,
                tools,
                usage,
                token.Token
            );
            if (result.Type is AiResponseType.Result)
                break;
            
            await HandleToolCalls(context, msgs, result, cfg, parameters);
        }

        string conversationId = null;
        if (cfg.Persistence is not null)
        {
            var metadata = new DynamicJsonValue
            {
                ["@collection"] = cfg.Persistence.Collection,
            };
            if (cfg.Persistence.Expires is { } expire)
            {
                metadata["@expires"] = DateTime.UtcNow.Add(expire);
            }

            foreach (var msg in msgs)
            {
                if(msg.TryGet("role", out string role) is false)
                    continue;
                switch (role)
                {
                    case "tool":
                    { // TODO: assuming an array only here. 
                        if(msg.TryGet("content", out string content) is false)
                            continue;
                        var array = context.ParseBufferToArray(content, "tool-response", BlittableJsonDocumentBuilder.UsageMode.None);
                        msg.Modifications = new DynamicJsonValue(msg)
                        {
                            ["content"] = array
                        };
                        break;
                    }
                    case "assistant":
                    {
                        if (msg.TryGet("content", out string content) && content is not null)
                        {
                            //TODO: assuming an object only here
                            var obj = context.Sync.ReadForMemory(content, "assistant-response");
                            msg.Modifications = new DynamicJsonValue(msg)
                            {
                                ["content"] = obj
                            };
                        }

                        if (msg.TryGet("tool_calls", out BlittableJsonReaderArray toolCalls) && toolCalls is not null)
                        {
                            foreach (BlittableJsonReaderObject call in toolCalls)
                            {
                                if (call.TryGet("function", out BlittableJsonReaderObject function) && function is not null)
                                {
                                    if (function.TryGet("arguments", out string args) && args is not null)
                                    {
                                        var obj = context.Sync.ReadForMemory(args, "tool-arguments");
                                        function.Modifications = new DynamicJsonValue(function)
                                        {
                                            ["arguments"] = obj
                                        };          
                                    }
                                }
                            }
                        }
                        break;
                    }
                }
            }
            var conversation = new DynamicJsonValue
            {
                ["@metadata"] = metadata,
                ["Messages"] = msgs,
            };
            
            var docJson = context.ReadObject(conversation, id);
            MergedPutCommand putCmd = new(docJson, id, null, Database);
            await Database.TxMerger.Enqueue(putCmd);
            conversationId = putCmd.PutResult.Id;
        }
        
        await using var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream());
        writer.WriteStartObject();
        writer.WritePropertyName("Result");
        writer.WriteObject(result.Result);
        writer.WriteComma();
        writer.WritePropertyName("Usage");
        usage.Write(writer);
        writer.WriteComma();
        writer.WritePropertyName("ConversationId");
        writer.WriteString(conversationId);
        writer.WriteEndObject();
    }

    private async Task HandleToolCalls(DocumentsOperationContext context, List<BlittableJsonReaderObject> messages, AiResponse result, AiRagConfiguration cfg,
        BlittableJsonReaderObject parameters)
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
                    ["QueryParameters"] = CreateParameters(context, call, parameters)
                }
            });
        }

        using var reqsBlittable = context.ReadObject(new DynamicJsonValue { ["Requests"] = reqs }, "ai-rag/multi-query");
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
                if(queryResponse.TryGet("Result", out BlittableJsonReaderObject queryResponseResult) is false)
                    throw new InvalidOperationException("Missing Result from query request output"); // TODO: shouldn't happen, but add error handling
                if(queryResponseResult.TryGet("Results", out BlittableJsonReaderArray queryResult) is false)
                    throw new InvalidOperationException("Missing Results from query output"); // TODO: shouldn't happen, but add error handling

                messages.Add(context.ReadObject(new DynamicJsonValue
                {
                    ["tool_call_id"] = toolCallsIds[i],
                    ["role"] = "tool",
                    ["content"] = queryResult.ToString()
                },"tool-call/response"));
            }
        }
    }

    private static BlittableJsonReaderObject CreateParameters(DocumentsOperationContext context, AiToolCall call, BlittableJsonReaderObject parameters)
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

    private static BlittableJsonReaderArray GenerateTools(AiRagConfiguration cfg, DocumentsOperationContext context)
    {
        DynamicJsonArray tools = [];
        foreach (var q in cfg.Queries ?? [])
        {
            string paramsSchema = ChatCompletionClient.GenerateJsonObjectFromSampleObject(q.ParametersSchema);
            tools.Add(new DynamicJsonValue
            {
                ["type"] = "function",
                ["function"] = new DynamicJsonValue
                {
                    ["name"] = q.Name,
                    ["description"] = q.Description,
                    ["parameters"] = context.Sync.ReadForMemory(paramsSchema, "params/schema")
                },
                ["strict"] = true
            });
        }
        foreach (var a in cfg.Actions ?? [])
        {
            string paramsSchema = ChatCompletionClient.GenerateJsonObjectFromSampleObject(a.ParametersSchema);
            tools.Add(new DynamicJsonValue
            {
                ["type"] = "function",
                ["function"] = new DynamicJsonValue
                {
                    ["name"] = a.Name,
                    ["description"] = a.Description,
                    ["parameters"] = context.Sync.ReadForMemory(paramsSchema, "params/schema")
                },
                ["strict"] = true
            });
        }

        var obj = context.ReadObject(new DynamicJsonValue { ["_"] = tools }, "ai-rag/tools");
        BlittableJsonReaderObject.PropertyDetails prop = default;
        obj.GetPropertyByIndex(0, ref prop);
        return (BlittableJsonReaderArray)prop.Value;
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
