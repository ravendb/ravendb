using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.IO;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.AI.AiGen;
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
    [RavenAction("/databases/*/ai/rag", "POST", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
    public async Task Rag()
    {
        using var _ = ContextPool.AllocateOperationContext(out DocumentsOperationContext context);
        using var token = CreateHttpRequestBoundOperationToken();
        var options = await context.ReadForMemoryAsync(RequestBodyStream(), "ai/rag", token.Token);
        var cfg = JsonDeserializationClient.AiRagConfiguration(options);
        var conStr = GetAiConnectionString(cfg.ConnectionStringName);
        options.TryGet("Parameters", out BlittableJsonReaderObject parameters);

        (string url, string model, string apikey) = conStr.GetSettings();

        string schemaOrSampleObject = cfg.OutputSchema ?? throw new InvalidOperationException("Missing output schema in configuration");
        string schema = AbstractChatCompletionClient.GetSchemaFor(schemaOrSampleObject);
        using var client = new AbstractChatCompletionClient(new Uri(url), model, apikey,
            schema);

        List<AiMessage> msgs =
        [
            new(AiMessageType.System){Message = cfg.SystemPrompt},
            new(AiMessageType.User) { Message = cfg.UserPrompt},
        ];
        DynamicJsonArray tools = GenerateTools(cfg, context);

        AiResponse result;
        while (true)
        {
            result = await client.CompleteAsync2(
                context,
                msgs,
                tools,
                token.Token
            );
            if (result.Type is AiResponseType.Result)
                break;
            
            // add the call to the messages, so the model will know it called it
            msgs.Add(new AiMessage(AiMessageType.Tool)
            {
                ToolCalls = result.ToolCalls
            });

            // TODO: handle a response that does both query & action
            DynamicJsonArray reqs = [];
            var queryUrl = $"/databases/{DatabaseName}/queries";
            var index = msgs.Count;
            foreach (var call in result.ToolCalls)
            {
                var q = cfg.FindQuery(call.Name);
                msgs.Add(new AiMessage(AiMessageType.ToolReply)
                {
                    ToolCallId = call.Id,
                });

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

                    msgs[index + i].Message = queryResult.ToString();//YUCK: any better way? 
                }
            }
        }

        await using var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream());
        writer.WriteStartObject();
        writer.WritePropertyName("Result");
        writer.WriteObject(result.Result);
        writer.WriteComma();
        writer.WritePropertyName("Usage");
        result.Usage.Write(writer);
        writer.WriteEndObject();
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

    private static DynamicJsonArray GenerateTools(AiRagConfiguration cfg, DocumentsOperationContext context)
    {
        DynamicJsonArray tools = [];
        foreach (var q in cfg.Queries ?? [])
        {
            string paramsSchema = AbstractChatCompletionClient.GenerateJsonObjectFromSampleObject(q.ParametersSchema);
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
        foreach (var a in cfg.Actions?? [])
        {
            string paramsSchema = AbstractChatCompletionClient.GenerateJsonObjectFromSampleObject(a.ParametersSchema);
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

        return tools;
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
