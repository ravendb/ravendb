using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.AI.AiGen;
using Raven.Server.ServerWide.Context;
using System.Net;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Handlers.Processors.MultiGet;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;
namespace Raven.Server.Documents.Handlers.AI.Agents
{
    internal abstract class AbstractSingleAiAgentProcessor : AbstractAiAgentProcessor<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AbstractSingleAiAgentProcessor([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.Database.ServerStore.ContextPool)
        {
        }

        protected override async Task HandleQueryToolCallsAsync(JsonOperationContext context, AiAgentConfiguration cfg, ChatDocument document, AiResponse result)
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
                if (resp.TryGet("Results", out BlittableJsonReaderArray results) is false)// TODO: shouldn't happen, but add error handling
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

                    document.Messages.Add(context.ReadObject(new DynamicJsonValue
                    {
                        ["tool_call_id"] = toolCallsIds[i],
                        ["role"] = "tool",
                        ["content"] = queryResult.ToString()
                    }, "tool-call/response"));
                }
            }
        }
        
        
        public async Task<string> TryPersistAsync(AiAgentConfiguration configuration, string chatId, BlittableJsonReaderObject docBjro)
        {
            if (configuration.Persistence is not null)
            {
                // we don't pass change vector here, so last write wins
                MergedPutCommand putCmd = new(docBjro, chatId, changeVector: null, RequestHandler.Database);
                await RequestHandler.Database.TxMerger.Enqueue(putCmd);
                return putCmd.PutResult.Id;
            }
            return null;
        }
    }
}
