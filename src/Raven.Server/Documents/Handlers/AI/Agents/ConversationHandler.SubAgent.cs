using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.Handlers.Processors.MultiGet;
using Raven.Server.Extensions;
using Raven.Server.Web;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.Handlers.AI.Agents
{
    public partial class ConversationHandler
    {
        private readonly Dictionary<string, AiAgentActionRequest> _childUserCalls = [];

        private async Task HandleSubAgentCalls(JsonOperationContext context, Dictionary<string, SubAgentActionResponse> subAgentsActions)
        {
            if (subAgentsActions?.Count > 0 == false)
                return;

            Dictionary<string, List<(AiToolCall, DynamicJsonValue)>> reqs = new();

            foreach (var (conversationId, subAgent) in subAgentsActions)
            {
                var call = new AiToolCall(subAgent.ParentId /*Parent call*/, subAgent.Agent, Arguments: null);
                var r = CreateAgentRequest(subAgent.Agent, conversationId, prompt: null, subAgent.Responses, creationOptions: new DynamicJsonValue());
                reqs.GetOrAdd(conversationId).Add((call, r));
            }

            await ExecuteMultiAgentAndQueryRequestsAsync(context, reqs);

            if (_childUserCalls.Count > 0)
                return;

            List<AiToolCall> activeToolCalls = [];
            foreach (var action in _document.OpenActionCalls)
            {
                var call = new AiToolCall(action.Key, action.Value.Name, action.Value.Arguments);
                activeToolCalls.Add(call);
            }

            await HandleQueryAndAgentCallsAsync(context, activeToolCalls);
        }

        private void BuildAgentRequest(JsonOperationContext context, ConversationDocument document, AiToolCall call, AiAgentToolSubAgent agent, Dictionary<string, List<(AiToolCall, DynamicJsonValue)>> reqs)
        {
            var args = context.Sync.ReadForMemory(call.Arguments, "call/args");
            if (args.TryGet(ConversationDocument.SubAgentUserPromptKey, out string prompt) is false)
            {
                throw new InvalidOperationException($"Missing required 'subAgentUserPrompt' parameter on call to {call.Name}. Arguments: {call.Arguments}.");
            }

            args.Modifications = new DynamicJsonValue(args);
            args.Modifications.Remove(ConversationDocument.SubAgentUserPromptKey);

            var parameters = MergeParams(context, document.Parameters, args);
            var subConversationParamsHash = call.Name + "/" + AttachmentsStorageHelper.CalculateHash(parameters.AsSpan());
            // Unique conversation identifier for this sub-agent (includes document ID, call name, and index).
            var conversationId = document.Id + "/" + subConversationParamsHash;

            reqs.GetOrAdd(conversationId).Add((call, CreateAgentRequest(agent.Identifier, conversationId,
                prompt, Array.Empty<object>(), new DynamicJsonValue
                {
                    [nameof(AiConversationCreationOptions.Parameters)] = parameters,
                    [nameof(AiConversationCreationOptions.ExpirationInSec)] = document.Expires switch
                    {
                        { } td => (int)td.TotalSeconds,
                        null => null
                    },
                    [nameof(AiConversationCreationOptions.MaxModelIterationsPerCall)] = _document.RemainingToolIterations
                })));

            _document.OpenActionCalls.TryAdd(call.Id, new AiAgentActionRequest
            {
                ToolId = call.Id, // Parent call
                Name = call.Name,
                Type = AiAgentActionRequestType.SubAgent,
                Arguments = call.Arguments,
                SubConversation = conversationId
            });
        }

        private DynamicJsonValue CreateAgentRequest(string agent, string conversationId, string prompt, IEnumerable<object> actionResponses, DynamicJsonValue creationOptions)
        {
            var queryString = new StringBuilder("?")
                .Append("&conversationId=").Append(Uri.EscapeDataString(conversationId))
                .Append("&agentId=").Append(Uri.EscapeDataString(agent))
                .ToString();

            return new DynamicJsonValue
            {
                [nameof(GetRequest.Url)] = $"/databases/{database.Name}/ai/agent",
                [nameof(GetRequest.Query)] = queryString,
                [nameof(GetRequest.Method)] = "POST",
                [nameof(GetRequest.Content)] = new DynamicJsonValue
                {
                    [nameof(ConversionRequestBody.UserPrompt)] = prompt,
                    [nameof(ConversionRequestBody.ActionResponses)] = actionResponses,
                    [nameof(ConversionRequestBody.CreationOptions)] = creationOptions
                }
            };
        }

        private static SubAgentActionResponse GetOrAddSubAgentsActionResponses(Dictionary<string, SubAgentActionResponse> subAgentsActions, AiAgentActionRequest parent, string parentToolId)
        {
            if (subAgentsActions.TryGetValue(parent.SubConversation, out var subAgent))
            {
                Debug.Assert(subAgent.ParentId == parentToolId, $"subAgent.ParentId != rootToolId. subAgent.ParentId is '{subAgent.ParentId}',  rootToolId is '{parentToolId}'");
                Debug.Assert(subAgent.Agent == parent.Name, $"subAgent.Agent != action.Name. subAgent.Agent is '{subAgent.Agent}', action.Name is '{parent.Name}'");
            }
            else
            {
                subAgent = subAgentsActions[parent.SubConversation] = new SubAgentActionResponse()
                {
                    ParentId = parentToolId,
                    Agent = parent.Name,
                    Responses = new()
                };
            }

            return subAgent;
        }

        private async Task<SubConversationResult> ExecuteSingleSubConversationToolCallsAsync(string conversationId, List<(AiToolCall Call, DynamicJsonValue Req)> requests)
        {
            IDisposable disposable = null;

            try
            {
                disposable = database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context);

                var result = new SubConversationResult(disposable);

                await foreach (var (getRequestResult, i) in ExecuteMultiRequestsAsync(context, new DynamicJsonArray(requests.Select(x => x.Req))))
                {
                    var currentCall = requests[i].Call;
                    var toolCall = FindToolFrom(_configuration, currentCall.Name);
                    if (toolCall == null)
                        throw new InvalidOperationException($"Ai-Agent has no tool in name '{currentCall.Name}'");

                    switch (toolCall)
                    {
                        case AiAgentToolSubAgent:
                            try
                            {
                                var subAgentResult = getRequestResult.Invoke();
                                if (TryCloseSubAgentCall(context, conversationId, subAgentResult, currentCall, result) == false)
                                    return result;
                            }
                            catch (Exception e)
                            {
                                result.Messages.Add(context.ReadObject(
                                        new DynamicJsonValue
                                        {
                                            ["tool_call_id"] = currentCall.Id,
                                            ["role"] = "tool",
                                            ["content"] = "Error has been occurred during the tool call execution: " + e.Message,
                                            ["subConversation"] = conversationId,
                                        }, "tool-call/response"));
                                result.OpenToolCallsToRemove.Add(currentCall.Id);
                            }
                            break;
                        case AiAgentToolQuery:
                            var requestResult = getRequestResult.Invoke();
                            if (requestResult.TryGet(nameof(QueryResult.Results), out BlittableJsonReaderArray queryResult) is false)
                                throw new InvalidOperationException($"Query output is missing the '{nameof(QueryResult.Results)}' field. (Query - Id: {currentCall.Id}, Name: {currentCall.Name})");

                            result.Messages.Add(context.ReadObject(
                                new DynamicJsonValue
                                {
                                    ["tool_call_id"] = currentCall.Id,
                                    ["role"] = "tool",
                                    ["content"] = GetToolResultContent(queryResult)
                                }, "tool-call/response"));
                            break;
                        default:
                            throw new InvalidOperationException(
                                $"Type mismatch for tool '{currentCall.Name}' in sub-conversation '{conversationId}'. " +
                                $"Expected type: '{nameof(AiAgentToolSubAgent)}' or '{nameof(AiAgentToolQuery)}', Actual type: '{toolCall.GetType().Name}'.");
                    }
                }

                return result;
            }
            catch
            {
                disposable?.Dispose();
                throw;
            }
        }

        private bool TryCloseSubAgentCall(JsonOperationContext context, string conversationId, BlittableJsonReaderObject requestResult, AiToolCall currentCall,
    SubConversationResult result)
        {
            if (requestResult.TryGet(nameof(ConversationResult<object>.Response), out BlittableJsonReaderObject agentResult) is false)
                throw new InvalidOperationException($"Sub-agent query output is missing the '{nameof(ConversationResult<object>.Response)}' field inside '{nameof(QueryResult.Results)}'. (Query - Id: {currentCall.Id}, Name: {currentCall.Name})");
            if (requestResult.TryGet(nameof(ConversationResult<object>.ActionRequests), out BlittableJsonReaderArray actionRequests) is false)
                throw new InvalidOperationException($"Sub-agent query output is missing the '{nameof(ConversationResult<object>.ActionRequests)}' field inside '{nameof(QueryResult.Results)}'. (Query - Id: {currentCall.Id}, Name: {currentCall.Name})");
            if (requestResult.TryGet(nameof(ConversationResult<object>.ToolsIterations), out int callToolsIterations) is false)
                throw new InvalidOperationException($"Sub-agent query output is missing the '{nameof(ConversationResult<object>.ToolsIterations)}' field inside '{nameof(QueryResult.Results)}'. (Query - Id: {currentCall.Id}, Name: {currentCall.Name})");

            result.ToolsIterations += callToolsIterations;

            if (actionRequests?.Length > 0)
            {
                foreach (BlittableJsonReaderObject req in actionRequests)
                {
                    var actionRequest = JsonDeserializationClient.ActionRequest(req);
                    var newActionCall = new AiAgentActionRequest
                    {
                        ToolId = currentCall.Id, // Parent call
                        Name = currentCall.Name + "/" + actionRequest.Name,
                        Type = AiAgentActionRequestType.UserAction,
                        Arguments = actionRequest.Arguments,
                    };

                    AddChildrenUserCall(result.ChildUserCalls, actionRequest.ToolId, newActionCall);
                }

                return false;
            }

            result.Messages.Add(context.ReadObject(
                new DynamicJsonValue
                {
                    ["tool_call_id"] = currentCall.Id,
                    ["role"] = "tool",
                    ["content"] = agentResult.ToString(),
                    ["subConversation"] = conversationId,
                }, "tool-call/response"));

            result.OpenToolCallsToRemove.Add(currentCall.Id);

            return true;
        }

        private static void AddChildrenUserCall(Dictionary<string, AiAgentActionRequest> childUserCalls, string subToolId, AiAgentActionRequest newActionCall)
        {
            if (childUserCalls.TryAdd(subToolId, // Sub call
                    newActionCall) == false)
            {
                // Already exists
                var existingActionCall = childUserCalls[subToolId];
                Debug.Assert(newActionCall.IsEqual(existingActionCall),
                    $"Mismatch detected in OpenActionCalls for key '{subToolId}'.\n" +
                    $"--- NEW ACTION CALL ---\n{newActionCall}\n\n" +
                    $"--- EXISTING ACTION CALL ---\n{existingActionCall}\n\n" +
                    "The existing ActionCall does not match the newly attempted ActionCall insertion.\n" +
                    "If this mismatch is valid, ensure higher-level logic prevents conflicting ActionCalls with the same ToolId."
                );
            }
        }

        private async IAsyncEnumerable<(Func<BlittableJsonReaderObject>, int)> ExecuteMultiRequestsAsync(JsonOperationContext context, DynamicJsonArray reqs)
        {
            var multiGetHandler = new MultiGetHandler();
            multiGetHandler.Init(new RequestHandlerContext
            {
                Database = database,
                RavenServer = server.Server,
                HttpContext = new DefaultHttpContext()
            });

            multiGetHandler.HttpContext.Features.Set<IHttpAuthenticationFeature>(Authentication);

            using (var reqsBlittable = context.ReadObject(new DynamicJsonValue { ["Requests"] = reqs }, "ai-agent/multi-query"))
            using (var handler = new MultiGetHandlerProcessorForPost(multiGetHandler))
            using (var memoryStream = RecyclableMemoryStreamFactory.GetRecyclableStream())
            {
                await handler.ExecuteMultiGetAsync(context, reqsBlittable, memoryStream);
                memoryStream.Position = 0;
                using var resp = context.Sync.ReadForMemory(memoryStream, "query/response");
                if (resp.TryGet("Results", out BlittableJsonReaderArray results) is false)
                    throw new InvalidOperationException("Missing Results from multi-get reply");

                for (int i = 0; i < results.Length; i++)
                {
                    var response = (BlittableJsonReaderObject)results[i];
                    if (response.TryGet(nameof(GetResponse.StatusCode), out int statusCode) == false)
                        throw new InvalidOperationException("Missing status code");
                    if (response.TryGet(nameof(GetResponse.Result), out BlittableJsonReaderObject requestResult) is false)
                        throw new InvalidOperationException("Missing Result from query request output");

                    yield return (() =>
                    {
                        if (statusCode != 200)
                            throw ExceptionDispatcher.Get(requestResult, (HttpStatusCode)statusCode);
                        return requestResult;
                    }, i);
                }
            }
        }

        private sealed class SubConversationResult
        {
            public IDisposable Disposable { get; }
            public List<BlittableJsonReaderObject> Messages { get; } = new();
            public List<string> OpenToolCallsToRemove { get; } = new();
            public Dictionary<string, AiAgentActionRequest> ChildUserCalls { get; } = new();
            public int ToolsIterations { get; set; }

            public SubConversationResult(IDisposable disposable)
            {
                Disposable = disposable;
            }
        }

    }
}
