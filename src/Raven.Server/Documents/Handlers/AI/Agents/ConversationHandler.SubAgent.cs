using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.AI;
using Raven.Server.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.Handlers.AI.Agents
{
    public partial class ConversationHandler
    {
        private readonly Dictionary<string, AiAgentActionRequest> _childUserCalls = [];

        private async Task HandleSubAgentCallsAsync(JsonOperationContext context, Dictionary<string, SubAgentActionResponse> subAgentsActions, CancellationToken token)
        {
            if (subAgentsActions?.Count > 0 == false)
                return;

            Dictionary<string, List<(AiToolCall, DynamicJsonValue)>> reqs = new();

            foreach (var (conversationId, subAgent) in subAgentsActions)
            {
                var call = new AiToolCall(subAgent.ParentId /*Parent call*/, subAgent.Agent, Arguments: null);
                var r = CreateAgentRequest(subAgent.Agent, conversationId, prompt: null, subAgent.Responses, creationOptions: new DynamicJsonValue(), _cancelPendingActionTools);
                reqs.GetOrAdd(conversationId).Add((call, r));
            }

            await ExecuteSubAgentAndQueryRequestsAsync(context, reqs, token);

            if (_childUserCalls.Count > 0)
                return;

            List<AiToolCall> activeToolCalls = [];
            foreach (var action in _document.OpenActionCalls)
            {
                var call = new AiToolCall(action.Key, action.Value.Name, action.Value.Arguments);
                activeToolCalls.Add(call);
            }

            await HandleQueryAndAgentCallsAsync(context, activeToolCalls, token);
        }

        private void BuildAgentRequest(JsonOperationContext context, ConversationDocument document, AiToolCall call, AiAgentToolSubAgent agent, Dictionary<string, List<(AiToolCall, DynamicJsonValue)>> reqs)
        {
            var args = context.Sync.ReadForMemory(call.Arguments, "call/args");
            if (args.TryGet(ConversationDocument.SubAgentUserPromptKey, out string prompt) is false)
            {
                throw new InvalidOperationException($"Missing required 'subAgentUserPrompt' parameter on call to {call.Name}. Arguments: {call.Arguments}.");
            }

            args.Modifications = new DynamicJsonValue(args);
            // this is going to be the prompt so we remove this from the parameters we pass to the sub-agent,
            // we want to keep the parameters clean and only with what the sub-agent needs to do its work
            args.Modifications.Remove(ConversationDocument.SubAgentUserPromptKey);

            var parameters = MergeParams(context, document.Parameters, args);

            // Hash only the parameters without the original prompt ('subAgentUserPrompt').
            // The prompt is sent separately, and excluding it keeps the same hash
            // so repeated calls with the same parameters (with the same values) continue the same sub-conversation.
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
                SubConversationId = conversationId
            });
        }

        private BlittableJsonReaderObject MergeParams(JsonOperationContext context, BlittableJsonReaderObject parentParameters, BlittableJsonReaderObject callParameters)
        {
            if (parentParameters is null)
                return callParameters;

            callParameters.Modifications ??= new DynamicJsonValue(callParameters);
            BlittableJsonReaderObject.PropertyDetails prop = default;

            for (int i = 0; i < parentParameters.Count; i++)
            {
                parentParameters.GetPropertyByIndex(i, ref prop);
                if (database.ForTestingPurposes?.ShouldAiAgentAddMutualParameterForSubAgentReq?.Invoke(_configuration, prop.Name) == false)
                    continue;

                callParameters.Modifications[prop.Name] = prop.Value;
            }

            return context.ReadObject(callParameters, "call/params");
        }

        protected virtual DynamicJsonValue CreateAgentRequest(string agent, string conversationId, string prompt, IEnumerable<object> actionResponses, DynamicJsonValue creationOptions, bool cancelPendingActionTools = false)
        {
            var queryStringBuilder = new StringBuilder("?")
                .Append("&conversationId=").Append(Uri.EscapeDataString(conversationId))
                .Append("&agentId=").Append(Uri.EscapeDataString(agent));

            // Always propagate the parent's current debug state to the sub-agent so the child
            // doesn't keep `Debug=true` sticky once the parent flips it off.
            queryStringBuilder.Append($"&debug={_document.Debug}");

            // When the parent is cancelling pending actions, propagate it so the sub-agent cancels
            // its own still-open action calls (recursively across all nesting levels).
            if (cancelPendingActionTools)
                queryStringBuilder.Append("&cancelPendingActionTools=true");

            var queryString = queryStringBuilder.ToString();

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
            if (subAgentsActions.TryGetValue(parent.SubConversationId, out var subAgent))
            {
                if (subAgent.ParentId != parentToolId)
                    throw new InvalidOperationException($"subAgent.ParentId mismatch. Expected '{parentToolId}', got '{subAgent.ParentId}'");

                if (subAgent.Agent != parent.Name)
                    throw new InvalidOperationException($"subAgent.Agent mismatch. Expected '{parent.Name}', got '{subAgent.Agent}'");
            }
            else
            {
                subAgent = subAgentsActions[parent.SubConversationId] = new SubAgentActionResponse()
                {
                    ParentId = parentToolId,
                    Agent = parent.Name,
                    Responses = new()
                };
            }

            return subAgent;
        }

        protected virtual bool TryCloseSubAgentCall(JsonOperationContext context, string conversationId, BlittableJsonReaderObject requestResult, AiToolCall currentCall,
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
                        SubConversationId = conversationId
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
                    ["subConversationId"] = conversationId,
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

                // If an entry already exists for this subToolId, it must be identical.
                // Different values indicate conflicting action calls for the same subToolId,
                var existingActionCall = childUserCalls[subToolId];
                if (newActionCall.IsEqual(existingActionCall) == false)
                    throw new InvalidOperationException(
                        $"Mismatch detected in OpenActionCalls for key '{subToolId}'.\n" +
                        $"--- NEW ACTION CALL ---\n{newActionCall}\n\n" +
                        $"--- EXISTING ACTION CALL ---\n{existingActionCall}\n\n" +
                        "The existing ActionCall does not match the newly attempted ActionCall insertion.\n" +
                        "If this mismatch is valid, ensure higher-level logic prevents conflicting ActionCalls with the same ToolId."
                    );
            }
        }

        public Dictionary<string, ParameterDefinition> BuildSubAgentParameters(JsonOperationContext context, AiAgentConfiguration parent, AiAgentConfiguration child)
        {
            var parameters = new Dictionary<string, ParameterDefinition>();
            // We only add what the sub-agent has that the root agent doesn't have
            // the mutual params will be added to the request when we create it
            foreach (var parameter in child.Parameters ?? [])
            {
                var parentParam = parent.Parameters?.FirstOrDefault(p => p.Name == parameter.Name);
                if (parentParam != null)
                {
                    // same name exists

                    if (parentParam.Type != parameter.Type)
                    {
                        // type conflict
                        throw new MissingAiAgentParameterException(
                            $"Parameter '{parameter.Name}' has mismatched types between parent and sub-agent. " +
                            $"Parent type: '{parentParam.Type}', Sub-agent type: '{parameter.Type}'. " +
                            "Both must declare the same ValueType.");
                    }

                    // parent has this parameter with matching type
                    continue;
                }

                if (_document.Parameters.TryGetMember(parameter.Name, out _))
                {
                    // conversation has this parameter
                    continue;
                }

                if (parameter.Policy.HasFlag(AiAgentParameterPolicy.ForbidModelGeneration) == false)
                {
                    // the parent doesn't have this parameter BUT it's allowed to be generated -> we add it to the tool schema
                    parameters[parameter.Name] = new ParameterDefinition(parameter.Description, parameter.Type);
                    continue;
                }

                throw new MissingAiAgentParameterException($"Parameter '{parameter.Name}' is missing from the parent scope." +
                                                           " To allow the root agent to generate this value dynamically, " +
                                                           $"unset the '{nameof(AiAgentParameterPolicy.ForbidModelGeneration)}' " +
                                                           "flag in the sub-agent parameter policy.");
            }

            parameters[ConversationDocument.SubAgentUserPromptKey] = new ParameterDefinition("A natural language prompt instructions for the sub-agent to do its work", AiAgentParameterValueType.String);
            return  parameters;
        }

        public static string GetSchemaForSubAgentTool(JsonOperationContext context, Dictionary<string, ParameterDefinition> parameters)
        {
            var properties = new DynamicJsonValue();
            var required = new DynamicJsonArray();

            foreach (var (name, value) in parameters)
            {
                var property = new DynamicJsonValue
                {
                    [ChatCompletionClient.Constants.JsonSchemaFields.Description] = value.Description
                };

                switch (value.Type)
                {
                    case AiAgentParameterValueType.Default:
                    case AiAgentParameterValueType.String:
                        property[ChatCompletionClient.Constants.JsonSchemaFields.Type] = ChatCompletionClient.Constants.JsonSchemaFields.TypeString;
                        break;

                    case AiAgentParameterValueType.Number:
                        property[ChatCompletionClient.Constants.JsonSchemaFields.Type] = ChatCompletionClient.Constants.JsonSchemaFields.TypeNumber;
                        break;

                    case AiAgentParameterValueType.Boolean:
                        property[ChatCompletionClient.Constants.JsonSchemaFields.Type] = ChatCompletionClient.Constants.JsonSchemaFields.TypeBoolean;
                        break;

                    case AiAgentParameterValueType.Null:
                        property[ChatCompletionClient.Constants.JsonSchemaFields.Type] = ChatCompletionClient.Constants.JsonSchemaFields.TypeNull;
                        break;

                    case AiAgentParameterValueType.ArrayOfString:
                        property[ChatCompletionClient.Constants.JsonSchemaFields.Type] = ChatCompletionClient.Constants.JsonSchemaFields.TypeArray;
                        property[ChatCompletionClient.Constants.JsonSchemaFields.Items] = new DynamicJsonValue { [ChatCompletionClient.Constants.JsonSchemaFields.Type] = ChatCompletionClient.Constants.JsonSchemaFields.TypeString };
                        break;

                    case AiAgentParameterValueType.ArrayOfNumber:
                        property[ChatCompletionClient.Constants.JsonSchemaFields.Type] = ChatCompletionClient.Constants.JsonSchemaFields.TypeArray;
                        property[ChatCompletionClient.Constants.JsonSchemaFields.Items] = new DynamicJsonValue { [ChatCompletionClient.Constants.JsonSchemaFields.Type] = ChatCompletionClient.Constants.JsonSchemaFields.TypeNumber };
                        break;

                    case AiAgentParameterValueType.ArrayOfBoolean:
                        property[ChatCompletionClient.Constants.JsonSchemaFields.Type] = ChatCompletionClient.Constants.JsonSchemaFields.TypeArray;
                        property[ChatCompletionClient.Constants.JsonSchemaFields.Items] = new DynamicJsonValue { [ChatCompletionClient.Constants.JsonSchemaFields.Type] = ChatCompletionClient.Constants.JsonSchemaFields.TypeBoolean };
                        break;

                    default:
                        throw new InvalidOperationException($"Unsupported ValueType: {value.Type}");
                }
                properties[name] = property;
                required.Add(name);
            }

            return context.ReadObject(new DynamicJsonValue
            {
                [ChatCompletionClient.Constants.JsonSchemaFields.Type] = ChatCompletionClient.Constants.JsonSchemaFields.TypeObject,
                [ChatCompletionClient.Constants.JsonSchemaFields.Properties] = properties,
                [ChatCompletionClient.Constants.JsonSchemaFields.Required] = required
            }, "tool/parameters").ToString();
        }

        public readonly struct ParameterDefinition
        {
            public string Description { get; }
            public AiAgentParameterValueType Type { get; }

            public ParameterDefinition(string description, AiAgentParameterValueType type)
            {
                Description = description;
                Type = type;
            }
        }

        protected sealed class SubConversationResult
        {
            public IDisposable Disposable { get; }
            public List<BlittableJsonReaderObject> Messages { get; } = new();
            public List<string> OpenToolCallsToRemove { get; } = new();
            public Dictionary<string, AiAgentActionRequest> ChildUserCalls { get; } = new();
            public int ToolsIterations { get; set; }
            public List<BlittableJsonReaderObject> SubAgentsResponses { get; set; }

            public SubConversationResult(IDisposable disposable)
            {
                Disposable = disposable;
            }
        }

    }
}
