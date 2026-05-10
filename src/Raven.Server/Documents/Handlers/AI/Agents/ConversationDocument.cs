using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Exceptions;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.AI;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.Handlers.AI.Agents;

public class ConversationDocument([NotNull] string agent, BlittableJsonReaderObject parameters)
{
    public const string SubAgentUserPromptKey = "subAgentUserPrompt";

    public string Agent = agent;

    public BlittableJsonReaderObject Parameters = parameters;
    public List<BlittableJsonReaderObject> Messages = [];
    public List<string> LinkedConversations = [];
    public Dictionary<string, AiAgentActionRequest> OpenActionCalls = [];
    public AiUsage TotalUsage = new AiUsage();
    public AiUsage CurrentUsage = new AiUsage();
    public string ChangeVector;
    public string Id;

    public DateTime LastMessageAt;
    public DateTime CreatedAt = DateTime.UtcNow;
    public TimeSpan? Expires;

    public int RemainingToolIterations;

    public bool EnableFullDebug;

    public HashSet<string> SubConversationIds = new (StringComparer.OrdinalIgnoreCase);

    public void Initialize(JsonOperationContext context, AiAgentConfiguration configuration, bool resetRemainingToolIterations, int maxModelIterationsPerCall)
    {
        if (Messages.Count > 0)
            throw new InvalidOperationException("conversation document is already initialized. Cannot re-initialize.");

        List<AiAgentParameter> modelParameters = [];
        var configParams = configuration.Parameters ?? [];

        foreach (var p in configParams)
        {
            // Skip parameters that should not be sent to the model
            if (p.SendToModel == false)
                continue;

            if (Parameters == null || Parameters.TryGet(p.Name, out object value) == false)
                throw new MissingAiAgentParameterException($"Parameter '{p.Name}' is missing.");

            var param = ConversationHandler.GetAiConversationParameter(p.Name, value);
            if (param.SendToModel)
                modelParameters.Add(p);
        }

        // Additional Parameters (that are not defined in the configuration, but still should be sent to the model)
        if (Parameters != null)
        {
            foreach (var paramName in Parameters.GetPropertyNames())
            {
                if (configParams.Any(p => p.Name == paramName))
                    continue;

                var param = ConversationHandler.GetAiConversationParameter(paramName, Parameters[paramName]);
                if (param.SendToModel)
                    modelParameters.Add(new AiAgentParameter(paramName));
            }
        }


        var promptMessage = configuration.SystemPrompt;
        if (TryCreateParameterDescriptionMessage(modelParameters, out string message))
        {
            promptMessage += "\n" + message;
        }

        AddMessage(context, context.ReadObject(new DynamicJsonValue
        {
            [ChatCompletionClient.Constants.RequestFields.Role] = ChatCompletionClient.Constants.RequestFields.RoleSystemValue,
            [ChatCompletionClient.Constants.RequestFields.Content] = promptMessage
        }, "system/msg"), usage: null);

        if (modelParameters.Count > 0)
        {
            AddMessage(context, context.ReadObject(new DynamicJsonValue
            {
                [ChatCompletionClient.Constants.RequestFields.Role] = ChatCompletionClient.Constants.RequestFields.RoleUserValue,
                [ChatCompletionClient.Constants.RequestFields.Content] = ParametersToString(modelParameters)
            }, "system/msg"), usage: null);
        }

        if (resetRemainingToolIterations == false)
            return;

        RemainingToolIterations = maxModelIterationsPerCall;
    }

    public List<AiToolCall> InitialOperations(JsonOperationContext context, AiAgentConfiguration configuration)
    {
        List<AiToolCall> result = null;

        foreach (AiAgentToolQuery query in configuration.Queries ?? [])
        {
            if (query.ShouldAddToInitialContext() == false)
                continue;

            result ??= [];
            result.Add(new AiToolCall(Guid.NewGuid().ToString("N"), query.Name, "{}"));
        }

        if (result is null)
            return null;

        // here we generate artificial tools calls, so the model will have a better grasp
        // of what information we are actually giving it
        AddArtificialToolCall(context, result);

        return result;
    }

    public void AddToolResponse(JsonOperationContext context, string toolId, string content)
    {
        AddMessage(context, context.ReadObject(
            new DynamicJsonValue
            {
                ["tool_call_id"] = toolId,
                ["role"] = "tool",
                ["content"] = content
            },
            "user/tool"), usage: null);
    }

    public void AddArtificialToolCall(JsonOperationContext context, List<AiToolCall> result)
    {
        var tools = new DynamicJsonArray();
        foreach (AiToolCall call in result)
        {
            tools.Add(new DynamicJsonValue
            {
                [ChatCompletionClient.Constants.ResponseFields.Id] = call.Id,
                [ChatCompletionClient.Constants.ResponseFields.Type] = ChatCompletionClient.Constants.ResponseFields.Function,
                [ChatCompletionClient.Constants.ResponseFields.Function] = new DynamicJsonValue
                {
                    [ChatCompletionClient.Constants.ResponseFields.Name] = call.Name,
                    [ChatCompletionClient.Constants.ResponseFields.Arguments] = call.Arguments
                }
            });
        }

        AddMessage(context, context.ReadObject(new DynamicJsonValue
        {
            [ChatCompletionClient.Constants.RequestFields.Role] = ChatCompletionClient.Constants.RequestFields.RoleAssistantValue,
            [ChatCompletionClient.Constants.ResponseFields.ToolCalls] = tools
        }, "tools/msg"), usage: null);
    }

    private string ParametersToString(List<AiAgentParameter> parameters)
    {
        var sb = new StringBuilder("AI Agent Parameters:\n");
        foreach (var parameter in parameters)
        {
            var value = ConversationHandler.GetAiConversationParameter(parameter.Name, Parameters[parameter.Name]).Value;
            sb.AppendLine($"{parameter.Name} = {value?.ToString() ?? "null"}");
        }

        return sb.ToString();
    }

    public void EnsureInitialized()
    {
        if (Messages.Count == 0)
            throw new InvalidOperationException("conversation document is not initialized. Call Initialize() first.");
    }

    public BlittableJsonReaderObject ToBlittable(JsonOperationContext context)
    {
        var metadata = new DynamicJsonValue
        {
            [Constants.Documents.Metadata.Collection] = Constants.Documents.Collections.AiAgentConversationCollection,
        };

        if (Expires.HasValue)
        {
            metadata[Constants.Documents.Metadata.Expires] = DateTime.UtcNow.Add(Expires.Value);
        }

        var conversation = ToJson();
        conversation[Constants.Documents.Metadata.Key] = metadata;

        return context.ReadObject(conversation, "create-conversion");
    }

    public BlittableJsonReaderObject ToHistoryBlittable(JsonOperationContext context, AiAgentConfiguration configuration, TimeSpan? expiration = null)
    {
        var metadata = new DynamicJsonValue
        {
            [Constants.Documents.Metadata.Collection] = Constants.Documents.Collections.AiAgentConversationHistoryCollection,
        };

        if (expiration.HasValue)
        {
            metadata[Constants.Documents.Metadata.Expires] = DateTime.UtcNow.Add(expiration.Value);
        }

        var conversation = ToJson();

        conversation[Constants.Documents.Metadata.Key] = metadata;
        conversation[nameof(LinkedConversations)] = new DynamicJsonArray
        {
            Id
        };
        return context.ReadObject(conversation, "create-conversion");
    }

    public DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(Agent)] = Agent,
            [nameof(Parameters)] = Parameters,
            [nameof(Messages)] = Messages,
            [nameof(LinkedConversations)] = LinkedConversations,
            [nameof(TotalUsage)] = TotalUsage.ToJson(),
            [nameof(OpenActionCalls)] = DynamicJsonValue.Convert(OpenActionCalls),
            [nameof(LastMessageAt)] = LastMessageAt,
            [nameof(CreatedAt)] = CreatedAt,
            [nameof(Expires)] = Expires,
            [nameof(CurrentUsage)] = CurrentUsage,
            [nameof(RemainingToolIterations)] = RemainingToolIterations,
            [nameof(SubConversationIds)] = new DynamicJsonArray(SubConversationIds)
        };

        if (EnableFullDebug)
            json[nameof(EnableFullDebug)] = true;

        return json;
    }

    public const string DateProperty = "date";
    public const string UsageProperty = "usage";

    public void AddMessage(JsonOperationContext context, BlittableJsonReaderObject msg, AiUsage usage)
    {
        var currentDate = DateTime.UtcNow;
        msg.Modifications ??= new DynamicJsonValue(msg);
        msg.Modifications[DateProperty] = currentDate;
        if (usage != null)
            msg.Modifications[UsageProperty] = usage.ToJson();
        Messages.Add(msg);
        LastMessageAt = currentDate;
    }

    public static ConversationDocument ToDocument(string id, BlittableJsonReaderObject document, int maxModelIterationsPerCall)
    {
        if (document.TryGet(nameof(Agent), out string agent) == false)
            throw new ArgumentException($"Missing Agent in '{id}' conversation document");
        if (document.TryGet(nameof(Parameters), out BlittableJsonReaderObject parameters) == false)
            throw new ArgumentException($"Missing Parameters in '{id}' conversation document");
        if (document.TryGet(nameof(Messages), out BlittableJsonReaderArray messages) == false)
            throw new ArgumentException($"Missing Messages in '{id}' conversation document");
        if (document.TryGet(nameof(LinkedConversations), out BlittableJsonReaderArray historyDocs) == false)
            throw new ArgumentException($"Missing HistoryDocuments in '{id}' conversation document");
        if (document.TryGet(nameof(TotalUsage), out BlittableJsonReaderObject usage) == false)
            throw new ArgumentException($"AI Usage in '{id}' conversation document");
        if (document.TryGet(nameof(OpenActionCalls), out BlittableJsonReaderObject openToolCalls) == false)
            throw new ArgumentException($"Missing Open Tool Calls in '{id}' conversation document");
        if (document.TryGet(nameof(LastMessageAt), out DateTime lastMessageAt) == false)
            throw new ArgumentException($"Missing LastMessageAt in '{id}' conversation document");
        if (document.TryGet(nameof(CreatedAt), out DateTime createAt) == false)
            throw new ArgumentException($"Missing CreatedAt in '{id}' conversation document");
        if (document.TryGet(nameof(Expires), out TimeSpan? expires) == false)
            throw new ArgumentException($"Missing Expires in '{id}' conversation document");
        if (document.TryGet(nameof(RemainingToolIterations), out int remainingToolIterations) == false)
            remainingToolIterations = maxModelIterationsPerCall;

        document.TryGet(nameof(EnableFullDebug), out bool enableFullDebug);

        var openTools = new Dictionary<string, AiAgentActionRequest>();
        foreach (var callId in openToolCalls.GetPropertyNames())
        {
            var call = JsonDeserializationClient.ActionRequest(openToolCalls[callId] as BlittableJsonReaderObject);
            openTools.Add(callId, call);
        }

        var conversation = new ConversationDocument(agent, parameters?.CloneOnTheSameContext())
        {
            Id = id,
            Messages = messages.Items.Select(m => ((BlittableJsonReaderObject)m).CloneOnTheSameContext()).ToList(),
            LinkedConversations = historyDocs.Items.Select(s => s.ToString()).ToList(),
            TotalUsage = JsonDeserializationClient.AiUsage(usage),
            OpenActionCalls = openTools,
            LastMessageAt = lastMessageAt,
            CreatedAt = createAt,
            Expires = expires,
            RemainingToolIterations = remainingToolIterations,
            EnableFullDebug = enableFullDebug
        };

        if (document.TryGet(nameof(CurrentUsage), out BlittableJsonReaderObject currentUsageBlittable))
        {
            conversation.CurrentUsage = JsonDeserializationClient.AiUsage(currentUsageBlittable);
        }

        if (document.TryGet(nameof(SubConversationIds), out BlittableJsonReaderArray subConversationIds))
        {
            conversation.SubConversationIds = subConversationIds.Items.Select(m => ((LazyStringValue)m).ToString(CultureInfo.InvariantCulture)).ToHashSet(StringComparer.OrdinalIgnoreCase);
        }

        return conversation;
    }

    private static bool TryCreateParameterDescriptionMessage(List<AiAgentParameter> parameters, out string message)
    {
        var hasDescription = false;
        var sb = new StringBuilder();
        sb.AppendLine("\nThe parameters for this conversation are described as follows:");
        foreach (var parameter in parameters)
        {
            if (string.IsNullOrEmpty(parameter.Description))
                continue;

            hasDescription = true;
            sb.AppendLine($"- {parameter.Name}: {parameter.Description}");
        }

        message = sb.ToString();
        return hasDescription;
    }

    public void UpdateUsage(AiUsage usage)
    {
        if (TotalUsage is null)
        {
            TotalUsage = usage;
            return;
        }

        TotalUsage.TotalTokens += usage.TotalTokens;
        TotalUsage.PromptTokens += usage.PromptTokens;
        TotalUsage.CompletionTokens += usage.CompletionTokens;
        TotalUsage.CachedTokens += usage.CachedTokens;
        TotalUsage.ReasoningTokens += usage.ReasoningTokens;
    }

    public bool TryGetDetailsOfRecentToolCall(AiAgentConfiguration configuration, out List<ExceededTokenThresholdDetails.ToolCallDetails> toolCalls)
    {
        toolCalls = null;

        var lastMessage = Messages.LastOrDefault();
        if (lastMessage?.TryGet(ChatCompletionClient.Constants.ResponseFields.ToolCalls, out BlittableJsonReaderArray _) == true)
            return false;

        for (var i = Messages.Count - 1; i >= 0; i--)
        {
            var m = Messages[i];

            if (m.TryGet(ChatCompletionClient.Constants.RequestFields.Role, out string role) == false)
            {
                continue;
            }

            switch (role)
            {
                case ChatCompletionClient.Constants.RequestFields.RoleUserValue:
                    return false;

                case ChatCompletionClient.Constants.RequestFields.RoleAssistantValue:
                    if (m.TryGet(ChatCompletionClient.Constants.ResponseFields.ToolCalls, out BlittableJsonReaderArray toolCallsArray))
                    {
                        toolCalls = [];
                        foreach (BlittableJsonReaderObject call in toolCallsArray)
                        {
                            call.TryGet(ChatCompletionClient.Constants.JsonSchemaFields.Id, out string id);
                            call.TryGet(ChatCompletionClient.Constants.JsonSchemaFields.Function, out BlittableJsonReaderObject function);
                            function.TryGet(ChatCompletionClient.Constants.JsonSchemaFields.Name, out string name);
                            function.TryGet(ChatCompletionClient.Constants.JsonSchemaFields.Arguments, out string arguments);

                            ToolType toolType = GetToolType(configuration, name);

                            var tc = new ExceededTokenThresholdDetails.ToolCallDetails
                            {
                                Id = id,
                                Name = name,
                                Type = toolType,
                                Arguments = arguments
                            };

                            if (toolType == ToolType.Query)
                            {
                                var q = configuration.FindQuery(name);
                                if (q != null)
                                {
                                    tc.Query = q.Query;
                                }
                            }
                            toolCalls.Add(tc);
                        }
                        return true;
                    }
                    break;

                default:
                    continue;
            }
        }

        return false;
    }

    private static ToolType GetToolType(AiAgentConfiguration configuration, string name)
    {
        if (configuration.FindAction(name) != null)
            return ToolType.Action;

        if (configuration.FindQuery(name) != null)
            return ToolType.Query;

        if (configuration.FindSubAgent(name) != null)
            return ToolType.SubAgent;

        return ToolType.Unknown;
    }
}
