using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.AI;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.Handlers.AI.Agents;

public class ConversationDocument
{
    public ConversationDocument([NotNull] string agent, BlittableJsonReaderObject parameters)
    {
        Agent = agent;
        Parameters = parameters;
    }

    private ConversationDocument()
    {
        // for de-serialization
    }
    
    public string Agent;
    public BlittableJsonReaderObject Parameters;
    public List<BlittableJsonReaderObject> Messages = [];
    public List<string> LinkedConversations = [];
    
    public record SubAgentInstance(string Agent, string ConversationId, string Hash);

    public bool HasOpenCalls;
    public List<SubAgentInstance> SubAgents = [];
    public Dictionary<string, AiAgentActionRequest> OpenActionCalls = [];
    public AiUsage TotalUsage = new AiUsage();
    public string ChangeVector;
    public string Id;

    public DateTime LastMessageAt;
    public DateTime CreatedAt = DateTime.UtcNow;
    public TimeSpan? Expires;
    public void Initialize(JsonOperationContext context, AiAgentConfiguration configuration)
    {
        if (Messages.Count > 0)
            throw new InvalidOperationException("conversation document is already initialized. Cannot re-initialize.");

        foreach (var parameter in configuration.Parameters)
        {
            if (Parameters == null || Parameters.TryGet(parameter.Name, out object _) == false)
                throw new ArgumentException($"Parameter '{parameter.Name}' is missing.");
        }

        var promptMessage = configuration.SystemPrompt;
        if (TryCreateParameterDescriptionMessage(configuration, out string message))
        {
            promptMessage += "\n" + message;
        }

        AddMessage(context, context.ReadObject(new DynamicJsonValue
        {
            [ChatCompletionClient.Constants.RequestFields.Role] = ChatCompletionClient.Constants.RequestFields.RoleSystemValue,
            [ChatCompletionClient.Constants.RequestFields.Content] = promptMessage
        }, "system/msg"), usage: null);

        if (configuration.Parameters.Count > 0)
        {
            AddMessage(context, context.ReadObject(new DynamicJsonValue
            {
                [ChatCompletionClient.Constants.RequestFields.Role] = ChatCompletionClient.Constants.RequestFields.RoleUserValue,
                [ChatCompletionClient.Constants.RequestFields.Content] = ParametersToString(configuration)
            }, "system/msg"), usage: null);
        }
    }

    public List<AiToolCall> InitialOperations(JsonOperationContext context, AiAgentConfiguration configuration)
    {
        List<AiToolCall> result = null ;
        
        foreach (AiAgentToolQuery query in configuration.Queries ??[])
        {
            if(query.Options.HasFlag(AiAgentToolQueryOptions.AddToInitialContext) is false)
                continue;
            
            result ??= [];
            result.Add(new AiToolCall(Guid.NewGuid().ToString("N"), query.Name, "{}"));
        }

        if (result is null)
            return null;

        // here we generate artificial tools calls, so the model will have a better grasp
        // of what information we are actually giving it
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

        return result;
    }

    private string ParametersToString(AiAgentConfiguration configuration)
    {
        var sb = new StringBuilder("AI Agent Parameters:\n"); 
        foreach (var parameter in configuration.Parameters)
        {
           var value = Parameters[parameter.Name];
           sb.AppendLine($"{parameter.Name} = {value.ToString()}");
        }

        return sb.ToString();
    }

    public void EnsureInitialized()
    {
        if (Messages.Count == 0)
            throw new InvalidOperationException("conversation document is not initialized. Call Initialize() first.");
    }

    public BlittableJsonReaderObject ToBlittable(JsonOperationContext context, AiAgentConfiguration configuration)
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
        return new DynamicJsonValue
        {
            [nameof(Agent)] = Agent,
            [nameof(Parameters)] = Parameters,
            [nameof(Messages)] = Messages,
            [nameof(LinkedConversations)] = LinkedConversations,
            [nameof(SubAgents)] = SubAgents,
            [nameof(TotalUsage)] = TotalUsage.ToJson(),
            [nameof(OpenActionCalls)] = DynamicJsonValue.Convert(OpenActionCalls),
            [nameof(LastMessageAt)] = LastMessageAt,
            [nameof(CreatedAt)] = CreatedAt,
            [nameof(Expires)] = Expires,
        };
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

    internal static List<BlittableJsonReaderObject> GenerateTools(JsonOperationContext context, AiAgentConfiguration configuration, AbstractAiAgentProcessor processor)
    {
        List<BlittableJsonReaderObject> tools = [];
        foreach (var q in configuration.Queries ?? [])
        {
            if(q.Options.HasFlag(AiAgentToolQueryOptions.AllowModelQueries) is false)
                continue;
            
            var paramsSchema = ChatCompletionClient.GetSchemaForTool(q.ParametersSchema, q.ParametersSampleObject);
            var tool = new DynamicJsonValue
            {
                ["type"] = "function",
                ["function"] = new DynamicJsonValue
                {
                    ["name"] = q.Name,
                    ["description"] = q.Description,
                    ["parameters"] = context.Sync.ReadForMemory(paramsSchema, "params/schema")
                },
                ["strict"] = true
            };
            tools.Add(context.ReadObject(tool, "tool"));
        }
        foreach (var a in configuration.Actions ?? [])
        {
            string paramsSchema = ChatCompletionClient.GetSchemaForTool(a.ParametersSchema, a.ParametersSampleObject);
            var tool = new DynamicJsonValue
            {
                ["type"] = "function",
                ["function"] = new DynamicJsonValue
                {
                    ["name"] = a.Name,
                    ["description"] = a.Description,
                    ["parameters"] = context.Sync.ReadForMemory(paramsSchema, "params/schema")
                },
                ["strict"] = true
            };
            tools.Add(context.ReadObject(tool, "tool"));
        }
        foreach (var a in configuration.SubAgents ?? [])
        {
            AiAgentConfiguration agentConfiguration = processor.GetAiAgentConfiguration(a.Identifier);
            var argsSampleObject = new DynamicJsonValue();
            foreach (AiAgentParameter parameter in agentConfiguration.Parameters ?? [])
            {
                argsSampleObject[parameter.Name] = parameter.Description;
            }
            argsSampleObject["subAgentUserPrompt"] = "A natural language prompt instructions for the sub-agent to do its work";
            using var args = context.ReadObject(argsSampleObject, "args");
            string paramsSchema = ChatCompletionClient.GetSchemaForTool(null, args.ToString());
            var description = new StringBuilder(a.Description).AppendLine();
            agentConfiguration.AppendCapabilities(description);
            var tool = new DynamicJsonValue
            {
                [ChatCompletionClient.Constants.JsonSchemaFields.Type] = "function",
                [ChatCompletionClient.Constants.ResponseFields.Function] = new DynamicJsonValue
                {
                    [ChatCompletionClient.Constants.ResponseFields.Name] = a.Identifier,
                    [ChatCompletionClient.Constants.JsonSchemaFields.Description] = description.ToString(),
                    ["parameters"] = context.Sync.ReadForMemory(paramsSchema, "params/schema")
                },
                [ChatCompletionClient.Constants.JsonSchemaFields.Strict] = true
            };
            tools.Add(context.ReadObject(tool, "tool"));
        }

        return tools;
    }

    private static bool TryCreateParameterDescriptionMessage(AiAgentConfiguration configuration, out string message)
    {
        var hasDescription = false;
        var sb = new StringBuilder();
        sb.AppendLine("\nThe parameters for this conversation are described as follows:");
        foreach (var parameter in configuration.Parameters)
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
    }
}
