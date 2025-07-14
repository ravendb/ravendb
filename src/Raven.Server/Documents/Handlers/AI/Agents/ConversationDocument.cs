using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.AI;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;
using Sparrow.Utils;

namespace Raven.Server.Documents.Handlers.AI.Agents;

public class ConversationDocument(string agent, BlittableJsonReaderObject parameters)
{
    public string Agent = agent ?? throw new ArgumentNullException(nameof(agent));
    
    public BlittableJsonReaderObject Parameters = parameters;
    public List<BlittableJsonReaderObject> Messages = [];
    public List<string> HistoryDocuments = [];
    public Dictionary<string, AiAgentActionRequest> OpenActionCalls = [];
    public AiUsage TotalUsage = new AiUsage();
    public string ChangeVector;
    public void Initialize(JsonOperationContext context, AiAgentConfiguration configuration, string userPrompt)
    {
        if (Messages.Count > 0)
            throw new InvalidOperationException("conversation document is already initialized. Cannot re-initialize.");

        foreach (var parameter in configuration.Parameters)
        {
            if (Parameters == null || Parameters.TryGet(parameter, out object _) == false)
                throw new ArgumentException($"Parameter '{parameter}' is missing.");
        }

        AddMessage(context, context.ReadObject(new DynamicJsonValue
        {
            ["role"] = "system",
            ["content"] = configuration.SystemPrompt
        }, "system/msg"));
    }

    public void EnsureInitialized()
    {
        if (Messages.Count == 0)
            throw new InvalidOperationException("conversation document is not initialized. Call Initialize() first.");
    }

    public BlittableJsonReaderObject ToBlittable(JsonOperationContext context, AiAgentConfiguration configuration, TimeSpan? expiration = null)
    {
        var metadata = new DynamicJsonValue
        {
            [Constants.Documents.Metadata.Collection] = configuration.Persistence.Collection,
        };
        if (expiration is { } expire)
        {
            metadata[Constants.Documents.Metadata.Expires] = DateTime.UtcNow.Add(expire);
        }
        else if (configuration.Persistence.Expires is { } configExpire)
        {
            metadata[Constants.Documents.Metadata.Expires] = DateTime.UtcNow.Add(configExpire);
        }

        var conversation = ToJson();
        conversation[Constants.Documents.Metadata.Key] = metadata;
            
        return context.ReadObject(conversation, "create-conversion");
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Agent)] = Agent,
            [nameof(Parameters)] = Parameters,
            [nameof(Messages)] = Messages,
            [nameof(HistoryDocuments)] = HistoryDocuments,
            [nameof(TotalUsage)] = TotalUsage.ToJson(),
            [nameof(OpenActionCalls)] = DynamicJsonValue.Convert(OpenActionCalls),
        };
    }

    public void AddMessage(JsonOperationContext context, BlittableJsonReaderObject msg)
    {
        if (msg.TryGet("role", out string role) is false)
            return;

        switch (role)
        {
            case "system":
            case "user":
                break;
            case "tool":
            {
                // TODO: assuming an array only here. 
                if (msg.TryGet("content", out string content) is false)
                    return;

                var array = context.ParseBufferToArray(content, "tool-response", BlittableJsonDocumentBuilder.UsageMode.None);
                msg.Modifications = new DynamicJsonValue(msg) { ["content"] = array };
                break;
            }
            case "assistant":
            {
                if (msg.TryGet("content", out string content) && content is not null)
                {
                    //TODO: assuming an object only here
                    var obj = context.Sync.ReadForMemory(content, "assistant-response");
                    msg.Modifications = new DynamicJsonValue(msg) { ["content"] = obj };
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
                                function.Modifications = new DynamicJsonValue(function) { ["arguments"] = obj };
                            }
                        }
                    }
                }
                break;
            }
        }

        Messages.Add(msg);
    }

    public static ConversationDocument ToDocument(string id, BlittableJsonReaderObject document)
    {
        if (document.TryGet(nameof(Agent), out string agent) == false)
            throw new ArgumentException($"Missing Agent in '{id}' conversation document");
        if (document.TryGet(nameof(Parameters), out BlittableJsonReaderObject parameters) == false)
            throw new ArgumentException($"Missing Parameters in '{id}' conversation document");
        if (document.TryGet(nameof(Messages), out BlittableJsonReaderArray messages) == false)
            throw new ArgumentException($"Missing Messages in '{id}' conversation document");
        if (document.TryGet(nameof(HistoryDocuments), out BlittableJsonReaderArray historyDocs) == false)
            throw new ArgumentException($"Missing HistoryDocuments in '{id}' conversation document");
        if (document.TryGet(nameof(TotalUsage), out BlittableJsonReaderObject usage) == false)
            throw new ArgumentException($"AI Usage in '{id}' conversation document");
        if (document.TryGet(nameof(OpenActionCalls), out BlittableJsonReaderObject openToolCalls) == false)
            throw new ArgumentException($"Missing Open Tool Calls in '{id}' conversation document");

        var openTools = new Dictionary<string, AiAgentActionRequest>();
        foreach (var callId in openToolCalls.GetPropertyNames())
        {
            var call = JsonDeserializationClient.ActionRequest(openToolCalls[callId] as BlittableJsonReaderObject);
            openTools.Add(callId, call);
        }

        DevelopmentHelper.ToDo(DevelopmentHelper.Feature.AI, DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "make messages IEnumerable?");

        return new ConversationDocument(agent, parameters?.CloneOnTheSameContext())
        {
            Messages = messages.Items.Select(m=>((BlittableJsonReaderObject)m).CloneOnTheSameContext()).ToList(),
            HistoryDocuments = historyDocs.Items.Select(s => s.ToString()).ToList(),
            TotalUsage = JsonDeserializationClient.AiUsage(usage),
            OpenActionCalls = openTools
        };
    }

    public List<BlittableJsonReaderObject> GenerateTools(JsonOperationContext context, AiAgentConfiguration configuration)
    {
        List<BlittableJsonReaderObject> tools = [];
        foreach (var q in configuration.Queries ?? [])
        {
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

        return tools;
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
