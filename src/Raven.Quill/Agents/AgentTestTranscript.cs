using System;
using System.Collections.Generic;
using System.Text.Json;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Quill.Agents;

public sealed record AgentQueryToolCall(
    string Id,
    string Name,
    string? Description,
    string? Query,
    string Arguments,
    string? Result);

public static class AgentTestTranscript
{
    private const string DocumentsField = "Documents";

    private const string MessagesField = "Messages";

    private const string RoleField = "role";
    private const string ContentField = "content";
    private const string ToolCallsField = "tool_calls";
    private const string ToolCallIdField = "tool_call_id";
    private const string FunctionField = "function";
    private const string IdField = "id";
    private const string NameField = "name";
    private const string ArgumentsField = "arguments";
    private const string ToolRole = "tool";
    private const string AssistantRole = "assistant";

    public static IReadOnlyList<AgentQueryToolCall> ExtractQueryToolCalls(JsonElement root, AiAgentConfiguration configuration)
    {
        var toolCalls = new List<AgentQueryToolCall>();

        if (root.ValueKind != JsonValueKind.Object ||
            root.TryGetProperty(DocumentsField, out var documents) == false ||
            documents.ValueKind != JsonValueKind.Object)
        {
            return toolCalls;
        }

        var queriesByName = new Dictionary<string, AiAgentToolQuery>(StringComparer.Ordinal);
        foreach (var query in configuration.Queries ?? [])
        {
            if (string.IsNullOrEmpty(query.Name) == false)
                queriesByName[query.Name] = query;
        }

        if (queriesByName.Count == 0)
            return toolCalls;

        foreach (var document in documents.EnumerateObject())
        {
            if (document.Value.ValueKind != JsonValueKind.Object ||
                document.Value.TryGetProperty(MessagesField, out var messages) == false ||
                messages.ValueKind != JsonValueKind.Array)
            {
                continue;
            }

            var resultsByCallId = new Dictionary<string, string?>(StringComparer.Ordinal);
            foreach (var message in messages.EnumerateArray())
            {
                if (GetString(message, RoleField) != ToolRole)
                    continue;

                var callId = GetString(message, ToolCallIdField);
                if (string.IsNullOrEmpty(callId) == false)
                    resultsByCallId[callId] = GetString(message, ContentField);
            }

            foreach (var message in messages.EnumerateArray())
            {
                if (GetString(message, RoleField) != AssistantRole)
                    continue;

                if (message.TryGetProperty(ToolCallsField, out var calls) == false || calls.ValueKind != JsonValueKind.Array)
                    continue;

                foreach (var call in calls.EnumerateArray())
                {
                    if (call.ValueKind != JsonValueKind.Object ||
                        call.TryGetProperty(FunctionField, out var function) == false ||
                        function.ValueKind != JsonValueKind.Object)
                    {
                        continue;
                    }

                    var name = GetString(function, NameField);
                    if (name is null || queriesByName.TryGetValue(name, out var query) == false)
                        continue; // not a (known) query tool — actions/sub-agents aren't surfaced

                    var id = GetString(call, IdField) ?? "";
                    var arguments = GetString(function, ArgumentsField) ?? "";
                    resultsByCallId.TryGetValue(id, out var result);

                    toolCalls.Add(new AgentQueryToolCall(id, name, query.Description, query.Query, arguments, result));
                }
            }
        }

        return toolCalls;
    }

    private static string? GetString(JsonElement element, string property) =>
        element.ValueKind == JsonValueKind.Object &&
        element.TryGetProperty(property, out var value) &&
        value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;
}
