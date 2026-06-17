using System;
using System.Collections.Generic;
using System.Text.Json;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.AiAppliance.Agents;

/// <summary>
/// One query tool the model invoked during a test turn, reconstructed from the conversation
/// transcript: the configured query (RQL + description), the parameters the model filled in,
/// and the content the query returned. The appliance only supports query tools, so action and
/// sub-agent calls are intentionally not surfaced.
/// </summary>
/// <param name="Id">The tool-call id (ties the call to its result message).</param>
/// <param name="Name">The configured query tool's name (the function the model invoked).</param>
/// <param name="Description">The tool's configured description, if any.</param>
/// <param name="Query">The tool's configured RQL.</param>
/// <param name="Arguments">The JSON arguments the model filled in for the call.</param>
/// <param name="Result">The content the query returned; null when no matching result message.</param>
public sealed record AgentQueryToolCall(
    string Id,
    string Name,
    string? Description,
    string? Query,
    string Arguments,
    string? Result);

/// <summary>
/// Reconstructs the query tool calls from a draft agent test run so the wizard's "Test agent"
/// panel can show what the agent did, mirroring Studio's transcript view (query tools only).
///
/// RavenDB's <c>ai/agent/test</c> endpoint returns the (non-persisted) conversation document(s)
/// under <c>Documents</c>; each holds the OpenAI-style message list (<c>Messages</c>). A tool
/// invocation spans two messages: an <c>assistant</c> message carrying <c>tool_calls</c>
/// (id + function name + JSON arguments) and a later <c>tool</c> message whose
/// <c>tool_call_id</c> ties the returned <c>content</c> back to the call. We pair them up and
/// keep only calls whose name matches a configured query tool.
/// </summary>
public static class AgentTestTranscript
{
    // The result object's document map (PascalCase); each value is a conversation document.
    private const string DocumentsField = "Documents";

    // The conversation document's OpenAI-style message list (PascalCase, per ConversationDocument.ToJson).
    private const string MessagesField = "Messages";

    // OpenAI message / tool-call field names (lowercase on the wire).
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

    /// <summary>
    /// Extracts the query tool calls from the test endpoint's final result object
    /// (<paramref name="root"/>), enriched with the matching query config from
    /// <paramref name="configuration"/>. Returns an empty list when the run invoked no query
    /// tool or the configuration declares none.
    /// </summary>
    public static IReadOnlyList<AgentQueryToolCall> ExtractQueryToolCalls(JsonElement root, AiAgentConfiguration configuration)
    {
        var toolCalls = new List<AgentQueryToolCall>();

        if (root.ValueKind != JsonValueKind.Object ||
            root.TryGetProperty(DocumentsField, out var documents) == false ||
            documents.ValueKind != JsonValueKind.Object)
        {
            return toolCalls;
        }

        // The appliance only supports query tools; index them by name to filter the calls and
        // to enrich each with its RQL + description.
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

            // First pass: tool_call_id -> returned content (the "tool" role messages).
            var resultsByCallId = new Dictionary<string, string?>(StringComparer.Ordinal);
            foreach (var message in messages.EnumerateArray())
            {
                if (GetString(message, RoleField) != ToolRole)
                    continue;

                var callId = GetString(message, ToolCallIdField);
                if (string.IsNullOrEmpty(callId) == false)
                    resultsByCallId[callId] = GetString(message, ContentField);
            }

            // Second pass: each assistant tool_call that names a configured query tool. Only
            // assistant messages carry the calls the model made; tool_calls on any other role
            // aren't the model's invocations, so skip them.
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
