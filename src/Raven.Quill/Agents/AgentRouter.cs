using System.Text.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Metrics;

using Raven.Quill.Logging;

namespace Raven.Quill.Agents;

public sealed record AgentRequest(
    string Database,
    string AgentId,
    string ConversationId,
    string Prompt,
    string ChannelId,
    IReadOnlyDictionary<string, JsonElement> Parameters);

public sealed record AgentRunResult(object Answer, string ConversationId);

public interface IAgentRouter
{
    Task<AgentRunResult> RunAsync(AgentRequest request, Func<string, ValueTask> onChunk, AiAgentConfiguration config, CancellationToken ct);
}

public sealed class InvalidParameterValueException(string name, AiAgentParameterValueType type, string reason)
    : Exception($"parameter '{name}' declared as {type}: {reason}")
{
    public string PublicMessage { get; } = $"the value bound for parameter '{name}' is not a valid {type}";
}

public sealed class UnknownAgentException(string agentId)
    : Exception($"unknown agentId '{agentId}'")
{
    public string AgentId { get; } = agentId;
}

internal sealed class AgentRouter(
    IDocumentStore store, WebhookActionExecutor actionExecutor, QuillLogger<AgentRouter> logger) : IAgentRouter
{
    public async Task<AgentRunResult> RunAsync(AgentRequest request, Func<string, ValueTask> onChunk, AiAgentConfiguration config, CancellationToken ct)
    {
        if (config is null)
            throw new UnknownAgentException(request.AgentId);

        var conversationId = NormalizeConversationId(request.ConversationId);

        var creationOptions = new AiConversationCreationOptions();
        foreach (var (key, value) in ConvertParameters(request, config))
            creationOptions.AddParameter(key, value);

        var conversation = store.AI.ForDatabase(request.Database).Conversation(
            agentId: config.Identifier,
            conversationId: conversationId,
            creationOptions: creationOptions);

        conversation.AddUserPrompt(request.Prompt);

        conversation.OnUnhandledAction += static _ => Task.CompletedTask; // we handle that manually

        var replyField = AgentOutputShape.ResolveReplyField(config);

        var result = await conversation.StreamAsync<Dictionary<string, object>>(
            replyField,
            async chunk => await onChunk(chunk),
            ct);

        using var session = store.OpenAsyncSession(request.Database);
        var lazyBindings = session.Advanced.Lazily.LoadAsync<AgentActionBindings>(AgentActionBindings.IdFor(config.Identifier), ct);

        while (result.Status == AiConversationResult.ActionRequired)
        {
            var bindings = await lazyBindings.Value;
            await RunActionsAsync(conversation, config, bindings, ct);

            result = await conversation.StreamAsync<Dictionary<string, object>>(
                replyField,
                async chunk => await onChunk(chunk),
                ct);
        }

        var reply = AgentOutputShape.ExtractReplyText(result.Answer, replyField);

        await UpsertPreviewAsync(store, request, config.Identifier, conversation.Id, reply, DateTime.UtcNow, ct);

        return new AgentRunResult(new { reply }, conversation.Id);
    }

    private static Dictionary<string, object?> ConvertParameters(AgentRequest request, AiAgentConfiguration config)
    {
        var declaredType = new Dictionary<string, AiAgentParameterValueType>(StringComparer.OrdinalIgnoreCase);
        foreach (var parameter in config.Parameters ?? [])
        {
            if (string.IsNullOrWhiteSpace(parameter.Name) == false)
                declaredType[parameter.Name] = parameter.Type;
        }

        var converted = new Dictionary<string, object?>();
        foreach (var (key, value) in request.Parameters)
        {
            var type = declaredType.GetValueOrDefault(key, AiAgentParameterValueType.Default);

            if (AgentParameterValue.TryNormalize(type, value, out var normalized, out var error) == false)
                throw new InvalidParameterValueException(key, type, error!);

            converted[key] = AgentTestParameterValue.Convert(normalized);
        }

        return converted;
    }

    private async Task RunActionsAsync(
        IAiConversationOperations conversation, AiAgentConfiguration config,
        AgentActionBindings bindings, CancellationToken ct)
    {
        var pending = conversation.RequiredActions().ToList();
        var responses = await Task.WhenAll(pending.Select(action => RunActionAsync(action, config, bindings, ct)));

        for (var i = 0; i < pending.Count; i++)
            conversation.AddActionResponse(pending[i].ToolId, responses[i]);
    }

    private Task<string> RunActionAsync(
        AiAgentActionRequest action, AiAgentConfiguration config,
        AgentActionBindings bindings, CancellationToken ct)
    {
        if (bindings?.Bindings?.TryGetValue(action.Name, out var binding) == true)
            return actionExecutor.ExecuteAsync(action, binding, ct);

        if (logger.IsWarnEnabled)
            logger.Warn(
                $"Agent '{config.Identifier}' invoked action '{action.Name}' (toolId {action.ToolId}) " +
                "with no binding configured");

        return Task.FromResult($"action failed: no binding configured for '{action.Name}'");
    }

    internal static async Task UpsertPreviewAsync(
        IDocumentStore store, AgentRequest request, string agent, string conversationId, string reply,
        DateTime nowUtc, CancellationToken ct)
    {
        using var session = store.OpenAsyncSession(request.Database);
        var id = ConversationPreview.IdFor(conversationId);
        var preview = await session.LoadAsync<ConversationPreview>(id, ct) ?? new ConversationPreview
        {
            ConversationId = conversationId,
            Agent = agent,
            ChannelId = request.ChannelId,
            Parameters = request.Parameters.ToDictionary(
                parameter => parameter.Key,
                parameter => AgentParameterValue.ToDisplayText(parameter.Value)),
            CreatedAt = nowUtc
        };

        preview.LastMessageAt = nowUtc;
        preview.LastUserPrompt = request.Prompt;
        preview.LastAgentReply = reply;

        await session.StoreAsync(preview, id, ct);
        await session.SaveChangesAsync(ct);
        return;
    }

    internal static string NormalizeConversationId(string? conversationId)
    {
        if (TryNormalizeConversationId(conversationId, out var normalized, out var error) == false)
            throw new ArgumentException(error, nameof(conversationId));

        return normalized;
    }

    internal static bool TryNormalizeConversationId(string? raw, out string normalized, out string? error)
    {
        var trimmed = raw?.Trim();

        if (string.IsNullOrWhiteSpace(trimmed))
        {
            normalized = "chats/";
            error = null;
            return true;
        }

        // pin to the chats/ prefix so a caller can't overwrite another document
        if (trimmed.StartsWith("chats/", StringComparison.Ordinal) == false)
        {
            normalized = "";
            error = "conversationId must start with 'chats/'";
            return false;
        }

        normalized = trimmed;
        error = null;
        return true;
    }
}
