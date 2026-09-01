using System.Text.Json;
using Microsoft.Extensions.Options;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Hosting;
using Raven.Quill.Metrics;

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
    IDocumentStore store, WebhookActionExecutor actionExecutor, IOptions<ApplianceOptions> options,
    ILogger<AgentRouter> logger) : IAgentRouter
{
    public async Task<AgentRunResult> RunAsync(AgentRequest request, Func<string, ValueTask> onChunk, AiAgentConfiguration config, CancellationToken ct)
    {
        var deadline = options.Value.AgentTurnDeadline;

        using var turn = CancellationTokenSource.CreateLinkedTokenSource(ct);
        turn.CancelAfter(deadline);

        try
        {
            return await RunTurnAsync(request, onChunk, config, turn.Token);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested == false && turn.IsCancellationRequested)
        {
            throw new ProviderTimeoutException(deadline);
        }
    }

    private async Task<AgentRunResult> RunTurnAsync(AgentRequest request, Func<string, ValueTask> onChunk, AiAgentConfiguration config, CancellationToken ct)
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

        conversation.OnUnhandledAction += static _ => Task.CompletedTask; // we handle that manually

        var replyField = AgentOutputShape.ResolveReplyField(config);

        var result = await StreamWithRetryAsync(
            conversation, () => conversation.AddUserPrompt(request.Prompt), replyField, onChunk, ct);

        using var session = store.OpenAsyncSession(request.Database);
        var lazyBindings = session.Advanced.Lazily.LoadAsync<AgentActionBindings>(AgentActionBindings.IdFor(config.Identifier), ct);

        while (result.Status == AiConversationResult.ActionRequired)
        {
            var bindings = await lazyBindings.Value;
            var responses = await RunActionsAsync(conversation, config, bindings, ct);

            result = await StreamWithRetryAsync(
                conversation,
                () =>
                {
                    foreach (var (toolId, response) in responses)
                        conversation.AddActionResponse(toolId, response);
                },
                replyField,
                onChunk,
                ct);
        }

        var reply = AgentOutputShape.ExtractReplyText(result.Answer, replyField);
        if (string.IsNullOrWhiteSpace(reply))
            throw new EmptyAnswerException();

        await UpsertPreviewAsync(store, request, config.Identifier, conversation.Id, reply, DateTime.UtcNow, ct);

        return new AgentRunResult(new { reply }, conversation.Id);
    }

    private async Task<AiAnswer<Dictionary<string, object>>> StreamWithRetryAsync(
        IAiConversationOperations conversation, Action arm, string replyField,
        Func<string, ValueTask> onChunk, CancellationToken ct)
    {
        for (var attempt = 0; ; attempt++)
        {
            var streamed = false;
            arm();

            try
            {
                return await conversation.StreamAsync<Dictionary<string, object>>(
                    replyField,
                    async chunk =>
                    {
                        streamed = true;
                        await onChunk(chunk);
                    },
                    ct);
            }
            catch (Exception e) when (streamed == false && RetryDelayFor(e, attempt) is { } delay)
            {
                logger.LogWarning(e,
                    "AI provider rate-limited the turn; retrying in {DelaySeconds}s (attempt {Attempt} of {Max})",
                    delay.TotalSeconds, attempt + 1, ProviderLimits.MaxRateLimitedRetries);

                await Task.Delay(delay, ct);
            }
        }
    }

    private static TimeSpan? RetryDelayFor(Exception e, int attempt)
    {
        if (attempt >= ProviderLimits.MaxRateLimitedRetries)
            return null;

        var failure = ProviderFailures.Classify(e);
        if (failure.Kind != ProviderFailureKind.RateLimited)
            return null;

        if (failure.RetryAfter is not { } retryAfter)
            return ProviderLimits.MinRetryDelay * Math.Pow(2, attempt);

        if (retryAfter > ProviderLimits.MaxRetryDelay)
            return null;

        return retryAfter < ProviderLimits.MinRetryDelay ? ProviderLimits.MinRetryDelay : retryAfter;
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

    private async Task<List<(string ToolId, string Response)>> RunActionsAsync(
        IAiConversationOperations conversation, AiAgentConfiguration config,
        AgentActionBindings bindings, CancellationToken ct)
    {
        var pending = conversation.RequiredActions().ToList();
        var responses = await Task.WhenAll(pending.Select(action => RunActionAsync(action, config, bindings, ct)));

        var applied = new List<(string ToolId, string Response)>(pending.Count);
        for (var i = 0; i < pending.Count; i++)
            applied.Add((pending[i].ToolId, responses[i]));

        return applied;
    }

    private Task<string> RunActionAsync(
        AiAgentActionRequest action, AiAgentConfiguration config,
        AgentActionBindings bindings, CancellationToken ct)
    {
        if (bindings?.Bindings?.TryGetValue(action.Name, out var binding) == true)
            return actionExecutor.ExecuteAsync(action, binding, ct);

        logger.LogWarning(
            "Agent '{AgentId}' invoked action '{Action}' (toolId {ToolId}) with no binding configured",
            config.Identifier, action.Name, action.ToolId);

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
