using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Quill.Contracts;

namespace Raven.Quill.Agents;

/// <summary>
/// Shared validation + preparation for the agent write endpoints (provision + edit).
/// Applies the demo gating (required fields, unsupported features, connection-string /
/// provider checks) and mutates the incoming configuration into its persisted shape
/// (query limit enforcement, disabled flag, chat trimming).
/// </summary>
internal static class AgentConfigValidator
{
    private static readonly Regex AgentQueryLimit =
        new(@"\blimit\s+(?:\S+\s*,\s*)?(\S+)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.RightToLeft);

    private const int MaxLimit = 32;

    /// <summary>
    /// Validates and prepares <paramref name="body"/> in place. Returns <c>null</c> on success;
    /// otherwise the error <see cref="IResult"/> the caller should return.
    /// </summary>
    public static async Task<IResult?> ValidateAndPrepareAsync(
        IDocumentStore store, string database, AiAgentConfiguration? body, CancellationToken ct)
    {
        // STJ uses the param-less ctor, bypassing the 3-arg guards; validate here
        if (body is null)
            return Results.BadRequest(new ApiErrorResponse("request body is required"));

        if (string.IsNullOrWhiteSpace(body.Name))
            return Results.BadRequest(new ApiErrorResponse("name is required"));

        if (string.IsNullOrWhiteSpace(body.SystemPrompt))
            return Results.BadRequest(new ApiErrorResponse("systemPrompt is required"));

        if (string.IsNullOrWhiteSpace(body.ConnectionStringName))
            return Results.BadRequest(new ApiErrorResponse("connectionStringName is required"));

        if (body.Actions is { Count: > 0 })
            return Results.BadRequest(new ApiErrorResponse("actions are not supported in demo"));

        if (body.SubAgents is { Count: > 0 })
            return Results.BadRequest(new ApiErrorResponse("subAgents are not supported in demo"));

        foreach (var query in body.Queries ?? [])
        {
            query.Query = EnforceLimit(query.Query);
        }

        var cs = await store.Maintenance.ForDatabase(database)
            .SendAsync(new GetConnectionStringsOperation(body.ConnectionStringName, ConnectionStringType.Ai), ct);

        if (cs.AiConnectionStrings is null ||
            cs.AiConnectionStrings.TryGetValue(body.ConnectionStringName, out var aiCs) == false)
        {
            return Results.BadRequest(new ApiErrorResponse(
                $"connection string '{body.ConnectionStringName}' not found; create it via " +
                $"POST /api/apps/{{slug}}/ai/connection-strings first"));
        }

        if (aiCs.ModelType != AiModelType.Chat)
            return Results.BadRequest(new ApiErrorResponse(
                $"connection string '{aiCs.Name}' has ModelType={aiCs.ModelType}; agent provisioning requires Chat"));

        // re-gate provider: a CS added via Studio bypasses the POST-time gate
        var provider = aiCs.GetActiveProvider();
        if (provider != AiConnectorType.OpenAi && provider != AiConnectorType.Ollama)
            return Results.BadRequest(new ApiErrorResponse(
                $"connection string '{aiCs.Name}' uses unsupported provider '{provider}' in demo; supported: OpenAi, Ollama"));

        body.Disabled = false;
        body.ChatTrimming = new AiAgentChatTrimmingConfiguration(new AiAgentSummarizationByTokens
        {
            MaxTokensBeforeSummarization = 256 * 1024, // max context window of gpt5.4-mini is 400k tokens
            MaxTokensAfterSummarization = 4 * 1024
        });

        return null;
    }

    public static string EnforceLimit(string rql)
    {
        var m = AgentQueryLimit.Match(rql);
        if (m.Success == false)
            return rql.TrimEnd() + " limit " + MaxLimit;

        var n = m.Groups[1];
        if (int.TryParse(n.Value, out var limit) && limit <= MaxLimit)
            return rql;

        return rql[..n.Index] + MaxLimit + rql[(n.Index + n.Length)..];
    }
}
