using System.Text.RegularExpressions;
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
    /// Validates and prepares <paramref name="request"/>'s configuration in place. Returns <c>null</c>
    /// on success; otherwise the error <see cref="IResult"/> the caller should return.
    /// </summary>
    public static async Task<IResult?> ValidateAndPrepareAsync(
        IDocumentStore store, string slug, EditAgentRequest? request, CancellationToken ct)
    {
        // STJ uses the param-less ctor, bypassing the 3-arg guards; validate here
        if (request?.Configuration is not { } body)
            return Results.BadRequest(new ApiErrorResponse("request body is required"));

        if (string.IsNullOrWhiteSpace(body.Name))
            return Results.BadRequest(new ApiErrorResponse("name is required"));

        if (string.IsNullOrWhiteSpace(body.SystemPrompt))
            return Results.BadRequest(new ApiErrorResponse("systemPrompt is required"));

        if (string.IsNullOrWhiteSpace(body.ConnectionStringName))
            return Results.BadRequest(new ApiErrorResponse("connectionStringName is required"));

        if (TryValidateActions(body, request.ActionBindings, out var actionErrors) == false)
            return Results.BadRequest(new ApiErrorResponse(Errors: actionErrors.ToArray()));

        if (body.SubAgents is { Count: > 0 })
            return Results.BadRequest(new ApiErrorResponse("subAgents are not supported in demo"));

        foreach (var query in body.Queries ?? [])
        {
            query.Query = EnforceLimit(query.Query);
        }

        var result = await store.Maintenance.ForDatabase(slug).SendAsync(new GetConnectionStringsOperation(body.ConnectionStringName, ConnectionStringType.Ai), ct);
        if (result.AiConnectionStrings is null ||
            result.AiConnectionStrings.TryGetValue(body.ConnectionStringName, out var aiCs) == false)
        {
            return Results.BadRequest(new ApiErrorResponse(
                $"connection string '{body.ConnectionStringName}' not found in app '{slug}'."));
        }

        if (aiCs.ModelType != AiModelType.Chat)
            return Results.BadRequest(new ApiErrorResponse(
                $"connection string '{aiCs.Name}' has ModelType={aiCs.ModelType}; agent provisioning requires Chat"));

        var provider = aiCs.GetActiveProvider();
        switch (provider)
        {
            case AiConnectorType.OpenAi:
            case AiConnectorType.AzureOpenAi:
            case AiConnectorType.Ollama:
                // supported providers
                break;
            default:
                return Results.BadRequest(new ApiErrorResponse($"unsupported provider '{provider}'"));
        }

        body.Disabled = false;
        body.ChatTrimming = new AiAgentChatTrimmingConfiguration(new AiAgentSummarizationByTokens
        {
            MaxTokensBeforeSummarization = 256 * 1024, // max context window of gpt5.4-mini is 400k tokens
            MaxTokensAfterSummarization = 4 * 1024
        });

        return null;
    }

    internal static bool TryValidateActions(
        AiAgentConfiguration body, Dictionary<string, WebhookBinding>? bindings, out List<string> errors)
    {
        errors = [];

        var actions = body.Actions ?? [];

        var byName = new Dictionary<string, WebhookBinding>(StringComparer.OrdinalIgnoreCase);
        foreach (var (name, binding) in bindings ?? [])
        {
            if (byName.TryAdd(name, binding) == false)
                errors.Add($"binding '{name}' is declared more than once; action names are case-insensitive");
        }

        foreach (var action in actions)
        {
            if (string.IsNullOrWhiteSpace(action.Name))
            {
                errors.Add("action name is required");
                continue;   // no key to match a binding against
            }

            if (string.IsNullOrWhiteSpace(action.Description))
            {
                errors.Add("action description is required");
            }

            var hasSchema = string.IsNullOrWhiteSpace(action.ParametersSchema) == false;
            var hasSample = string.IsNullOrWhiteSpace(action.ParametersSampleObject) == false;
            if (hasSchema == hasSample)
            {
                errors.Add(hasSchema
                    ? $"action '{action.Name}': set parametersSampleObject or parametersSchema, not both"
                    : $"action '{action.Name}': parametersSampleObject or parametersSchema is required");
            }

            if (byName.Remove(action.Name, out var binding) == false)
                errors.Add($"action '{action.Name}' has no binding");
            else if (IsHttpUrl(binding.Url) == false)
                errors.Add($"action '{action.Name}': url must be http(s)");
        }

        foreach (var name in byName.Keys)
        {
            errors.Add($"binding '{name}' has no matching action");
        }

        return errors.Count == 0;
    }

    private static bool IsHttpUrl(string? url) =>
        Uri.TryCreate(url, UriKind.Absolute, out var uri) &&
        (uri.Scheme == Uri.UriSchemeHttp || uri.Scheme == Uri.UriSchemeHttps);

    public static string EnforceLimit(string rql)
    {
        // match against a literal-masked copy (length-preserving) so a 'limit' inside a string literal is invisible
        var masked = MaskStringLiterals(rql);

        var m = AgentQueryLimit.Match(masked);
        if (m.Success == false)
            return rql.TrimEnd() + " limit " + MaxLimit;

        var n = m.Groups[1];
        if (int.TryParse(n.Value, out var limit) && limit <= MaxLimit)
            return rql;

        return rql[..n.Index] + MaxLimit + rql[(n.Index + n.Length)..];
    }

    private static string MaskStringLiterals(string rql)
    {
        var chars = rql.ToCharArray();
        var i = 0;
        while (i < chars.Length)
        {
            var quote = chars[i];
            if (quote != '\'' && quote != '"')
            {
                i++;
                continue;
            }

            var j = i + 1;
            while (j < chars.Length)
            {
                if (chars[j] == quote)
                {
                    // doubled quote = RQL escape, still inside the literal
                    if (j + 1 < chars.Length && chars[j + 1] == quote)
                    {
                        chars[j] = ' ';
                        chars[j + 1] = ' ';
                        j += 2;
                        continue;
                    }

                    break;
                }

                chars[j] = ' ';
                j++;
            }

            i = j + 1;
        }

        return new string(chars);
    }
}
