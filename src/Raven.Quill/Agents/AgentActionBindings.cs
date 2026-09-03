namespace Raven.Quill.Agents;

public sealed class WebhookBinding
{
    public string? Url { get; set; }

    public string? Secret { get; set; }

    public int? MaxResponseSize { get; set; }
}

internal sealed class AgentActionBindings
{
    public static string IdFor(string agentId) => "agent-actions/" + agentId;

    public Dictionary<string, WebhookBinding> Bindings { get; set; } =
        new(StringComparer.OrdinalIgnoreCase);      // key = action name

    public static string DescribeTargetsForAudit(Dictionary<string, WebhookBinding>? bindings) =>
        bindings is null || bindings.Count == 0
            ? string.Empty
            : string.Join(", ", bindings
                .OrderBy(binding => binding.Key, StringComparer.Ordinal)
                .Select(binding => $"{binding.Key}->{DescribeUrlForAudit(binding.Value.Url)}"));

    private static string DescribeUrlForAudit(string? url)
    {
        if (string.IsNullOrWhiteSpace(url))
            return "none";

        // a URL we cannot parse could be anything, so it is described rather than echoed
        if (Uri.TryCreate(url, UriKind.Absolute, out var parsed) == false)
            return "(unparsable)";

        return string.IsNullOrEmpty(parsed.Authority)
            ? $"{parsed.Scheme}:(no host)"
            : $"{parsed.Scheme}://{parsed.Authority}";
    }
}
