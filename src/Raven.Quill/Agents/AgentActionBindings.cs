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
}
