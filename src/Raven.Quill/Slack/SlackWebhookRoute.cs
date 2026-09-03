namespace Raven.Quill.Slack;

internal sealed class SlackWebhookRoute
{
    internal const string IdPrefix = "slack-webhooks/";

    internal static string IdFor(string token) => IdPrefix + token;

    public string? Id { get; set; }

    public string Database { get; set; } = "";

    public string ChannelId { get; set; } = "";
}
