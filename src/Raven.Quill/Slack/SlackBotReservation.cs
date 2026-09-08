using Raven.Quill.Channels;

namespace Raven.Quill.Slack;

internal sealed class SlackBotReservation : IChannelBotReservation
{
    internal const string IdPrefix = "slack-bots/";

    internal static string IdFor(string teamId, string botUserId) => IdPrefix + teamId + "-" + botUserId;

    public string? Id { get; set; }

    public string Database { get; set; } = "";

    public string ChannelId { get; set; } = "";

    public string WebhookToken { get; set; } = "";
}
