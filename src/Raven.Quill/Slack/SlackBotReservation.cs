namespace Raven.Quill.Slack;

internal sealed class SlackBotReservation
{
    internal const string IdPrefix = "slack-bots/";

    internal static string IdFor(string teamId, string botUserId) => IdPrefix + teamId + "-" + botUserId;

    public string? Id { get; set; }

    public string Database { get; set; } = "";

    public string ChannelId { get; set; } = "";
}
