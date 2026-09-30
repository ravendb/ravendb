using Raven.Quill.Channels;

namespace Raven.Quill.Discord;

internal sealed class DiscordBotReservation : IChannelBotReservation
{
    internal const string IdPrefix = "discord-bots/";

    internal static string IdFor(string botUserId) => IdPrefix + botUserId;

    public string? Id { get; set; }

    public string Database { get; set; } = "";

    public string ChannelId { get; set; } = "";
}
