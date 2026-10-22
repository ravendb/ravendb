using System.Security.Cryptography;
using System.Text;
using Raven.Quill.Channels;

namespace Raven.Quill.Slack;

internal sealed class SlackAppTokenReservation : IChannelBotReservation
{
    internal const string IdPrefix = "slack-app-tokens/";

    internal static string IdFor(string appToken) =>
        IdPrefix + Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(appToken)))[..32];

    public string? Id { get; set; }

    public string Database { get; set; } = "";

    public string ChannelId { get; set; } = "";
}
