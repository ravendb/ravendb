using System.Net;
using Discord.Net;

namespace Raven.Quill.Discord;

internal static class DiscordApiErrors
{
    internal const int InvalidTokenClose = 4004;

    internal const string TokenRejected =
        "discord rejected the bot token; reset it on the app's Bot page and try again";

    public static bool IsTokenRejected(Exception e) => e switch
    {
        HttpException { HttpCode: HttpStatusCode.Unauthorized } => true,
        DiscordApiException { Status: HttpStatusCode.Unauthorized } => true,
        _ => CloseOf(e)?.CloseCode == InvalidTokenClose,
    };

    public static bool IsFixableInDiscord(int closeCode) => closeCode is 4013 or 4014;

    public static WebSocketClosedException? CloseOf(Exception e) =>
        e as WebSocketClosedException ?? e.InnerException as WebSocketClosedException;
}
