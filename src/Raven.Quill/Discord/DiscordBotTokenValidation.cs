using System.Net;
using Discord;
using Discord.Net;

namespace Raven.Quill.Discord;

internal sealed record DiscordBotIdentity(
    string ApplicationId,
    string BotUserId,
    string BotUsername);

internal sealed class DiscordApiException(string message, HttpStatusCode? status = null, Exception? inner = null)
    : Exception(message, inner)
{
    public HttpStatusCode? Status { get; } = status;
}

internal static class DiscordBotTokenValidation
{
    internal const string TokenRejected =
        "discord rejected the bot token; reset it on the app's Bot page and try again";

    public static async Task<(DiscordBotIdentity? Identity, string? Error, bool DiscordResponded)> GetBotIdentityAsync(
        this DiscordSdk sdk, string botToken, CancellationToken ct)
    {
        try
        {
            await using var client = sdk.NewRestClient();
            await client.LoginAsync(TokenType.Bot, botToken, validateToken: false).WaitAsync(sdk.RequestTimeout, ct);

            var user = client.CurrentUser;
            if (user is null || string.IsNullOrEmpty(user.Username))
                return (null, "discord returned an unrecognized users/@me payload", true);

            if (user.IsBot == false)
                return (null, "that token belongs to a user account, not a bot; copy the token from the app's Bot page", true);

            var application = await client.GetApplicationInfoAsync(sdk.NewRequestOptions(ct));
            if (application is null)
                return (null, "discord returned an unrecognized oauth2/applications/@me payload", true);

            return (new DiscordBotIdentity(
                DiscordBotClient.Snowflake(application.Id), DiscordBotClient.Snowflake(user.Id), user.Username), null, true);
        }
        catch (RateLimitedException)
        {
            return (null, "discord is rate-limiting the token check; try again shortly", false);
        }
        catch (HttpException e) when ((int)e.HttpCode >= 500)
        {
            return (null, $"the Discord API is unavailable (status {(int)e.HttpCode})", false);
        }
        catch (HttpException e) when (e.HttpCode is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden)
        {
            return (null, TokenRejected, true);
        }
        catch (HttpException e)
        {
            return (null, $"discord refused the token check: {DiscordBotClient.ErrorTextOf(e)}", true);
        }
        catch (TimeoutException)
        {
            return (null, "the Discord API did not respond while validating the bot token", false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested == false)
        {
            return (null, "the Discord API did not respond while validating the bot token", false);
        }
        catch (HttpRequestException e)
        {
            return (null, $"could not reach the Discord API: {e.Message}", false);
        }
    }
}
