using Discord;

namespace Raven.Quill.Discord;

internal sealed record DiscordBotUser(string BotUserId, string BotUsername);

internal sealed record DiscordBotIdentity(
    string ApplicationId,
    string BotUserId,
    string BotUsername);

internal static class DiscordBotTokenValidation
{
    private const string TokenCheck = "the token check";

    private const string UnrecognizedUser = "discord returned an unrecognized users/@me payload";

    public static Task<(DiscordBotUser? User, string? Error, bool DiscordResponded)> CheckBotTokenAsync(
        this DiscordSdk sdk, string botToken, CancellationToken ct) =>
        ProbeAsync<DiscordBotUser>(ct, async () =>
        {
            var user = await sdk.GetCurrentUserAsync(botToken, ct);
            return BotUserOf(user.Id, user.Username, user.IsBot);
        });

    public static async Task<(DiscordBotIdentity? Identity, string? Error, bool DiscordResponded)> GetBotIdentityAsync(
        this DiscordSdk sdk, string botToken, CancellationToken ct)
    {
        await using var client = sdk.NewRestClient();
        return await ProbeAsync<DiscordBotIdentity>(ct, async () =>
        {
            await client.LoginAsync(TokenType.Bot, botToken, validateToken: false).WaitAsync(sdk.RequestTimeout, ct);

            var self = client.CurrentUser;
            var (user, error) = self is null
                ? (null, UnrecognizedUser)
                : BotUserOf(DiscordBotClient.Snowflake(self.Id), self.Username, self.IsBot);
            if (user is null)
                return (null, error);

            var application = await client.GetApplicationInfoAsync(sdk.NewRequestOptions(ct));
            if (application is null)
                return (null, "discord returned an unrecognized oauth2/applications/@me payload");

            return (new DiscordBotIdentity(DiscordBotClient.Snowflake(application.Id), user.BotUserId, user.BotUsername), null);
        });
    }

    private static (DiscordBotUser? User, string? Error) BotUserOf(string id, string? username, bool isBot)
    {
        if (string.IsNullOrEmpty(id) || string.IsNullOrEmpty(username))
            return (null, UnrecognizedUser);

        if (isBot == false)
            return (null, "that token belongs to a user account, not a bot; copy the token from the app's Bot page");

        return (new DiscordBotUser(id, username), null);
    }

    private static async Task<(T? Result, string? Error, bool DiscordResponded)> ProbeAsync<T>(
        CancellationToken ct, Func<Task<(T? Result, string? Error)>> probe) where T : class
    {
        try
        {
            var (result, error) = await probe();
            return (result, error, true);
        }
        catch (Exception e) when (ct.IsCancellationRequested == false && DiscordApiErrors.IsApiFailure(e))
        {
            return (null, DiscordApiErrors.Describe(e, TokenCheck), DiscordApiErrors.Responded(e));
        }
    }
}
