using System.Globalization;
using Discord;

namespace Raven.Quill.Discord;

internal static class DiscordMessages
{
    private static readonly AllowedMentions SuppressMentions = new(AllowedMentionTypes.None);

    public static async Task<string> CreateAsync(
        this IMessageChannel channel, DiscordSdk sdk, string content, bool suppressEmbeds, CancellationToken ct)
    {
        var message = await GuardAsync("create message", ct, channel.SendMessageAsync(
            content, allowedMentions: SuppressMentions, options: sdk.NewRequestOptions(ct), flags: FlagsFor(suppressEmbeds)));

        return Snowflake(message.Id);
    }

    public static Task EditAsync(
        this IMessageChannel channel, DiscordSdk sdk, string messageId, string content, bool suppressEmbeds, CancellationToken ct)
    {
        var id = ParseSnowflake(messageId, "message id");
        return GuardAsync("edit message", ct, channel.ModifyMessageAsync(id, message =>
        {
            message.Content = content;
            message.AllowedMentions = SuppressMentions;
            message.Flags = FlagsFor(suppressEmbeds);
        }, sdk.NewRequestOptions(ct)));
    }

    public static Task TypingAsync(this IMessageChannel channel, DiscordSdk sdk, CancellationToken ct) =>
        GuardAsync("typing indicator", ct, channel.TriggerTypingAsync(sdk.NewRequestOptions(ct)));

    internal static string Snowflake(ulong id) => id.ToString(CultureInfo.InvariantCulture);

    internal static ulong ParseSnowflake(string value, string what) =>
        ulong.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var id)
            ? id
            : throw new DiscordApiException($"'{value}' is not a Discord {what}");

    private static async Task<TResult> GuardAsync<TResult>(string what, CancellationToken ct, Task<TResult> call)
    {
        try
        {
            return await call;
        }
        catch (Exception e) when (ct.IsCancellationRequested == false && DiscordApiErrors.IsApiFailure(e))
        {
            throw new DiscordApiException(DiscordApiErrors.Describe(e, what), DiscordApiErrors.StatusOf(e), e);
        }
    }

    private static async Task GuardAsync(string what, CancellationToken ct, Task call)
    {
        await GuardAsync(what, ct, Completed(call));

        static async Task<bool> Completed(Task call)
        {
            await call;
            return true;
        }
    }

    private static MessageFlags FlagsFor(bool suppressEmbeds) =>
        suppressEmbeds ? MessageFlags.SuppressEmbeds : MessageFlags.None;
}
