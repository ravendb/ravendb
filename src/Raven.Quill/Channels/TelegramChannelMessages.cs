using Raven.Quill.Telegram;

namespace Raven.Quill.Channels;

/// Per-channel overrides for the bot's canned replies; a null field falls back to the default text.
public sealed class TelegramChannelMessages
{
    internal const int MaxLength = TelegramMessageSplitter.TelegramApiMessageLimit;

    public string? Greeting { get; set; }

    public string? ConversationCleared { get; set; }

    public string? UsernameMissing { get; set; }

    public string? PhoneNumberRequest { get; set; }

    public string? SharePhoneNumberButton { get; set; }

    public string? OwnContactRequired { get; set; }

    public string? PhoneNumberReceived { get; set; }

    public string? NotConfigured { get; set; }

    public string? Overloaded { get; set; }

    public string? SomethingWentWrong { get; set; }

    public string? GroupChatRefusal { get; set; }

    internal bool HasAnyOverride =>
        Fields().Any(entry => entry.Value is not null);

    internal bool TryNormalize(out string? error)
    {
        error = null;

        foreach (var (name, value, set) in Fields())
        {
            var cleaned = string.IsNullOrWhiteSpace(value) ? null : value.Trim();
            if (cleaned is not null)
            {
                if (cleaned.Length > MaxLength)
                {
                    error = $"messages.{name} exceeds {MaxLength} characters";
                    return false;
                }

                if (cleaned.Any(c => char.IsControl(c) && c is not '\n' and not '\r'))
                {
                    error = $"messages.{name} contains control characters";
                    return false;
                }
            }

            set(cleaned);
        }

        return true;
    }

    private (string Name, string? Value, Action<string?> Set)[] Fields() =>
    [
        ("greeting", Greeting, v => Greeting = v),
        ("conversationCleared", ConversationCleared, v => ConversationCleared = v),
        ("usernameMissing", UsernameMissing, v => UsernameMissing = v),
        ("phoneNumberRequest", PhoneNumberRequest, v => PhoneNumberRequest = v),
        ("sharePhoneNumberButton", SharePhoneNumberButton, v => SharePhoneNumberButton = v),
        ("ownContactRequired", OwnContactRequired, v => OwnContactRequired = v),
        ("phoneNumberReceived", PhoneNumberReceived, v => PhoneNumberReceived = v),
        ("notConfigured", NotConfigured, v => NotConfigured = v),
        ("overloaded", Overloaded, v => Overloaded = v),
        ("somethingWentWrong", SomethingWentWrong, v => SomethingWentWrong = v),
        ("groupChatRefusal", GroupChatRefusal, v => GroupChatRefusal = v),
    ];
}

internal sealed record ResolvedTelegramMessages(
    string Greeting,
    string ConversationCleared,
    string UsernameMissing,
    string PhoneNumberRequest,
    string SharePhoneNumberButton,
    string OwnContactRequired,
    string PhoneNumberReceived,
    string NotConfigured,
    string Overloaded,
    string SomethingWentWrong,
    string GroupChatRefusal)
{
    internal static readonly ResolvedTelegramMessages Defaults = new(
        Greeting: "Hi! Ask me anything and I'll answer. Send /clear anytime to start a fresh conversation.",
        ConversationCleared: "Conversation cleared. The next message starts a fresh one.",
        UsernameMissing: "This assistant needs your Telegram username. Set one in Telegram Settings and send your message again.",
        PhoneNumberRequest: "This assistant needs your phone number. Tap the button below to share it, then send your message again.",
        SharePhoneNumberButton: "Share phone number",
        OwnContactRequired: "That looks like someone else's contact. Tap the button below to share your own number.",
        PhoneNumberReceived: "Thanks, got your phone number. Now send your message again.",
        NotConfigured: "This assistant is not fully configured yet. Please contact whoever set up this bot.",
        Overloaded: "I'm still working through your earlier messages, so that one didn't make it. Please resend it once I've replied.",
        SomethingWentWrong: "Sorry - something went wrong handling that message. Please try again.",
        GroupChatRefusal: "I only work in one-on-one chats. Message me directly to start a conversation.");

    internal static ResolvedTelegramMessages Resolve(TelegramChannelMessages? overrides) => overrides is null
        ? Defaults
        : new ResolvedTelegramMessages(
            Pick(overrides.Greeting, Defaults.Greeting),
            Pick(overrides.ConversationCleared, Defaults.ConversationCleared),
            Pick(overrides.UsernameMissing, Defaults.UsernameMissing),
            Pick(overrides.PhoneNumberRequest, Defaults.PhoneNumberRequest),
            Pick(overrides.SharePhoneNumberButton, Defaults.SharePhoneNumberButton),
            Pick(overrides.OwnContactRequired, Defaults.OwnContactRequired),
            Pick(overrides.PhoneNumberReceived, Defaults.PhoneNumberReceived),
            Pick(overrides.NotConfigured, Defaults.NotConfigured),
            Pick(overrides.Overloaded, Defaults.Overloaded),
            Pick(overrides.SomethingWentWrong, Defaults.SomethingWentWrong),
            Pick(overrides.GroupChatRefusal, Defaults.GroupChatRefusal));

    private static string Pick(string? overrideText, string defaultText) =>
        string.IsNullOrWhiteSpace(overrideText) ? defaultText : overrideText;
}
