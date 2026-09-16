namespace Raven.Quill.Metrics;

// Read-model for the conversations list: one doc per conversation carrying exactly what the list row
// needs (last exchange, params, channel, timestamps), updated on each turn. Lets the list be a single
// indexed query instead of N GetConversationMessages AI calls. The full transcript stays on the AI
// conversation (read by the detail endpoint).
internal sealed class ConversationPreview
{
    internal const string IdPrefix = "conversation-previews/";

    // System (@) collection: keeps this internal read-model out of the tables/collections views by the
    // same @-filter that hides @conversations. Mapped via QuillConventions.FindCollectionName.
    // No hyphen, so the raw index map can reference it as docs.@ConversationPreviews.
    internal const string Collection = "@ConversationPreviews";

    internal static string IdFor(string conversationId) => IdPrefix + conversationId;

    public string? Id { get; set; }

    public string ConversationId { get; set; } = "";

    public string Agent { get; set; } = "";

    // full channel document id (channels/<guid>) of the serving channel; empty for a direct chat
    public string ChannelId { get; set; } = "";

    // the conversation's bound (agent-declared) parameters, shown in the list
    public Dictionary<string, string> Parameters { get; set; } = new();

    public string LastUserPrompt { get; set; } = "";

    public string LastAgentReply { get; set; } = "";

    public DateTime CreatedAt { get; set; }

    public DateTime LastMessageAt { get; set; }
}
