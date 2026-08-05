using FastTests;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Quill.Agents;
using Raven.Quill.Channels;

namespace QuillTests.E2E.Fixtures;

/// Seeds the <c>@conversations</c> docs (and their read-model previews) the metric indexes aggregate — the one
/// thing with no EP. A helper, not a base class: needing it says nothing about which host a test wants, so
/// classes pull it in with <c>using static</c> instead of inheriting it.
public static class ConversationSeed
{
    /// A seed time inside the current month that is never a future bucket (which UsagePeriod drops).
    public static DateTime PastInMonth(DateTime now, int hoursBack)
    {
        var monthStart = new DateTime(now.Year, now.Month, 1, 0, 0, 0, DateTimeKind.Utc);
        var seed = now.AddHours(-hoursBack);
        return seed < monthStart ? monthStart : seed;
    }

    /// Writes the <c>ConversationPreview</c> read-model doc; production co-writes it on every turn.
    private static Task SeedPreviewAsync(
        IDocumentStore store, string database, string conversationId, string agent, DateTime lastMessageAt,
        string? channelId = null, string lastUserPrompt = "", string lastAgentReply = "",
        IReadOnlyDictionary<string, string>? parameters = null) =>
        AgentRouter.UpsertPreviewAsync(store,
            new AgentRequest(database, AgentId: agent, ConversationId: conversationId, Prompt: lastUserPrompt,
                ChannelId: channelId is null ? "" : Channel.IdPrefix + channelId,
                Parameters: parameters ?? new Dictionary<string, string>()),
            agent, conversationId, reply: lastAgentReply, nowUtc: lastMessageAt, CancellationToken.None);

    /// Seeds a <c>@conversations</c> doc so the metric index can aggregate it without running a live turn.
    public static async Task SeedConversationAsync(
        IDocumentStore store, string database, string id, string agent, DateTime createdAt,
        int messages = 1, long tokens = 0, (string Role, string Text)[]? turns = null,
        IReadOnlyDictionary<string, object>? parameters = null, string? channelId = null,
        IReadOnlyList<(string Role, object? Content)>? richMessages = null)
    {
        var conversation = new SeedConversation
        {
            Agent = agent,
            CreatedAt = createdAt,
            LastMessageAt = createdAt,
            TotalUsage = new SeedUsage { TotalTokens = tokens },
            Parameters = parameters is null ? new() : new Dictionary<string, object>(parameters),
        };
        if (richMessages is not null)
            // per-index second offset keeps a deterministic order for transcript assertions
            for (var i = 0; i < richMessages.Count; i++)
                conversation.Messages.Add(new SeedMessage { date = createdAt.AddSeconds(i), role = richMessages[i].Role, content = richMessages[i].Content });
        else if (turns is not null)
            foreach (var (role, text) in turns)
                conversation.Messages.Add(new SeedMessage { date = createdAt, role = role, content = text });
        else
            // default messages are user prompts (the index counts user messages as invocations)
            for (var i = 0; i < messages; i++)
                conversation.Messages.Add(new SeedMessage { date = createdAt, role = "user" });

        await PutConversationDocAsync(store, database, id, conversation);

        // production co-writes the preview on every turn, so a seeded conversation must too
        var lastUser = turns is null ? "" : (turns.LastOrDefault(t => t.Role == "user").Text ?? "");
        var lastAgent = turns is null ? "" : (turns.LastOrDefault(t => t.Role is "assistant" or "agent").Text ?? "");
        await SeedPreviewAsync(store, database, id, agent, createdAt, channelId,
            lastUserPrompt: lastUser, lastAgentReply: lastAgent,
            parameters: parameters?.ToDictionary(kv => kv.Key, kv => kv.Value?.ToString() ?? ""));
    }

    /// Seeds a <c>@conversations</c> doc shaped like real AI-runtime output, to exercise transcript role-filtering + array extraction.
    public static Task SeedRealisticConversationAsync(
        IDocumentStore store, string database, string id, string agent, DateTime createdAt, long tokens = 0) =>
        SeedConversationAsync(store, database, id, agent, createdAt, tokens: tokens, richMessages:
        [
            ("system", "You are a helpful assistant."),
            ("user", new List<object> { new Dictionary<string, object> { ["type"] = "text", ["text"] = "hello" } }),
            ("assistant", new Dictionary<string, object> { ["reply"] = "hi there" }),
            ("assistant", null),
            ("tool", "{\"result\":42}"),
        ]);

    /// PUTs a raw <c>@conversations</c> doc with <c>TypeNameHandling.None</c> so no <c>$type</c> leaks into object members the server can't parse.
    public static async Task PutConversationDocAsync(IDocumentStore store, string database, string id, object body)
    {
        var json = JsonConvert.SerializeObject(body, new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.None });
        using var commands = store.Commands(database);
        var document = commands.ParseJson(json);
        await commands.PutAsync(id, changeVector: null, document,
            new Dictionary<string, object> { [Constants.Documents.Metadata.Collection] = "@conversations" });
    }

    // Mirrors the real @conversations doc shape so GetConversationMessages can read it: the operation
    // requires Parameters/LinkedConversations/OpenActionCalls/Expires (the metrics index tolerates their
    // absence). Casing must match: PascalCase top-level, lowercase role/content/date per message.
    private sealed class SeedConversation
    {
        public string Agent { get; set; } = "";
        public Dictionary<string, object> Parameters { get; set; } = new();
        public List<string> LinkedConversations { get; set; } = [];
        public Dictionary<string, object> OpenActionCalls { get; set; } = new();
        public TimeSpan? Expires { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime LastMessageAt { get; set; }
        public List<SeedMessage> Messages { get; set; } = [];
        public SeedUsage TotalUsage { get; set; } = new();
    }

    private sealed class SeedMessage
    {
        public DateTime date { get; set; }
        public string? role { get; set; }
        public object? content { get; set; }
    }

    private sealed class SeedUsage
    {
        public long TotalTokens { get; set; }
    }
}
