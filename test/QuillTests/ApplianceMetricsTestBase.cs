using FastTests;
using Newtonsoft.Json;
using QuillTests.E2E.Fixtures;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Wizard;
using Xunit;

namespace QuillTests;

/// <summary>
/// Shared scaffolding for the dashboard stats endpoint tests: hosts the
/// appliance against the test store, creates a per-app database, and seeds the
/// data the metric indexes aggregate (apps, agents, <c>@conversations</c> docs).
/// </summary>
public abstract class ApplianceMetricsTestBase(ITestOutputHelper output) : RavenTestBase(output)
{
    private protected ApplianceWebApplicationFactory NewApplianceFactory(IDocumentStore store) =>
        new(licenseApiUrl: "http://unused-in-unit-tests",
            setupPackagePath: NewDataPath(forceCreateDir: true),
            applianceStore: store,
            configureOptions: opts => opts.ConfigDatabase = store.Database);

    protected async Task<(string Name, IDisposable Cleanup)> CreatePerAppDatabaseAsync(IDocumentStore store)
    {
        var name = "per-app-" + Guid.NewGuid().ToString("N");
        await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(name)));
        return (name, Databases.EnsureDatabaseDeletion(name, store));
    }

    protected static async Task SeedAppAsync(IDocumentStore store, string slug, string database)
    {
        using var session = store.OpenAsyncSession();
        await session.StoreAsync(new App
        {
            Slug = slug,
            AppName = slug,
            Database = database,
            CdcTaskName = $"{slug}-cdc",
            CreatedAt = DateTime.UtcNow,
        }, id: $"apps/{slug}");
        await session.SaveChangesAsync();
    }

    /// <summary>A seed time inside the current month that is never a future bucket
    /// (<c>UsagePeriod</c> clamps the window end to now and drops future buckets, so
    /// fixed month-start offsets fail near month boundaries): <c>now - hoursBack</c>,
    /// clamped to the start of the month.</summary>
    protected static DateTime PastInMonth(DateTime now, int hoursBack)
    {
        var monthStart = new DateTime(now.Year, now.Month, 1, 0, 0, 0, DateTimeKind.Utc);
        var seed = now.AddHours(-hoursBack);
        return seed < monthStart ? monthStart : seed;
    }

    /// <summary>Provisions a real RavenDB AI agent in the per-app DB (connection
    /// string + agent are pure maintenance ops; no LLM is dialed). Used to drive
    /// the configured-agent count.</summary>
    protected static async Task SeedAgentAsync(
        IDocumentStore store, string database, string name, string connectionStringName = "demo-llm")
    {
        await store.Maintenance.Server.SendAsync(new PutServerWideConnectionStringOperation(
            new ServerWideConnectionString
            {
                ConnectionString = new AiConnectionString
                {
                    Name = connectionStringName,
                    ModelType = AiModelType.Chat,
                    OpenAiSettings = new OpenAiSettings { ApiKey = "sk-test", Model = "gpt-4o-mini" },
                }
            }));

        await store.AI.ForDatabase(database).CreateAgentAsync(new AiAgentConfiguration
        {
            Name = name,
            ConnectionStringName = connectionStringName,
            SampleObject = """{"reply":""}""",
        });
    }

    /// <summary>Writes a <c>Channel</c> doc into the per-app DB so the channels
    /// stats endpoint has something to count.</summary>
    protected static async Task SeedChannelAsync(
        IDocumentStore store, string database, string channelId, bool enabled,
        string agentId = "demo", string? displayName = null)
    {
        using var session = store.OpenAsyncSession(database);
        await session.StoreAsync(new Channel
        {
            Type = ChannelType.IFrame,
            DisplayName = displayName ?? channelId,
            AgentId = agentId,
            Enabled = enabled,
            CreatedAt = DateTime.UtcNow,
        }, $"{Channel.IdPrefix}{channelId}");
        await session.SaveChangesAsync();
    }

    /// <summary>Writes a <c>ConversationPreview</c> read-model doc — what the conversations list reads
    /// (one row per conversation, updated each turn by the agent router). Private: the read-model is a
    /// production side effect of a turn, so the only writer is <see cref="SeedConversationAsync"/>.</summary>
    private static Task SeedPreviewAsync(
        IDocumentStore store, string database, string conversationId, string agent, DateTime lastMessageAt,
        string? channelWidgetId = null, string lastUserPrompt = "", string lastAgentReply = "",
        IReadOnlyDictionary<string, string>? parameters = null) =>
        AgentRouter.UpsertPreviewAsync(store,
            new AgentRequest(database, AgentId: agent, ConversationId: conversationId, Prompt: lastUserPrompt,
                ChannelId: channelWidgetId is null ? "" : Channel.IdPrefix + channelWidgetId, Parameters: parameters ?? new Dictionary<string, string>()),
            agent, conversationId, reply: lastAgentReply, nowUtc: lastMessageAt, CancellationToken.None);

    /// <summary>Writes a document into the per-app <c>@conversations</c> collection
    /// (the collection the AI agent runtime owns) so the metric index can aggregate
    /// it without running a live turn.</summary>
    protected static async Task SeedConversationAsync(
        IDocumentStore store, string database, string id, string agent, DateTime createdAt,
        int messages = 1, long tokens = 0, (string Role, string Text)[]? turns = null,
        IReadOnlyDictionary<string, object>? parameters = null, string? channelWidgetId = null,
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
            // arbitrary AI-runtime shapes (array-of-parts / {reply} / null content, tool messages); the
            // per-index second offset keeps a deterministic order for transcript assertions.
            for (var i = 0; i < richMessages.Count; i++)
                conversation.Messages.Add(new SeedMessage { date = createdAt.AddSeconds(i), role = richMessages[i].Role, content = richMessages[i].Content });
        else if (turns is not null)
            foreach (var (role, text) in turns)
                conversation.Messages.Add(new SeedMessage { date = createdAt, role = role, content = text });
        else
            // Default messages are user prompts (the index counts user messages as invocations).
            for (var i = 0; i < messages; i++)
                conversation.Messages.Add(new SeedMessage { date = createdAt, role = "user" });

        await PutConversationAsync(store, database, id, conversation);

        // Production co-writes the read-model preview on every turn, so a seeded conversation must too —
        // the conversations list (and the detail endpoint's channel attribution) reads it. Last exchange
        // is derived from the seeded turns; params are flattened to strings to match the preview shape.
        var lastUser = turns is null ? "" : (turns.LastOrDefault(t => t.Role == "user").Text ?? "");
        var lastAgent = turns is null ? "" : (turns.LastOrDefault(t => t.Role is "assistant" or "agent").Text ?? "");
        await SeedPreviewAsync(store, database, id, agent, createdAt, channelWidgetId,
            lastUserPrompt: lastUser, lastAgentReply: lastAgent,
            parameters: parameters?.ToDictionary(kv => kv.Key, kv => kv.Value?.ToString() ?? ""));
    }

    /// <summary>Seeds a <c>@conversations</c> doc shaped like the real AI-runtime output:
    /// a <c>system</c> prompt message, <c>user</c>/<c>assistant</c> turns (assistant
    /// <c>content</c> as an array-of-parts), and a <c>tool</c> message — to exercise
    /// transcript role-filtering + array-content extraction. One user turn → invocations = 1.</summary>
    protected static Task SeedRealisticConversationAsync(
        IDocumentStore store, string database, string id, string agent, DateTime createdAt, long tokens = 0) =>
        // Single write through the one seed owner: the realistic AI-runtime shape goes in as the
        // @conversations doc, and the preview is co-written by SeedConversationAsync. system prompt +
        // array-of-parts user content + {reply} assistant content + a contentless tool-call step + a
        // tool message — exercises the detail endpoint's transcript role-filtering + array extraction.
        SeedConversationAsync(store, database, id, agent, createdAt, tokens: tokens, richMessages:
        [
            ("system", "You are a helpful assistant."),
            ("user", new List<object> { new Dictionary<string, object> { ["type"] = "text", ["text"] = "hello" } }),
            ("assistant", new Dictionary<string, object> { ["reply"] = "hi there" }),
            ("assistant", null),
            ("tool", "{\"result\":42}"),
        ]);

    private static Task PutConversationAsync(IDocumentStore store, string database, string id, SeedConversation conversation)
        => PutConversationDocAsync(store, database, id, conversation);

    /// <summary>PUTs an arbitrary object as a raw <c>@conversations</c> doc. Serialized with
    /// Newtonsoft + <c>TypeNameHandling.None</c> (the default RavenDB conventions emit
    /// <c>$type</c>/<c>$values</c> for object-typed members like the array-of-parts / <c>{reply}</c>
    /// content, which the server's GetConversationMessages reader can't parse). The real AI runtime
    /// stores clean JSON; this matches it. Pass a partial object to exercise the index's
    /// missing-member tolerance.</summary>
    protected static async Task PutConversationDocAsync(IDocumentStore store, string database, string id, object body)
    {
        var json = JsonConvert.SerializeObject(body, new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.None });
        using var commands = store.Commands(database);
        var document = commands.ParseJson(json);
        await commands.PutAsync(id, changeVector: null, document,
            new Dictionary<string, object> { [Constants.Documents.Metadata.Collection] = "@conversations" });
    }

    // Mirrors the real @conversations doc shape so the server's GetConversationMessages
    // operation (ConversationDocument.ToDocument) can read it: Parameters / LinkedConversations
    // / OpenActionCalls / Expires are all required (the metrics index tolerates their absence,
    // the operation does not). Property casing must match (PascalCase top-level, lowercase
    // role/content/date per message).
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
