using AiApplianceTests.E2E.Fixtures;
using FastTests;
using Raven.AiAppliance.Channels;
using Raven.AiAppliance.Wizard;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;

namespace AiApplianceTests;

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

    /// <summary>Provisions a real RavenDB AI agent in the per-app DB (connection
    /// string + agent are pure maintenance ops; no LLM is dialed). Used to drive
    /// the configured-agent count.</summary>
    protected static async Task SeedAgentAsync(
        IDocumentStore store, string database, string name, string connectionStringName = "demo-llm")
    {
        await store.Maintenance.ForDatabase(database).SendAsync(
            new PutConnectionStringOperation<AiConnectionString>(new AiConnectionString
            {
                Name = connectionStringName,
                ModelType = AiModelType.Chat,
                OpenAiSettings = new OpenAiSettings { ApiKey = "sk-test", Model = "gpt-4o-mini" },
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
        IDocumentStore store, string database, string channelId, bool enabled, string agentId = "demo")
    {
        using var session = store.OpenAsyncSession(database);
        await session.StoreAsync(new Channel
        {
            Type = ChannelType.IFrame,
            DisplayName = channelId,
            AgentId = agentId,
            Enabled = enabled,
            CreatedAt = DateTime.UtcNow,
        }, $"{Channel.IdPrefix}{channelId}");
        await session.SaveChangesAsync();
    }

    /// <summary>Writes a document into the per-app <c>@conversations</c> collection
    /// (the collection the AI agent runtime owns) so the metric index can aggregate
    /// it without running a live turn.</summary>
    protected static async Task SeedConversationAsync(
        IDocumentStore store, string database, string id, string agent, DateTime createdAt, int messages = 1, long tokens = 0)
    {
        using var session = store.OpenAsyncSession(database);
        var conversation = new SeedConversation
        {
            Agent = agent,
            CreatedAt = createdAt,
            LastMessageAt = createdAt,
            TotalUsage = new SeedUsage { TotalTokens = tokens },
        };
        for (var i = 0; i < messages; i++)
            conversation.Messages.Add(new SeedMessage { date = createdAt });

        await session.StoreAsync(conversation, id);
        session.Advanced.GetMetadataFor(conversation)[Constants.Documents.Metadata.Collection] = "@conversations";
        await session.SaveChangesAsync();
    }

    private sealed class SeedConversation
    {
        public string Agent { get; set; } = "";
        public DateTime CreatedAt { get; set; }
        public DateTime LastMessageAt { get; set; }
        public List<SeedMessage> Messages { get; set; } = [];
        public SeedUsage TotalUsage { get; set; } = new();
    }

    private sealed class SeedMessage
    {
        public DateTime date { get; set; }
    }

    private sealed class SeedUsage
    {
        public long TotalTokens { get; set; }
    }
}
