using System.Globalization;
using System.Text.Json;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Quill.Agents;
using Raven.Quill.Logging;
using Raven.Quill.Metrics;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillAgentActionsCollection.Name)]
public class ConversationLifetimeE2ETests(ITestOutputHelper output, QuillCollectionHost collection)
    : QuillTestBase(output, collection)
{
    private const string AgentId = "lifetime";

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Transcript_expires_with_the_idle_window_and_the_preview_with_the_retention()
    {
        await using var mock = await MockQuillServices.StartAsync(new FinalTurn("""{"reply":"ok"}"""));

        var app = await ProvisionAsync(mock);
        var config = (await app.GetAgentAsync(AgentId)).Configuration;

        var idleWindow = TimeSpan.FromHours(24);
        var retention = TimeSpan.FromDays(30);
        var request = new AgentRequest(app.Slug, AgentId, "chats/lifetime-e2e", "hello", "",
            new Dictionary<string, JsonElement>(),
            new ConversationLifetime(idleWindow, retention));

        var before = DateTime.UtcNow;
        var result = await Router(app).RunAsync(request, _ => ValueTask.CompletedTask, config, CancellationToken.None);
        var after = DateTime.UtcNow;

        using var session = app.Store.OpenAsyncSession(app.Slug);

        var conversation = await session.LoadAsync<object>(result.ConversationId);
        var conversationExpires = ExpiresOf(session.Advanced.GetMetadataFor(conversation));
        Assert.InRange(conversationExpires, before.Add(idleWindow), after.Add(idleWindow));

        var preview = await session.LoadAsync<ConversationPreview>(
            ConversationPreview.IdFor(result.ConversationId));
        var previewExpires = ExpiresOf(session.Advanced.GetMetadataFor(preview));
        Assert.InRange(previewExpires, before.Add(retention), after.Add(retention));
    }

    private static DateTime ExpiresOf(Raven.Client.Documents.Session.IMetadataDictionary metadata) =>
        DateTime.Parse((string)metadata[Raven.Client.Constants.Documents.Metadata.Expires],
            null, DateTimeStyles.RoundtripKind);

    private static AgentRouter Router(QuillApp app) =>
        new(app.Store,
            new WebhookActionExecutor(new SingleClientFactory(), new QuillLogger<WebhookActionExecutor>()),
            new QuillLogger<AgentRouter>());

    private sealed class SingleClientFactory : IHttpClientFactory
    {
        public HttpClient CreateClient(string name) => new();
    }

    private async Task<QuillApp> ProvisionAsync(MockQuillServices mock)
    {
        var app = await NewAppAsync();

        var connectionStringName = "mock-llm-" + Guid.NewGuid().ToString("N");
        await Host.PostConnectionStringAsync(new AiConnectionString
        {
            Name = connectionStringName,
            ModelType = AiModelType.Chat,
            OpenAiSettings = new OpenAiSettings("test-key", mock.BaseAddress + "/", "mock-model"),
        });

        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = AgentId,
            Name = "Lifetime",
            SystemPrompt = "You answer questions.",
            ConnectionStringName =
                ServerWideConnectionString.GetDatabaseRecordConnectionStringName(connectionStringName),
        });

        return app;
    }
}
