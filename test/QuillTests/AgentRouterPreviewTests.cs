using System.Globalization;
using System.Text.Json;
using QuillTests.E2E.Fixtures;
using Raven.Quill.Agents;
using Raven.Quill.Metrics;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class AgentRouterPreviewTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Preview_of_a_retention_bound_conversation_expires_with_the_retention()
    {
        await using var app = await NewAppAsync();

        var now = new DateTime(2026, 8, 17, 12, 0, 0, DateTimeKind.Utc);
        var retention = TimeSpan.FromDays(30);
        var request = new AgentRequest(app.Slug, "support", "chats/x", "hello", "",
            new Dictionary<string, JsonElement>(),
            new ConversationLifetime(TimeSpan.FromHours(24), retention));

        await AgentRouter.UpsertPreviewAsync(
            app.Store, request, "support", "chats/x", "hi", now, CancellationToken.None);

        using var session = app.Store.OpenAsyncSession(app.Slug);
        var preview = await session.LoadAsync<ConversationPreview>(ConversationPreview.IdFor("chats/x"));
        var expires = (string)session.Advanced.GetMetadataFor(preview)[
            Raven.Client.Constants.Documents.Metadata.Expires];

        Assert.Equal(now.Add(retention), DateTime.Parse(expires, null, DateTimeStyles.RoundtripKind));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Preview_without_a_retention_gets_no_expiration()
    {
        await using var app = await NewAppAsync();

        var request = new AgentRequest(app.Slug, "support", "chats/y", "hello", "",
            new Dictionary<string, JsonElement>());

        await AgentRouter.UpsertPreviewAsync(
            app.Store, request, "support", "chats/y", "hi", DateTime.UtcNow, CancellationToken.None);

        using var session = app.Store.OpenAsyncSession(app.Slug);
        var preview = await session.LoadAsync<ConversationPreview>(ConversationPreview.IdFor("chats/y"));

        Assert.False(session.Advanced.GetMetadataFor(preview)
            .ContainsKey(Raven.Client.Constants.Documents.Metadata.Expires));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Preview_with_only_an_idle_window_gets_no_expiration()
    {
        await using var app = await NewAppAsync();

        var request = new AgentRequest(app.Slug, "support", "chats/z", "hello", "",
            new Dictionary<string, JsonElement>(),
            new ConversationLifetime(TimeSpan.FromHours(24), PreviewRetention: null));

        await AgentRouter.UpsertPreviewAsync(
            app.Store, request, "support", "chats/z", "hi", DateTime.UtcNow, CancellationToken.None);

        using var session = app.Store.OpenAsyncSession(app.Slug);
        var preview = await session.LoadAsync<ConversationPreview>(ConversationPreview.IdFor("chats/z"));

        Assert.False(session.Advanced.GetMetadataFor(preview)
            .ContainsKey(Raven.Client.Constants.Documents.Metadata.Expires));
    }
}
