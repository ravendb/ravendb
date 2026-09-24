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
    public async Task Preview_expires_with_the_conversation_idle_window()
    {
        await using var app = await NewAppAsync();

        var now = new DateTime(2026, 8, 17, 12, 0, 0, DateTimeKind.Utc);
        var idleWindow = TimeSpan.FromHours(24);
        var request = new AgentRequest(app.Slug, "support", "chats/x", "hello", "",
            new Dictionary<string, JsonElement>(), idleWindow);

        await AgentRouter.RecordTurnAsync(
            app.Store, request, "support", "chats/x", "hi", tokens: 0, now, CancellationToken.None);

        using var session = app.Store.OpenAsyncSession(app.Slug);
        var preview = await session.LoadAsync<ConversationPreview>(ConversationPreview.IdFor("chats/x"));
        var expires = (string)session.Advanced.GetMetadataFor(preview)[
            Raven.Client.Constants.Documents.Metadata.Expires];

        Assert.Equal(now.Add(idleWindow), DateTime.Parse(expires, null, DateTimeStyles.RoundtripKind));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Preview_without_an_idle_window_gets_no_expiration()
    {
        await using var app = await NewAppAsync();

        var request = new AgentRequest(app.Slug, "support", "chats/y", "hello", "",
            new Dictionary<string, JsonElement>());

        await AgentRouter.RecordTurnAsync(
            app.Store, request, "support", "chats/y", "hi", tokens: 0, DateTime.UtcNow, CancellationToken.None);

        using var session = app.Store.OpenAsyncSession(app.Slug);
        var preview = await session.LoadAsync<ConversationPreview>(ConversationPreview.IdFor("chats/y"));

        Assert.False(session.Advanced.GetMetadataFor(preview)
            .ContainsKey(Raven.Client.Constants.Documents.Metadata.Expires));
    }
}
