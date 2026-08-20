using System.Text.Json;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;
using static QuillTests.E2E.Fixtures.ConversationSeed;

namespace QuillTests;

public class EmbedPageHistoryTests(ITestOutputHelper output) : QuillTestBase(output)
{
    private const string EmbedOrigin = "http://shop.example";

    private sealed record SeededWidget(QuillApp App, string Token) : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => App.DisposeAsync();
    }

    private async Task<SeededWidget> SeedWidgetAsync(bool withHistory, string conversationId = "chats/embed")
    {
        var app = await NewAppAsync();

        if (withHistory)
        {
            // the seeded system prompt must never surface in the visitor-facing history
            await SeedConversationAsync(app.Store, app.Slug, conversationId, "demo", DateTime.UtcNow.AddMinutes(-5),
                turns: [("system", "You help."), ("user", "hello there"), ("assistant", "hi, how can I help?")]);
        }

        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "demo", Name = "Demo", SystemPrompt = "You help.", ConnectionStringName = Host.ConnectionStringName,
        });
        var channel = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, "demo", [EmbedOrigin]));

        var token = (await app.MintEmbedLinkAsync(new MintEmbedLinkRequest(channel.ChannelId))).Token;

        if (withHistory)
        {
            // ConversationId is set directly: the mint EP doesn't expose it (the server binds it on the first live-LLM turn).
            using var session = app.Store.OpenAsyncSession(app.Slug);
            var link = await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + token);
            link.ConversationId = conversationId;
            await session.SaveChangesAsync();
        }

        return new SeededWidget(app, token);
    }

    /// The shell hands the widget its config as a JSON block; reading it back the way the widget does is the
    /// only assertion that survives a markup change.
    private static JsonElement ReadConfig(string html)
    {
        const string open = "<script type=\"application/json\" id=\"rq-config\"";
        var start = html.IndexOf(open, StringComparison.Ordinal);
        Assert.True(start >= 0, "the embed page carries no rq-config block");

        var bodyStart = html.IndexOf('>', start) + 1;
        var bodyEnd = html.IndexOf("</script>", bodyStart, StringComparison.Ordinal);
        Assert.True(bodyEnd > bodyStart, "the rq-config block is not closed");

        return JsonDocument.Parse(html[bodyStart..bodyEnd]).RootElement;
    }

    private static (string Role, string Content)[] HistoryOf(JsonElement config) =>
        config.GetProperty("history").EnumerateArray()
            .Select(turn => (turn.GetProperty("role").GetString()!, turn.GetProperty("content").GetString()!))
            .ToArray();

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Embed_page_config_carries_prior_conversation_history()
    {
        await using var widget = await SeedWidgetAsync(withHistory: true);

        var html = await widget.App.GetEmbedPageAsync(widget.Token);

        var config = ReadConfig(html);
        Assert.Equal("live", config.GetProperty("mode").GetString());
        Assert.Equal($"/apps/{widget.App.Slug}/embed/{widget.Token}/chat", config.GetProperty("chatUrl").GetString());

        var history = HistoryOf(config);
        Assert.Equal([("user", "hello there"), ("assistant", "hi, how can I help?")], history);
    }

    /// The visitor-facing document must carry only what the widget renders - never timestamps, token usage
    /// or raw tool-call payloads from the stored conversation.
    [RavenFact(RavenTestCategory.Quill)]
    public async Task History_turns_carry_only_role_and_content()
    {
        await using var widget = await SeedWidgetAsync(withHistory: true);

        var config = ReadConfig(await widget.App.GetEmbedPageAsync(widget.Token));
        var turns = config.GetProperty("history").EnumerateArray().ToArray();

        Assert.NotEmpty(turns);
        Assert.All(turns, turn =>
            Assert.Equal(["role", "content"], turn.EnumerateObject().Select(p => p.Name).ToArray()));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Embed_page_for_a_fresh_link_carries_an_empty_history()
    {
        await using var widget = await SeedWidgetAsync(withHistory: false);

        var html = await widget.App.GetEmbedPageAsync(widget.Token);

        Assert.Empty(HistoryOf(ReadConfig(html)));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Embed_page_config_carries_the_resolved_theme()
    {
        await using var widget = await SeedWidgetAsync(withHistory: false);

        var config = ReadConfig(await widget.App.GetEmbedPageAsync(widget.Token));
        var theme = config.GetProperty("theme");

        Assert.Equal(WidgetTheme.Default.Light.ButtonColor, theme.GetProperty("light").GetProperty("buttonColor").GetString());
        Assert.Equal(WidgetTheme.Default.Dark.BackgroundColor, theme.GetProperty("dark").GetProperty("backgroundColor").GetString());
        Assert.Equal(WidgetTheme.Default.Appearance.ToString(), theme.GetProperty("appearance").GetString());
        Assert.Equal(WidgetTheme.Default.Radius.ToString(), theme.GetProperty("radius").GetString());
    }

    /// The whole point of a JSON block over a `window.` assignment: a message body can carry `</script>`
    /// without closing it.
    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_history_message_containing_a_script_close_tag_cannot_break_out_of_the_config_block()
    {
        const string hostile = "</script><script>alert(1)</script>";
        await using var widget = await SeedWidgetAsync(withHistory: false);

        await SeedConversationAsync(widget.App.Store, widget.App.Slug, "chats/hostile", "demo", DateTime.UtcNow.AddMinutes(-5),
            turns: [("user", hostile), ("assistant", "noted")]);
        using (var session = widget.App.Store.OpenAsyncSession(widget.App.Slug))
        {
            var link = await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + widget.Token);
            link.ConversationId = "chats/hostile";
            await session.SaveChangesAsync();
        }

        var html = await widget.App.GetEmbedPageAsync(widget.Token);

        Assert.DoesNotContain("<script>alert(1)</script>", html);
        Assert.Equal(hostile, HistoryOf(ReadConfig(html))[0].Content);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Embed_page_references_the_manifests_hashed_asset_paths()
    {
        await using var widget = await SeedWidgetAsync(withHistory: false);

        var html = await widget.App.GetEmbedPageAsync(widget.Token);

        Assert.Contains("<script type=\"module\" src=\"/widget/assets/widget-test123.js\"", html);
        Assert.Contains("<link rel=\"stylesheet\" href=\"/widget/assets/widget-test123.css\">", html);
        Assert.Contains("<link rel=\"modulepreload\" href=\"/widget/assets/vendor-test456.js\">", html);
    }
}
