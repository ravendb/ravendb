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
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Embed_page_renders_prior_conversation_history()
    {
        await using var app = await NewAppAsync();

        var now = DateTime.UtcNow;
        await SeedConversationAsync(app.Store, app.Slug, "chats/embed", "demo", now.AddMinutes(-5),
            turns: [("user", "hello there"), ("assistant", "hi, how can I help?")]);
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "demo", Name = "Demo", SystemPrompt = "You help.", ConnectionStringName = Host.ConnectionStringName,
        });
        var channel = await app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.IFrame, "demo", Array.Empty<string>()));

        // ConversationId is set directly: the mint EP doesn't expose it (the server binds it on the first live-LLM turn).
        var token = (await app.MintEmbedLinkAsync(new MintEmbedLinkRequest(channel.ChannelId))).Token;
        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            var link = await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + token);
            link.ConversationId = "chats/embed";
            await session.SaveChangesAsync();
        }

        var html = await app.GetEmbedPageAsync(token);

        Assert.DoesNotContain("__HISTORY__", html);
        Assert.Contains("for (const turn of [{", html);
        Assert.Contains("hello there", html);
        Assert.Contains("hi, how can I help?", html);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Embed_page_for_a_fresh_link_renders_an_empty_feed()
    {
        await using var app = await NewAppAsync();
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "demo", Name = "Demo", SystemPrompt = "You help.", ConnectionStringName = Host.ConnectionStringName,
        });
        var channel = await app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.IFrame, "demo", Array.Empty<string>()));

        var token = (await app.MintEmbedLinkAsync(new MintEmbedLinkRequest(channel.ChannelId))).Token;

        var html = await app.GetEmbedPageAsync(token);

        Assert.DoesNotContain("__HISTORY__", html);
        Assert.Contains("for (const turn of [])", html);
    }
}
