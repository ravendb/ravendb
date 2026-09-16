using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// Every embed state that has no widget to show still has to say something: a blank iframe is the failure
/// mode both the visitor and the operator can do nothing with. An expired link is the *normal* end state
/// of an embed, not an edge case, so these are pinned rather than assumed.
public class EmbedNoticePageTests(ITestOutputHelper output) : QuillTestBase(output)
{
    private const string HostOrigin = "http://shop.example";

    [RavenFact(RavenTestCategory.Quill)]
    public async Task An_expired_link_renders_a_notice_instead_of_an_empty_body()
    {
        await using var app = await NewAppAsync();
        var channelId = await ProvisionAsync(app, [HostOrigin]);
        var token = (await app.MintEmbedLinkAsync(new MintEmbedLinkRequest(channelId))).Token;
        await ExpireAsync(app, token);

        using var response = await GetPageAsync(app, token);

        Assert.Equal(HttpStatusCode.Gone, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.StartsWith("text/html", response.Content.Headers.ContentType!.ToString());
        Assert.Contains("This conversation has ended", body);
        // the host page hears about it too, so it can mint a replacement
        Assert.Contains("\"expired\"", body);
        Assert.Contains("raven-quill", body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task An_unknown_link_renders_a_notice()
    {
        await using var app = await NewAppAsync();
        await ProvisionAsync(app, [HostOrigin]);

        using var response = await GetPageAsync(app, new string('a', 32));

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Contains("This conversation is not available", await response.Content.ReadAsStringAsync());
    }

    /// The notice pages carry no conversation data and no controls, and they exist precisely to be
    /// readable inside a frame the CSP would otherwise blank out.
    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_notice_page_omits_frame_ancestors_but_keeps_the_rest_of_the_csp()
    {
        await using var app = await NewAppAsync();
        var channelId = await ProvisionAsync(app, [HostOrigin]);
        var token = (await app.MintEmbedLinkAsync(new MintEmbedLinkRequest(channelId))).Token;
        await ExpireAsync(app, token);

        using var response = await GetPageAsync(app, token);

        var csp = response.Headers.GetValues("Content-Security-Policy").Single();
        Assert.DoesNotContain("frame-ancestors", csp);
        Assert.Contains("default-src 'none'", csp);
        Assert.DoesNotContain("unsafe-inline", csp);
        Assert.Equal("no-referrer", response.Headers.GetValues("Referrer-Policy").Single());
    }

    private static async Task<string> ProvisionAsync(QuillApp app, string[] origins)
    {
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "demo", Name = "Demo", SystemPrompt = "You help.",
            ConnectionStringName = app.Host.ConnectionStringName,
        });

        var channel = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, "demo", origins));

        return channel.ChannelId;
    }

    private static async Task ExpireAsync(QuillApp app, string token)
    {
        using var session = app.Store.OpenAsyncSession(app.Slug);
        var link = await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + token);
        link.ExpiresAt = DateTime.UtcNow.AddMinutes(-1);
        await session.SaveChangesAsync();
    }

    private static Task<HttpResponseMessage> GetPageAsync(QuillApp app, string token) =>
        app.Host.Client.GetAsync(QuillRoutes.EmbedPage(app.Slug, token));
}
