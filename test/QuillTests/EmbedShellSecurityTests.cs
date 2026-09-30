using System.Net;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Embed;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// Pins the embed shell's response headers. These are the whole security story of a document that runs on
/// somebody else's website, so each one is asserted rather than assumed.
public class EmbedShellSecurityTests(ITestOutputHelper output) : QuillTestBase(output)
{
    private const string EmbedOrigin = "http://shop.example";

    private static async Task<string> MintForOriginsAsync(QuillApp app, string[] origins)
    {
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "demo", Name = "Demo", SystemPrompt = "You help.",
            ConnectionStringName = app.Host.ConnectionStringName,
        });
        var channel = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, "demo", origins));

        return (await app.MintEmbedLinkAsync(new MintEmbedLinkRequest(channel.ChannelId))).Token;
    }

    private static string CspOf(HttpResponseMessage response) =>
        response.Headers.TryGetValues("Content-Security-Policy", out var values) ? string.Join(" ", values) : "";

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Embed_page_csp_uses_a_nonce_and_no_unsafe_inline()
    {
        await using var app = await NewAppAsync();
        var token = await MintForOriginsAsync(app, [EmbedOrigin]);

        var response = await Host.Client.GetAsync(QuillRoutes.EmbedPage(app.Slug, token));
        var csp = CspOf(response);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("script-src 'self' 'nonce-", csp);
        Assert.Contains("style-src 'self' 'nonce-", csp);
        Assert.DoesNotContain("'unsafe-inline'", csp);
        Assert.DoesNotContain("'unsafe-eval'", csp);
        Assert.Contains("default-src 'none'", csp);
        Assert.Contains("base-uri 'none'", csp);
        Assert.Contains("form-action 'none'", csp);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task The_csp_nonce_matches_the_markup_and_differs_between_requests()
    {
        await using var app = await NewAppAsync();
        var token = await MintForOriginsAsync(app, [EmbedOrigin]);

        var first = await Host.Client.GetAsync(QuillRoutes.EmbedPage(app.Slug, token));
        var firstHtml = await first.Content.ReadAsStringAsync();
        var firstNonce = Regex.Match(CspOf(first), "'nonce-([^']+)'").Groups[1].Value;

        Assert.NotEmpty(firstNonce);
        Assert.Contains($"<style nonce=\"{firstNonce}\">", firstHtml);
        Assert.Contains($"id=\"rq-config\" nonce=\"{firstNonce}\"", firstHtml);
        Assert.Contains($"type=\"module\" src=\"/widget/assets/widget-test123.js\" nonce=\"{firstNonce}\"", firstHtml);

        var second = await Host.Client.GetAsync(QuillRoutes.EmbedPage(app.Slug, token));
        var secondNonce = Regex.Match(CspOf(second), "'nonce-([^']+)'").Groups[1].Value;

        Assert.NotEqual(firstNonce, secondNonce);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Embed_page_is_never_cached_and_never_sniffed()
    {
        await using var app = await NewAppAsync();
        var token = await MintForOriginsAsync(app, [EmbedOrigin]);

        var response = await Host.Client.GetAsync(QuillRoutes.EmbedPage(app.Slug, token));

        Assert.Equal("no-store", response.Headers.CacheControl?.ToString());
        Assert.Equal("nosniff", response.Headers.GetValues("X-Content-Type-Options").Single());
        Assert.Equal("no-referrer", response.Headers.GetValues("Referrer-Policy").Single());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Frame_ancestors_lists_the_channels_allowed_origins()
    {
        await using var app = await NewAppAsync();
        var token = await MintForOriginsAsync(app, [EmbedOrigin, "https://other.example"]);

        var csp = CspOf(await Host.Client.GetAsync(QuillRoutes.EmbedPage(app.Slug, token)));

        Assert.Contains("frame-ancestors", csp);
        Assert.Contains(EmbedOrigin, csp);
        Assert.Contains("https://other.example", csp);
    }

    /// A channel with no origins is the operator's explicit opt-in to open embedding, so the directive is
    /// dropped rather than narrowed - the rest of the policy still applies.
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Frame_ancestors_is_omitted_when_a_channel_has_no_allowed_origins()
    {
        await using var app = await NewAppAsync();
        var token = await MintForOriginsAsync(app, []);

        var response = await Host.Client.GetAsync(QuillRoutes.EmbedPage(app.Slug, token));
        var csp = CspOf(response);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.DoesNotContain("frame-ancestors", csp);
        Assert.Contains("default-src 'none'", csp);
    }

    /// A blank 200 looks like a hung widget; an operator debugging a bad image needs the 503 to say so.
    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_missing_widget_bundle_yields_503_not_a_blank_page()
    {
        await using var host = await NewHostAsync(configureServices: services =>
        {
            services.RemoveAll<WidgetAssets>();
            services.AddSingleton(WidgetAssets.Unavailable);
        });
        await using var app = await NewAppAsync(host);
        var token = await MintForOriginsAsync(app, [EmbedOrigin]);

        var response = await host.Client.GetAsync(QuillRoutes.EmbedPage(app.Slug, token));

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Contains("widget", await response.Content.ReadAsStringAsync(), StringComparison.OrdinalIgnoreCase);
    }
}
