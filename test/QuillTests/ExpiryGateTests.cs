using System.Net;
using System.Text.RegularExpressions;
using FastTests;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using QuillTests.E2E.Fixtures;
using Raven.Client.Properties;
using Raven.Quill.Auth;
using Raven.Quill.Hosting;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// An expired build answers one page on every path, and it does so from the composition root rather than
/// per request: the pipeline an expired build assembles is not the pipeline a live one assembles. That is
/// what these pin — a route reaching its endpoint is the failure, not a wrong status code.
public class ExpiryGateTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private static readonly DateTime StoppedOn = new(2026, 1, 2, 0, 0, 0, DateTimeKind.Utc);

    [RavenFact(RavenTestCategory.Quill)]
    public async Task An_expired_build_answers_the_root_with_the_notice_page()
    {
        using var factory = NewFactory(expired: true);

        using var response = await factory.CreateClient().GetAsync("/");

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.StartsWith("text/html", response.Content.Headers.ContentType!.ToString());

        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("This Quill build has expired", body);
        Assert.Contains("2026-01-02", body);
        // the command carries `&&`, and this is HTML
        Assert.Contains("docker compose pull &amp;&amp; docker compose up -d", body);
        Assert.Contains("quill-data", body);
    }

    /// Proves the gate precedes authentication: an unauthenticated operator gets the reason, not a 401 they
    /// would read as a lost API key.
    [RavenFact(RavenTestCategory.Quill)]
    public async Task An_expired_build_answers_the_api_with_the_page_rather_than_json_or_a_401()
    {
        using var factory = NewFactory(expired: true);
        var client = factory.CreateClient();
        client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        using var response = await client.GetAsync(QuillRoutes.Apps);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.StartsWith("text/html", response.Content.Headers.ContentType!.ToString());
        Assert.Contains("This Quill build has expired", await response.Content.ReadAsStringAsync());
    }

    /// The anonymous embed surface is mapped ahead of the SPA fallback and answers visitors, not operators;
    /// it is the one route a gate placed late would still serve.
    [RavenFact(RavenTestCategory.Quill)]
    public async Task An_expired_build_answers_the_anonymous_embed_surface()
    {
        using var factory = NewFactory(expired: true);

        using var response = await factory.CreateClient()
            .GetAsync(QuillRoutes.EmbedPage("some-app", new string('a', 32)));

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.StartsWith("text/html", response.Content.Headers.ContentType!.ToString());
        Assert.Contains("This Quill build has expired", await response.Content.ReadAsStringAsync());
    }

    /// 200 by choice: the container's HEALTHCHECK curls this and nothing else, and an expired appliance is
    /// not a crashing one to restart.
    [RavenFact(RavenTestCategory.Quill | RavenTestCategory.Monitoring)]
    public async Task An_expired_build_still_reports_healthy()
    {
        using var factory = NewFactory(expired: true);

        using var response = await factory.CreateClient().GetAsync("/healthz");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("expired", await response.Content.ReadAsStringAsync());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_live_build_adds_nothing_to_the_pipeline()
    {
        using var factory = NewFactory(expired: false);
        var client = factory.CreateClient();

        using var root = await client.GetAsync("/");

        Assert.NotEqual(HttpStatusCode.ServiceUnavailable, root.StatusCode);
        Assert.DoesNotContain("This Quill build has expired", await root.Content.ReadAsStringAsync());

        // /healthz reaches MapHealthChecks rather than the gate: a live build must add no delegate at all,
        // and this is the one route the gate answers with a body of its own.
        using var health = await client.GetAsync("/healthz");

        Assert.NotEqual("expired", await health.Content.ReadAsStringAsync());
    }

    /// The command is the one thing on the page the operator has to reproduce exactly, and a 503 page gives
    /// them nothing to click through to. The control ships inline for the same reason the rest of the page
    /// does — there is no bundle to load it from — and it reads the block it sits on rather than carrying its
    /// own copy of the command, so the button and the text it claims to copy cannot drift apart.
    [RavenFact(RavenTestCategory.Quill)]
    public async Task The_notice_page_carries_an_inline_copy_control_for_the_command()
    {
        using var factory = NewFactory(expired: true);

        using var response = await factory.CreateClient().GetAsync("/");
        var body = await response.Content.ReadAsStringAsync();

        Assert.Contains("""<pre id="q-command">docker compose pull &amp;&amp; docker compose up -d</pre>""", body);
        Assert.Single(Regex.Matches(body, Regex.Escape("docker compose pull")));
        Assert.Contains("""<button class="q-copy" type="button">""", body);
        Assert.Contains("navigator.clipboard", body);
        // nothing to fetch: the page renders with the SPA build and the widget bundle both absent
        Assert.DoesNotContain("<script src", body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void The_window_is_ninety_days_from_the_build_release_date()
    {
        var release = RavenVersionAttribute.Instance.ReleaseDate;

        Assert.Equal(release.AddDays(90), new QuillExpiry(release).ExpiresAt);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void The_verdict_flips_the_day_after_the_window_closes()
    {
        var expiresAt = RavenVersionAttribute.Instance.ReleaseDate.AddDays(90);

        Assert.False(new QuillExpiry(expiresAt.AddDays(-1)).IsExpired);
        Assert.False(new QuillExpiry(expiresAt).IsExpired);
        Assert.True(new QuillExpiry(expiresAt.AddDays(1)).IsExpired);
    }

    private ApplianceWebApplicationFactory NewFactory(bool expired) => new(
        setupPackagePath: NewDataPath(forceCreateDir: true),
        applianceStore: GetDocumentStore(),
        configureServices: services =>
        {
            services.RemoveAll<IQuillExpiry>();
            services.AddSingleton<IQuillExpiry>(new FakeQuillExpiry(expired, StoppedOn));
        });
}
