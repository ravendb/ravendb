using System.IO;
using System.Net;
using FastTests;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.FileProviders;
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

    /// Stands in for wwwroot/expired.html, which only the web build emits: the gate serves whatever
    /// ExpiryNotice loaded, and these tests pin that plumbing rather than the page.
    private const string StubNoticePage =
        "<!doctype html><html><body>This Quill build has expired (stub notice)</body></html>";

    [RavenFact(RavenTestCategory.Quill)]
    public async Task An_expired_build_answers_the_root_with_the_notice_page()
    {
        using var factory = NewFactory(expired: true);

        using var response = await factory.CreateClient().GetAsync("/");

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.StartsWith("text/html", response.Content.Headers.ContentType!.ToString());
        Assert.Equal(StubNoticePage, await response.Content.ReadAsStringAsync());
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
        Assert.Equal(StubNoticePage, await response.Content.ReadAsStringAsync());
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
        Assert.Equal(StubNoticePage, await response.Content.ReadAsStringAsync());
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

    [RavenFact(RavenTestCategory.Quill)]
    public void The_notice_is_read_from_the_web_root()
    {
        var webRoot = NewDataPath(forceCreateDir: true);
        File.WriteAllText(Path.Combine(webRoot, ExpiryNotice.FileRelativePath), StubNoticePage);

        var notice = ExpiryNotice.Load(new StubWebHostEnvironment { WebRootPath = webRoot });

        Assert.Equal(StubNoticePage, notice.Page);
    }

    /// The image always ships the page, so a missing one is a broken build - and an expired build has
    /// nothing else to serve, so it must fail loudly rather than answer every request with nothing.
    [RavenFact(RavenTestCategory.Quill)]
    public void A_missing_notice_is_a_broken_build_and_throws()
    {
        var webRoot = NewDataPath(forceCreateDir: true);

        var e = Assert.Throws<InvalidOperationException>(() =>
            ExpiryNotice.Load(new StubWebHostEnvironment { WebRootPath = webRoot }));

        Assert.Contains(ExpiryNotice.FileRelativePath, e.Message);
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

            services.RemoveAll<ExpiryNotice>();
            services.AddSingleton(ExpiryNotice.FromHtml(StubNoticePage));
        });

    private sealed class StubWebHostEnvironment : IWebHostEnvironment
    {
        public string WebRootPath { get; set; } = "";
        public IFileProvider WebRootFileProvider { get; set; } = new NullFileProvider();
        public string ApplicationName { get; set; } = "";
        public IFileProvider ContentRootFileProvider { get; set; } = new NullFileProvider();
        public string ContentRootPath { get; set; } = "";
        public string EnvironmentName { get; set; } = "";
    }
}
