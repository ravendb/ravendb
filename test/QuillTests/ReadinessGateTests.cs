using System.Net;
using FastTests;
using Microsoft.Extensions.DependencyInjection;
using QuillTests.E2E.Fixtures;
using Raven.Quill.Hosting;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// Readiness-gate middleware coverage. The middleware short-circuits non-
/// bootstrap /api/* requests with 503 until <see cref="IServerReady"/> flips.
/// These tests drive the gate by toggling the resolved <see cref="IServerReady"/>
/// instance directly — the WAF's MarkReady() call at host build time gets
/// overridden by an explicit MarkFailed() before each gated request.
public class ReadinessGateTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Gate_503_response_does_not_leak_LastError_to_unauthenticated_callers()
    {
        // C4 (Copilot review #4365219160): MarkFailed call sites pass ex.Message
        // (license-API errors, RavenDB probe failures, etc.) — anything from
        // those exception strings would otherwise surface in the 503 body on
        // every gated /api/* request to anyone who can reach the bridge.
        // Server-side logging (verified at all 5 MarkFailed call sites — each
        // pairs with a logger.LogError/LogWarning) keeps the detail visible
        // to the operator; the response body just needs the public-safe
        // static text.
        var store = GetDocumentStore();
        using var factory = new ApplianceWebApplicationFactory(
            licenseApiUrl: "http://unused-in-unit-tests",
            setupPackagePath: NewDataPath(forceCreateDir: true),
            applianceStore: store,
            configureOptions: opts => opts.ConfigDatabase = store.Database);

        // Force the gate path: WAF's CreateHost calls MarkReady() at build
        // time; override here so the next /api/* request 503s through the
        // middleware.
        const string secretishError = "redis://internal-prod-host:6379/sensitive-path";
        factory.Services.GetRequiredService<IServerReady>().MarkFailed(secretishError);

        var client = factory.CreateClient();
        var resp = await client.GetAsync("/api/apps");

        Assert.Equal(HttpStatusCode.ServiceUnavailable, resp.StatusCode);

        var body = await resp.Content.ReadAsStringAsync();
        Assert.Contains("appliance is not ready yet", body, StringComparison.Ordinal);
        Assert.DoesNotContain(secretishError, body, StringComparison.Ordinal);
        // The field itself must be absent (not just empty) so the JSON shape
        // doesn't advertise that there's an error string to fish for in logs.
        Assert.DoesNotContain("lastError", body, StringComparison.Ordinal);
    }
}
