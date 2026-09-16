using System.Net;
using Microsoft.Extensions.DependencyInjection;
using QuillTests.E2E.Fixtures;
using Raven.Quill.Hosting;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class ReadinessGateTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Gate_503_response_does_not_leak_LastError_to_unauthenticated_callers()
    {
        await using var host = await NewHostAsync(setupPackagePath: NewDataPath(forceCreateDir: true));

        // host starts Ready (MarkReady at build time); override to force the 503 gate path
        const string secretishError = "redis://internal-prod-host:6379/sensitive-path";
        host.Services.GetRequiredService<IServerReady>().MarkFailed(secretishError);

        var resp = await host.Client.GetAsync(QuillRoutes.Apps);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, resp.StatusCode);

        var body = await resp.Content.ReadAsStringAsync();
        Assert.Contains("appliance is not ready yet", body, StringComparison.Ordinal);
        Assert.DoesNotContain(secretishError, body, StringComparison.Ordinal);
        // field must be ABSENT not empty so no error string leaks
        Assert.DoesNotContain("lastError", body, StringComparison.Ordinal);
    }
}
