using System.Net;
using QuillTests.E2E.Fixtures;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class SpaFallbackTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_post_to_a_retired_route_is_refused_instead_of_answered_with_the_dashboard()
    {
        using var webhook = await Host.Client.PostAsync("/webhooks/slack/stale-token", new StringContent("{}"));
        using var page = await Host.Client.GetAsync("/apps/some-app/channels");

        Assert.Equal(HttpStatusCode.NotFound, webhook.StatusCode);
        Assert.Equal(HttpStatusCode.OK, page.StatusCode);
        Assert.StartsWith("text/html", page.Content.Headers.ContentType!.ToString());
    }
}
