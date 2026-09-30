using QuillTests.E2E.Fixtures;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class ActivityEndpointTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Activity_returns_empty_feed()
    {
        await using var app = await NewAppAsync();

        var feed = await app.GetActivityAsync();
        Assert.Empty(feed);
    }
}
