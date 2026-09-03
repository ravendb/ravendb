using QuillTests.E2E.Fixtures;
using Raven.Quill.Metrics;
using Tests.Infrastructure;
using Xunit;
using static QuillTests.E2E.Fixtures.ConversationSeed;

namespace QuillTests;

[Collection(QuillFanOutCollection.Name)]
public class TokensByAppEndpointTests(ITestOutputHelper output, QuillCollectionHost collection)
    : QuillTestBase(output, collection)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task TokensByApp_sums_tokens_per_app_sorted_descending()
    {
        await using var appOne = await NewAppAsync();
        await using var appTwo = await NewAppAsync();

        var now = DateTime.UtcNow;
        await SeedConversationAsync(appOne.Store, appOne.Slug, "chats/a", "demo", now.AddHours(-1), tokens: 100);
        await SeedConversationAsync(appOne.Store, appOne.Slug, "chats/b", "demo", now.AddDays(-10), tokens: 50);
        await SeedConversationAsync(appTwo.Store, appTwo.Slug, "chats/c", "demo", now.AddHours(-2), tokens: 400);

        var result = await Host.GetTokensByAppAsync();

        var apps = result.Apps;
        Assert.Equal(2, apps.Length);

        // all-time (no window): appOne = 100 + 50, incl. the -10d conversation
        Assert.Equal(appTwo.Slug, apps[0].Slug);
        Assert.Equal(400, apps[0].Tokens);
        Assert.Equal(appOne.Slug, apps[1].Slug);
        Assert.Equal(150, apps[1].Tokens);
    }
}
