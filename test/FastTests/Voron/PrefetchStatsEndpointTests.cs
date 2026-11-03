using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using Sparrow.Platform;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Voron;

public class PrefetchStatsEndpointTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public async Task ReturnsThePrefetchRingCounters()
    {
        using var store = GetDocumentStore();
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new { Name = "x" }, "items/1");
            await session.SaveChangesAsync();
        }

        using var client = new HttpClient();
        var json = await client.GetStringAsync($"{Server.WebUrl}/admin/debug/io-metrics/prefetch");
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        Assert.Equal(PlatformDetails.RunningOnLinux, root.GetProperty("IsAvailable").GetBoolean());
        Assert.True(root.GetProperty("MaxFiles").GetInt32() > 0);
        foreach (var name in new[] { "Submitted", "DroppedFull", "DroppedStale", "Failed" })
        {
            var value = root.GetProperty(name);
            if (PlatformDetails.RunningOnLinux)
                Assert.True(value.GetInt64() >= 0, name);
            else
                Assert.Equal(JsonValueKind.Null, value.ValueKind);
        }
        Assert.True(root.TryGetProperty("SegmentReadAheadWarmUp", out _));
    }
}
