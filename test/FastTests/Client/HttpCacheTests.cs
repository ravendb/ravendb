using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Http;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client;

public class HttpCacheTests : NoDisposalNeeded
{
    public HttpCacheTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task Can_Internally_Dispose_Http_Cache_Item()
    {
        using (var httpCache = new HttpCache(1024))
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (var httpCacheUpdate = new ManualResetEvent(false))
        using (var httpCacheFreeSpaceDone = new ManualResetEvent(false))
        {
            const string url = "http://localhost:8080";
            const string expectedName = "Grisha";
            var blittable = await GenerateBlittable(context, expectedName);

            httpCache.Set(url, changeVector: null, blittable);

            httpCache.ForTestingPurposesOnly().OnHttpCacheSetUpdate = () =>
            {
                httpCacheUpdate.Set();
                httpCacheFreeSpaceDone.WaitOne();
            };

            using (var releaseCacheItem = httpCache.Get(context, url, out _, out var result))
            {
                result.TryGet("Name", out string name);
                Assert.Equal(expectedName, name);

                var setTask = Task.Run(() => httpCache.Set(url, changeVector: null, blittable));
                httpCacheUpdate.WaitOne();
                httpCache.FreeSpace();
                httpCacheFreeSpaceDone.Set();

                await setTask;

                blittable = await GenerateBlittable(context, "Kotler");
                httpCache.Set(url, changeVector: null, blittable);

                Assert.Equal(1, releaseCacheItem.Item.Usages);
                Assert.NotNull(releaseCacheItem.Item.Allocation);

                result.TryGet("Name", out name);
                Assert.Equal(expectedName, name);
            }
        }
    }

    [Fact]
    public async Task Can_Internally_Dispose_Not_Found_Item()
    {
        using (var httpCache = new HttpCache(1024))
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (var httpCacheUpdate = new ManualResetEvent(false))
        using (var httpCacheFreeSpaceDone = new ManualResetEvent(false))
        {
            const string url = "http://localhost:8080";

            httpCache.SetNotFound(url, aggressivelyCached: false);

            httpCache.ForTestingPurposesOnly().OnHttpCacheNotFoundUpdate = () =>
            {
                httpCacheUpdate.Set();
                httpCacheFreeSpaceDone.WaitOne();
            };

            using (var releaseCacheItem = httpCache.Get(context, url, out _, out var result))
            {
                var setTask = Task.Run(() => httpCache.SetNotFound(url, aggressivelyCached: false));
                httpCacheUpdate.WaitOne();
                httpCache.FreeSpace();
                httpCacheFreeSpaceDone.Set();

                await setTask;

                Assert.Equal(1, releaseCacheItem.Item.Usages);
            }
        }
    }

    [Fact]
    public async Task Can_Internally_Dispose_Http_Cache_Item_Before_Setting_Not_Found()
    {
        using (var httpCache = new HttpCache(1024))
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (var httpCacheUpdate = new ManualResetEvent(false))
        using (var httpCacheFreeSpaceDone = new ManualResetEvent(false))
        {
            const string url = "http://localhost:8080";
            const string expectedName = "Grisha";
            var blittable = await GenerateBlittable(context, expectedName);

            httpCache.Set(url, changeVector: null, blittable);

            httpCache.ForTestingPurposesOnly().OnHttpCacheNotFoundUpdate = () =>
            {
                httpCacheUpdate.Set();
                httpCacheFreeSpaceDone.WaitOne();
            };

            using (var releaseCacheItem = httpCache.Get(context, url, out _, out var result))
            {
                result.TryGet("Name", out string name);
                Assert.Equal(expectedName, name);

                var setTask = Task.Run(() => httpCache.SetNotFound(url, aggressivelyCached: false));
                httpCacheUpdate.WaitOne();
                httpCache.FreeSpace();
                httpCacheFreeSpaceDone.Set();

                await setTask;

                Assert.Equal(1, releaseCacheItem.Item.Usages);
                Assert.NotNull(releaseCacheItem.Item.Allocation);

                blittable = await GenerateBlittable(context, "Kotler");
                httpCache.Set(url, changeVector: null, blittable);

                result.TryGet("Name", out name);
                Assert.Equal(expectedName, name);
            }
        }
    }

    private static async Task<BlittableJsonReaderObject> GenerateBlittable(JsonOperationContext context, string name)
    {
        await using (var ms = new MemoryStream())
        await using (var writer = new AsyncBlittableJsonTextWriter(context, ms))
        {
            writer.WriteStartObject();
            writer.WritePropertyName("Name");
            writer.WriteString(name);
            writer.WriteEndObject();
            await writer.FlushAsync();
            await ms.FlushAsync();

            ms.Position = 0;
            return await context.ReadForDiskAsync(ms, "test");
        }
    }
}
