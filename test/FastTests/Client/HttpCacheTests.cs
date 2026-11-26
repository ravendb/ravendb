using System;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Http;
using Sparrow;
using Sparrow.Json;
using Xunit;
using Tests.Infrastructure;
using Xunit.Abstractions;
using Size = Sparrow.Size;

namespace FastTests.Client;

public class HttpCacheTests : RavenTestBase
{
    public HttpCacheTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.ClientApi)]
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

    [RavenFact(RavenTestCategory.ClientApi)]
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

    [RavenFact(RavenTestCategory.ClientApi)]
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

    // RavenDB-28147: HttpCache.Set adds the size of the pooled allocation (the response size rounded up to a power of two)
    // to the tracked total, so releasing the item must subtract that same size. Otherwise replacing the same URL
    // grows the tracked total, although the cache holds a single item.
    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task Replacing_The_Same_Url_Should_Not_Grow_The_Tracked_Cache_Size()
    {
        using (var httpCache = new HttpCache(maxSize: 1024 * 1024))
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            // Keep the background cleanup out of the measurement.
            httpCache.ForTestingPurposesOnly().DisableFreeSpaceCleanup = true;

            var blittable = await GenerateBlittable(context, RandomString(40_000));
            var pooledSize = (long)BitOperations.RoundUpToPowerOf2((uint)blittable.Size);
            Assert.True(pooledSize - blittable.Size >= 8 * 1024,
                $"Test data must not be close to a power of two: {blittable.Size:N0} bytes, pooled as {pooledSize:N0}.");

            for (var i = 0; i < 100; i++)
                httpCache.Set("/docs?&id=items%2F1", changeVector: $"A:{i + 1}-dbid", blittable);

            Assert.Equal(1, httpCache.NumberOfItems);

            // The cache holds one item, so its tracked size cannot exceed one pooled allocation.
            var trackedSize = GetTrackedSize(httpCache);
            Assert.True(trackedSize <= pooledSize,
                $"Tracked size is {trackedSize:N0} bytes while the cache holds one item of {blittable.Size:N0} bytes (pooled as {pooledSize:N0}).");
        }
    }

    // RavenDB-28147: once the tracked total passed MaxHttpCacheSize, every new cache entry started a FreeSpace run whose
    // target (half the inflated total) was larger than everything the cache really held, so it evicted every item.
    // Two queries that alternate against an unchanged database then kept evicting each other and never got a 304.
    [RavenFact(RavenTestCategory.ClientApi)]
    public void Unchanged_Queries_Should_Be_Served_From_Cache_After_One_Document_Was_Updated_And_Reloaded_Many_Times()
    {
        using var store = GetDocumentStore(new Options
        {
            ModifyDocumentStore = s => s.Conventions.MaxHttpCacheSize = new Size(1, SizeUnit.Megabytes)
        });

        using (var session = store.OpenSession())
        {
            session.Store(new Item { Payload = RandomString(40_000) }, "items/large");
            session.Store(new Item { Payload = "a" }, "items/a");
            session.Store(new Item { Payload = "b" }, "items/b");
            session.SaveChanges();
        }

        // Update one ~40 KB document and read it back 100 times. Each read replaces the same cache entry,
        // so the cache never holds more than one copy of it: far below the 1 MB limit.
        for (var i = 0; i < 100; i++)
        {
            using (var session = store.OpenSession())
            {
                var item = session.Load<Item>("items/large");
                item.Version = i + 1;
                session.SaveChanges();
            }
        }

        // No writes from here on. After the first round, every query should be answered from the cache (304).
        const int rounds = 20;
        var servedFromCache = 0;
        for (var round = 0; round < rounds; round++)
        {
            foreach (var id in new[] { "items/a", "items/b" })
            {
                using (var session = store.OpenSession())
                {
                    var results = session.Query<Item>()
                        .Statistics(out var stats)
                        .Where(x => x.Id == id)
                        .ToList();

                    Assert.Single(results);

                    if (stats.DurationInMs == -1)
                        servedFromCache++;
                }
            }
        }

        Assert.Equal(2 * (rounds - 1), servedFromCache);
    }

    private static long GetTrackedSize(HttpCache cache)
    {
        var field = typeof(HttpCache).GetField("_totalSize", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        return (long)field.GetValue(cache)!;
    }

    private static string RandomString(int length)
    {
        var random = new Random(12345);
        return string.Create(length, random, (chars, r) =>
        {
            for (var i = 0; i < chars.Length; i++)
                chars[i] = (char)('a' + r.Next(26));
        });
    }

    private class Item
    {
        public string Id { get; set; }
        public string Payload { get; set; }
        public int Version { get; set; }
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

