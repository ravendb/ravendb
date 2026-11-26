using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Sparrow;
using Tests.Infrastructure;
using Voron.Impl.Paging;
using Xunit;
using NativeMemory = Sparrow.Utils.NativeMemory;

namespace SlowTests.Voron.Issues;

// Growing a scratch file maps the whole file again. Once no transaction uses the smaller mappings, only the current one
// may stay: each extra mapping counts the same resident pages again in the working set and in the clean memory.
public class RavenDB_23116(ITestOutputHelper output) : RavenTestBase(output)
{
    private class Item
    {
        public string Name { get; set; }
    }

    private class Items_ByName : AbstractIndexCreationTask<Item>
    {
        public Items_ByName()
        {
            Map = items => from item in items select new { item.Name };
        }
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Indexes)]
    public async Task GrownIndexScratchFileKeepsOnlyItsCurrentMapping()
    {
        Assert.SkipWhen(RavenTestHelper.EnvironmentVariables.IsRunningOnCI,
            "Forces full GCs and waits for finalizers, which stalls the other tests running in the same process");

        using var store = GetDocumentStore(new Options
        {
            // a small index is flushed in the background at most once per this interval, 30 s by default
            ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Storage.TimeToSyncAfterFlush)] = "1"
        });
        var index = new Items_ByName();
        await index.ExecuteAsync(store);

        using (var bulk = store.BulkInsert())
        {
            for (var i = 0; i < 200; i++)
                await bulk.StoreAsync(new Item { Name = $"item-{i}-{new string('x', 200)}" });
        }

        Indexes.WaitForIndexing(store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var env = database.IndexStore.GetIndex(index.IndexName)._environment;

        var scratchFile = env.ScratchBufferPool._current.File;
        var path = scratchFile.Pager.FileName;

        // the idle index gets flushed in the background, after that no transaction needs the smaller mappings anymore
        WaitForValue(() =>
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            Pager.State.DrainPendingDisposal();
            return MappingSizes(path).Length;
        }, expectedVal: 1, timeout: 30_000, interval: 500);

        var sizes = MappingSizes(path);
        Assert.True(sizes.Length == 1,
            $"{Path.GetFileName(path)} ({new Size(scratchFile.Size, SizeUnit.Bytes)}, {scratchFile.AllocatedPagesCount} pages still allocated) " +
            $"has {sizes.Length} live mappings, expected 1. Mapped {new Size(sizes.Sum(), SizeUnit.Bytes)} in total: " +
            string.Join(", ", sizes.Select(s => new Size(s, SizeUnit.Bytes))));
    }

    private static long[] MappingSizes(string path) =>
        NativeMemory.FileMapping.TryGetValue(path, out var mapping)
            ? mapping.Value.Info.Values.Order().ToArray()
            : [];
}
