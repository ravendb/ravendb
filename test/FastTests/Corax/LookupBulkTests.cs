using System;
using System.Collections.Generic;
using System.Linq;
using FastTests.Voron;
using FastTests.Voron.FixedSize;
using Voron.Data.Lookups;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class LookupBulkTests : StorageTest
{
    public LookupBulkTests(ITestOutputHelper output) : base(output)
    {
    }
    [Theory]
    [InlineData(214)]
    [InlineDataWithRandomSeed]
    public void CanBulkAddToEmptyLookup(int seed)
    {
        var random = new Random(seed);
        var items = Enumerable.Range(0, 1024)
            .Select(_ => new Int64LookupKey(random.NextInt64()))
            .OrderBy(x=>x.Value).ToArray();

        var values = new long[items.Length];
        var offsets = new int[items.Length];

        using (var wtx = Env.WriteTransaction())
        {
            var lookup = wtx.LookupFor<Int64LookupKey>("test");
            lookup.InitializeCursorState();
            
            int wrote = 0;
            while (wrote < items.Length)
            {
                var curItems = items[wrote..];
                var curValues = values[wrote..];
                var curOffsets = offsets[wrote..];
                var read = lookup.BulkUpdateStart(curItems, curValues, curOffsets, out var pg);

                Assert.Equal(curItems.Length, read);
                Assert.NotEqual(0, pg);

                var changed = lookup.CheckTreeStructureChanges();
                int adjustment = 0;
                for (int i = 0; i < read && changed.Changed == false; i++)
                {
                    lookup.BulkUpdateSet(ref curItems[i], 1 + wrote, pg, curOffsets[i], ref adjustment);
                    wrote++;
                }
            }
            
            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var lookup = rtx.LookupFor<Int64LookupKey>("test");
            var it = lookup.Iterate();
            it.Reset();
            for (int i = 0; i < items.Length; i++)
            {
                Assert.True(it.MoveNext(out Int64LookupKey k, out var v, out _));
                Assert.Equal(items[i].Value, k.Value);
                Assert.Equal(i+1, v);
            }
            Assert.False(it.MoveNext(out Int64LookupKey _, out _, out _));

        }
    }
    
    [Theory]
    [InlineData(214)]
    [InlineDataWithRandomSeed]
    public void CanBulkUpdateAndInsertAndRemove(int seed)
    {
        int count = 0;
        var random = new Random(214);
        var firstItems = Enumerable.Range(0, 1024)
            .Select(_ => new Int64LookupKey(random.NextInt64()))
            .ToArray();

        using (var wtx = Env.WriteTransaction())
        {
            var lookup = wtx.LookupFor<Int64LookupKey>("test");

            for (int i = 0; i < firstItems.Length; i++)
            {
                lookup.Add(ref firstItems[i],  (firstItems[i].Value % 601) + 1000);
            }
            
            wtx.Commit();
        }


        var secondItems = Enumerable.Range(0, 512)
            .Select(_ => new Int64LookupKey(random.NextInt64()))
            .ToArray();
        var updatesAndInserts = secondItems
            .Concat(firstItems.Where((_, i) => i % 2 == 0)) // update half of them
            .OrderBy(x => x.Value)
            .ToArray();
        
        var values = new long[updatesAndInserts.Length];
        var offsets = new int[updatesAndInserts.Length];
        var removed = new HashSet<long>();
        using (var wtx = Env.WriteTransaction())
        {
            var lookup = wtx.LookupFor<Int64LookupKey>("test");
            
            lookup.InitializeCursorState();
            
            int wrote = 0;
            while (wrote < updatesAndInserts.Length)
            {
                var curItems = updatesAndInserts[wrote..];
                var curValues = values[wrote..];
                var curOffsets = offsets[wrote..];
                var read = lookup.BulkUpdateStart(curItems, curValues, curOffsets, out var pg);

                var changed = lookup.CheckTreeStructureChanges();
                int adjustment = 0;
                for (int i = 0; i < read && changed.Changed == false; i++)
                {
                    if (wrote % 15 == 0)
                    {
                        lookup.BulkUpdateRemove(ref curItems[i], pg, curOffsets[i], ref adjustment);
                        removed.Add(curItems[i].Value);
                    }
                    else
                    {
                        lookup.BulkUpdateSet(ref curItems[i], (curItems[i].Value % 317), pg, curOffsets[i], ref adjustment);
                    }
                    wrote++;
                }
            }
            
            wtx.Commit();
        }

        var allItems = firstItems.Concat(secondItems).OrderBy(x => x.Value).ToArray();
        using (var rtx = Env.ReadTransaction())
        {
            var lookup = rtx.LookupFor<Int64LookupKey>("test");
            var it = lookup.Iterate();
            it.Reset();
            for (int i = 0; i < allItems.Length; i++)
            {
                if (removed.Contains(allItems[i].Value))
                {
                    continue; // was removed...
                }
                Assert.True(it.MoveNext(out Int64LookupKey k, out var v, out _));
                Assert.Equal(allItems[i].Value, k.Value);
                Assert.True(allItems[i].Value % 317 == v || v == (allItems[i].Value % 601) + 1000);
            }
            Assert.False(it.MoveNext(out Int64LookupKey _, out _, out _));
        }
    }
}
