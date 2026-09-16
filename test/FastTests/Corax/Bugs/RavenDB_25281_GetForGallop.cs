using System;
using System.Collections.Generic;
using System.Linq;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron.Data.Lookups;
using Xunit;

namespace FastTests.Corax.Bugs;

public class RavenDB_25281_GetForGallop : StorageTest
{
    public RavenDB_25281_GetForGallop(ITestOutputHelper output) : base(output)
    {
    }

    // GetFor walks the lookup once for a sorted batch of keys, galloping forward from the previous
    // position within each page. This differential test asserts the gallop returns exactly the same
    // value (or the missing sentinel) as a ground-truth dictionary, across multi-page trees and the
    // dense / sparse / boundary key distributions the gallop branches on.
    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(214)]
    [InlineData(1337)]
    [InlineDataWithRandomSeed]
    public void GetForMatchesGroundTruthAcrossPages(int seed)
    {
        const long missing = long.MinValue;
        var random = new Random(seed);

        // Enough distinct keys to span many leaf pages so SearchNextPage and the in-page gallop both run.
        var keys = new HashSet<long>();
        while (keys.Count < 20_000)
            keys.Add(random.NextInt64(0, 1_000_000));

        var reference = new Dictionary<long, long>();
        var sortedKeys = keys.OrderBy(x => x).ToArray();

        using (var wtx = Env.WriteTransaction())
        {
            var lookup = wtx.LookupFor<Int64LookupKey>("test");
            foreach (var k in sortedKeys)
            {
                long value = (k * 2) + 1; // any deterministic, recoverable value
                var key = new Int64LookupKey(k);
                lookup.Add(ref key, value);
                reference[k] = value;
            }
            wtx.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var lookup = rtx.LookupFor<Int64LookupKey>("test");

            // 1. All keys present, fully dense merge (every entry is wanted).
            AssertGetFor(lookup, reference, sortedKeys, missing);

            // 2. Sparse random subset of present keys.
            var sparse = sortedKeys.Where(_ => random.Next(20) == 0).ToArray();
            AssertGetFor(lookup, reference, sparse, missing);

            // 3. A dense contiguous run from the middle of the key space.
            var run = sortedKeys.Skip(sortedKeys.Length / 3).Take(500).ToArray();
            AssertGetFor(lookup, reference, run, missing);

            // 4. Mix of present and absent keys (forces the "should be here but isn't" path).
            var mixed = new SortedSet<long>(sparse);
            for (int i = 0; i < 2000; i++)
                mixed.Add(random.NextInt64(0, 1_000_000)); // some land between/at existing keys, some absent
            AssertGetFor(lookup, reference, mixed.ToArray(), missing);

            // 5. Keys entirely below, between, and above the stored range (boundary / next-page exhaustion).
            long min = sortedKeys[0], max = sortedKeys[^1];
            var boundary = new long[] { min - 100, min - 1, min, max, max + 1, max + 100 }
                .Distinct().OrderBy(x => x).ToArray();
            AssertGetFor(lookup, reference, boundary, missing);

            // 6. Single-key batches at the extremes.
            AssertGetFor(lookup, reference, new[] { min }, missing);
            AssertGetFor(lookup, reference, new[] { max }, missing);
            AssertGetFor(lookup, reference, new[] { max + 1 }, missing);
        }
    }

    private static void AssertGetFor(Lookup<Int64LookupKey> lookup, Dictionary<long, long> reference, long[] queryKeys, long missing)
    {
        if (queryKeys.Length == 0)
            return;

        var terms = new long[queryKeys.Length];
        lookup.GetFor(queryKeys, terms, missing);

        for (int i = 0; i < queryKeys.Length; i++)
        {
            long expected = reference.TryGetValue(queryKeys[i], out var v) ? v : missing;
            Assert.Equal(expected, terms[i]);
        }
    }
}
