using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FastTests.Voron;
using FastTests.Voron.FixedSize;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_26746 : StorageTest
    {
        public RavenDB_26746(ITestOutputHelper output) : base(output)
        {
        }

        // Reproduces structural tree corruption caused by rebalancing with a stale cursor.
        // During a delete, the rebalance cascade re-adds separators of moved pages into their parents.
        // With large keys a parent can overflow, so the re-add splits it (possibly recursively), moving
        // pages under different parents and changing node positions. Later iterations of the cascade
        // used the stale descent-time positions and could unlink a wrong subtree (lost entries),
        // reference the same page from two branches and insert out-of-order separators, leaving keys
        // visible to iteration but unreachable for searches (deletes silently no-op).
        // The workload mimics the original scenario: tombstone-like keys (short ids mixed with ~1.5 KB
        // change-vector suffixes, so branch pages hold only a few nodes), waves of sorted inserts
        // followed by mass deletes.
        // Fixed-seed variants run in StressTests.Voron.RavenDB_26746.
        // Here we keep a single random-seed case so every PR run fuzzes it.
        [RavenTheory(RavenTestCategory.Voron)]
        [InlineDataWithRandomSeed(-1)] // maxCycles -1 -> pick a random 15..20 below
        public void Rebalancing_must_not_corrupt_tree_when_separator_readd_splits_ancestors(int maxCycles, int seed)
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tombstones");
                tx.Commit();
            }

            var rng = new Random(seed);
            if (maxCycles < 0)
                maxCycles = rng.Next(15, 21); // 15..20, derived from the seed so a failure stays reproducible
            var generations = new Dictionary<long, int>();
            var live = new HashSet<string>(StringComparer.Ordinal);

            for (int cycle = 0; cycle < maxCycles; cycle++)
            {
                int toInsert = 15_000 + rng.Next(15_000);
                var insertBatch = new List<string>(toInsert);
                for (int i = 0; i < toInsert; i++)
                {
                    long docId = 8_000_000 + rng.Next(120_000);
                    bool docTombstoneStyle = rng.Next(5) == 0; // bare key, reused and re-put across cycles
                    string key;
                    if (docTombstoneStyle)
                    {
                        key = $"tombstones/{docId}";
                    }
                    else
                    {
                        generations.TryGetValue(docId, out int gen);
                        generations[docId] = gen + 1;
                        key = MakeChangeVectorLikeKey(docId, gen, seed);
                    }

                    if (live.Add(key) || docTombstoneStyle)
                        insertBatch.Add(key);
                }

                // ascending order packs pages full through the sequential-insert split path,
                // so later rebalances meet full parents
                insertBatch.Sort(StringComparer.Ordinal);
                ApplyBatches(insertBatch, isInsert: true, rng);

                var liveList = live.ToList();
                liveList.Sort(StringComparer.Ordinal);

                int toDelete = (int)(liveList.Count * (0.3 + rng.NextDouble() * 0.6));
                var deleteBatch = new List<string>(toDelete);

                if (rng.Next(4) == 0 && liveList.Count > 1000)
                {
                    int start = rng.Next(liveList.Count / 2);
                    int len = Math.Min(toDelete, liveList.Count - start);
                    for (int i = start; i < start + len; i++)
                        deleteBatch.Add(liveList[i]);
                }
                else
                {
                    foreach (string key in liveList.OrderBy(_ => rng.Next()).Take(toDelete))
                        deleteBatch.Add(key);

                    if (seed % 3 == 0)
                        deleteBatch.Sort(StringComparer.Ordinal);
                }

                foreach (string key in deleteBatch)
                    live.Remove(key);

                ApplyBatches(deleteBatch, isInsert: false, rng);

                AssertTreeMatchesShadowSet(live, cycle);
            }
        }

        private void ApplyBatches(List<string> keys, bool isInsert, Random rng)
        {
            int pos = 0;
            while (pos < keys.Count)
            {
                int batchSize = 100 + rng.Next(400);
                using (var tx = Env.WriteTransaction())
                {
                    Tree tree = tx.ReadTree("tombstones");
                    for (int i = 0; i < batchSize && pos < keys.Count; i++, pos++)
                    {
                        using (Slice.From(tx.Allocator, keys[pos], ByteStringType.Immutable, out Slice keySlice))
                        {
                            if (isInsert)
                            {
                                using (Slice.From(tx.Allocator, BitConverter.GetBytes((long)pos), ByteStringType.Immutable, out Slice value))
                                    tree.Add(keySlice, value);
                            }
                            else
                            {
                                tree.Delete(keySlice);
                            }
                        }
                    }

                    tx.Commit();
                }
            }
        }

        private void AssertTreeMatchesShadowSet(HashSet<string> live, int cycle)
        {
            using (var tx = Env.ReadTransaction())
            {
                Tree tree = tx.ReadTree("tombstones");

                Assert.True(live.Count == tree.State.Header.NumberOfEntries,
                    $"cycle {cycle}: tree header says {tree.State.Header.NumberOfEntries} entries but {live.Count} keys should be live (deletes silently missed keys or subtrees were lost)");

                var seen = new HashSet<string>(StringComparer.Ordinal);
                using (var it = tree.Iterate(prefetch: false))
                {
                    if (it.Seek(Slices.BeforeAllKeys))
                    {
                        do
                        {
                            string key = it.CurrentKey.ToString();
                            Assert.True(seen.Add(key), $"cycle {cycle}: key '{key}' was iterated twice (duplicate page reference)");
                        } while (it.MoveNext());
                    }
                }

                Assert.True(seen.Count == live.Count,
                    $"cycle {cycle}: iteration found {seen.Count} entries, expected {live.Count}");

                foreach (string key in live)
                {
                    Assert.True(seen.Contains(key), $"cycle {cycle}: live key '{key}' was not seen by iteration (lost subtree)");

                    using (Slice.From(tx.Allocator, key, ByteStringType.Immutable, out Slice keySlice))
                    {
                        Assert.True(tree.Read(keySlice) != null,
                            $"cycle {cycle}: live key '{key}' is not reachable by search (misrouted by out-of-order separators) although iteration {(seen.Contains(key) ? "sees" : "does not see")} it");
                    }
                }
            }
        }

        private static string MakeChangeVectorLikeKey(long docId, int generation, int seed)
        {
            // deterministic per (seed, docId, generation), ~1.2-1.8 KB so a branch page holds only
            // a few nodes and separator re-adds regularly overflow the parents
            int stableSeed = unchecked((int)((seed * 1000003L) ^ (docId * 31L) ^ (generation * 514229L)));
            var keyRng = new Random(stableSeed);
            var sb = new StringBuilder(2000);
            sb.Append("tombstones/").Append(docId);
            sb.Append('\x1e');
            int segments = 28 + keyRng.Next(12);
            const string b64Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
            for (int i = 0; i < segments; i++)
            {
                if (i > 0)
                    sb.Append(", ");
                sb.Append((char)('A' + keyRng.Next(4))).Append(':');
                sb.Append(1_000_000 + keyRng.Next(2_000_000_000));
                sb.Append('-');
                for (int j = 0; j < 22; j++)
                    sb.Append(b64Chars[keyRng.Next(64)]);
            }

            sb.Append("==gen:").Append(generation);
            return sb.ToString();
        }
    }
}
