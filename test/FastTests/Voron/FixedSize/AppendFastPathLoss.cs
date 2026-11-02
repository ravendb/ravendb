using System;
using System.Collections.Generic;
using System.Linq;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Fixed;
using Xunit;

namespace FastTests.Voron.FixedSize
{
    // The append fast path caches the rightmost leaf page and the tree's max key on the
    // FixedSizeTree *instance*, so that a strictly-increasing key can be written without a
    // root-to-leaf descent.
    //
    // TryRepurposeInstance re-points one instance at a different tree (the map-reduce index does
    // this per document, walking one instance across every document's map-entries tree). The
    // cached rightmost page and max key belong to the *previous* tree, so they must be dropped
    // when the instance is repurposed - otherwise the next append is judged against another
    // tree's max key and written into another tree's page.
    //
    // Map-reduce entry ids are globally increasing, so the stale max key is almost always lower
    // than the incoming key and the fast path arms on a page that is not part of this tree at all.
    public class AppendFastPathLoss(ITestOutputHelper output) : StorageTest(output)
    {
        [RavenFact(RavenTestCategory.Voron)]
        public void RepurposedInstance_DoesNotCarryAppendCacheAcrossTrees()
        {
            const int treeCount = 40;
            const int perTree = 400;

            var expected = new Dictionary<string, List<long>>();
            long nextId = 1; // globally increasing ids, exactly like map-reduce entry ids

            using (var tx = Env.WriteTransaction())
            {
                var parent = tx.CreateTree("map-phase");

                Slice.From(Allocator, "seed", out Slice seed);
                var fst = new FixedSizeTree(tx.LowLevelTransaction, parent, seed, valSize: sizeof(long), clone: false);

                for (var t = 0; t < treeCount; t++)
                {
                    var name = $"doc/{t}";
                    Slice.From(Allocator, name, out Slice nameSlice);

                    // one instance, walked across every document's tree - the pattern the
                    // map-reduce index uses
                    FixedSizeTree.TryRepurposeInstance(fst, nameSlice, clone: false);

                    var keys = new List<long>();
                    for (var i = 0; i < perTree; i++, nextId++)
                    {
                        fst.Add(nextId, BitConverter.GetBytes(nextId));
                        keys.Add(nextId);
                    }

                    expected[name] = keys;
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var parent = tx.ReadTree("map-phase");

                foreach (var (name, keys) in expected)
                {
                    Slice.From(Allocator, name, out Slice nameSlice);
                    var fst = new FixedSizeTree(tx.LowLevelTransaction, parent, nameSlice, valSize: sizeof(long), clone: false);

                    var actual = new List<long>();
                    using (var it = fst.Iterate())
                    {
                        if (it.Seek(long.MinValue))
                        {
                            do
                            {
                                actual.Add(it.CurrentKey);
                            } while (it.MoveNext());
                        }
                    }

                    if (keys.Count != actual.Count || keys.SequenceEqual(actual) == false)
                    {
                        var missing = keys.Except(actual).Take(5).ToList();
                        var extra = actual.Except(keys).Take(5).ToList();
                        Assert.Fail($"{name}: holds {actual.Count} keys ({fst.NumberOfEntries} per header), expected {keys.Count}. " +
                                    $"Missing: [{string.Join(", ", missing)}]. Unexpected: [{string.Join(", ", extra)}].");
                    }

                    Assert.Equal(keys.Count, fst.NumberOfEntries);
                }
            }
        }

        // Same aliasing hazard without repurposing: two live instances over one tree each keep
        // their own cached rightmost page, so an append through one invalidates the other's cache.
        [RavenFact(RavenTestCategory.Voron)]
        public void TwoInstancesOverOneTree_InterleavedAppends_KeepEveryEntry()
        {
            var expected = new List<long>();

            using (var tx = Env.WriteTransaction())
            {
                var parent = tx.CreateTree("shared");
                Slice.From(Allocator, "entries", out Slice name);

                var a = new FixedSizeTree(tx.LowLevelTransaction, parent, name, valSize: sizeof(long), clone: false);
                var b = new FixedSizeTree(tx.LowLevelTransaction, parent, name, valSize: sizeof(long), clone: false);

                for (long key = 1; key <= 4000; key++)
                {
                    var target = (key % 2 == 0) ? a : b;
                    target.Add(key, BitConverter.GetBytes(key));
                    expected.Add(key);
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var parent = tx.ReadTree("shared");
                Slice.From(Allocator, "entries", out Slice name);
                var fst = new FixedSizeTree(tx.LowLevelTransaction, parent, name, valSize: sizeof(long), clone: false);

                var actual = new List<long>();
                using (var it = fst.Iterate())
                {
                    if (it.Seek(long.MinValue))
                    {
                        do
                        {
                            actual.Add(it.CurrentKey);
                        } while (it.MoveNext());
                    }
                }

                Assert.Equal(expected.Count, fst.NumberOfEntries);
                Assert.Equal(expected, actual);
            }
        }
    }
}
