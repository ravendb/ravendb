using System;
using System.Collections.Generic;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace FastTests.Voron.FixedSize
{
    // Regression for the etag-index churn pattern an update-heavy workload produces:
    // every document update deletes its old etag from the fixed size tree and appends a new,
    // strictly larger one. The delete must always find the previously added entry.
    public class EtagChurn(ITestOutputHelper output) : StorageTest(output)
    {
        [RavenFact(RavenTestCategory.Voron)]
        public void DeleteOldAppendNew_ManyTransactions()
        {
            Slice.From(Allocator, "etags", out Slice treeId);
            var rnd = new Random(1337);
            var live = new List<long>();
            long next = 1;

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 8);
                for (int i = 0; i < 200_000; i++)
                {
                    fst.Add(next, new byte[8]);
                    live.Add(next);
                    next++;
                }
                tx.Commit();
            }

            for (int txn = 0; txn < 200; txn++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var fst = tx.FixedTreeFor(treeId, valSize: 8);
                    for (int i = 0; i < 2_000; i++)
                    {
                        var victimIdx = rnd.Next(live.Count);
                        var victim = live[victimIdx];

                        var result = fst.Delete(victim);
                        Assert.True(result.NumberOfEntriesDeleted == 1,
                            $"tx {txn}, op {i}: delete of existing key {victim} removed {result.NumberOfEntriesDeleted} entries");

                        fst.Add(next, new byte[8]);
                        live[victimIdx] = next;
                        next++;
                    }
                    fst.ValidateTree_Forced();
                    tx.Commit();
                }
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 8);
                Assert.Equal(200_000, fst.NumberOfEntries);
                live.Sort();
                using (var it = fst.Iterate())
                {
                    Assert.True(it.Seek(long.MinValue));
                    foreach (var expected in live)
                    {
                        Assert.Equal(expected, it.CurrentKey);
                        it.MoveNext();
                    }
                }
            }
        }

        // The raw-data-section defrag path re-inserts an EXISTING (interior, non-maximum) key:
        // delete etag X, re-add etag X with the new row id. A fresh tree instance whose first
        // add is such an interior key must not treat "bigger than anything I have seen" as
        // "global maximum": arming the append fast-path on that interior leaf strands every
        // subsequent (genuinely maximal) append in an unreachable page.
        [RavenFact(RavenTestCategory.Voron)]
        public void ReAddingAnInteriorKeyMustNotHijackTheAppendFastPath()
        {
            Slice.From(Allocator, "etags2", out Slice treeId);
            long next = 1;

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 8);
                for (int i = 0; i < 10_000; i++, next += 2)
                    fst.Add(next, new byte[8]);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                // fresh instance: its first add is an interior insert (the defrag re-add pattern,
                // after the original slot was physically compacted away)
                var fst = tx.FixedTreeFor(treeId, valSize: 8);
                fst.Add(1000, new byte[8]); // between existing keys 999 and 1001, deep inside the tree

                for (int i = 0; i < 64; i++) // genuinely new maxima must remain findable
                {
                    var key = next;
                    next += 2;
                    fst.Add(key, new byte[8]);
                    Assert.True(fst.Contains(key), $"key {key} vanished right after its append");
                }
                fst.ValidateTree_Forced();
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 8);
                Assert.Equal(10_065, fst.NumberOfEntries);
            }
        }
    }
}
