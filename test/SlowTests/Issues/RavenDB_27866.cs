using System;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Fixed;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27866(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public void RepurposedInstance_WritesHeaderOfCurrentTree()
    {
        using (var tx = Env.WriteTransaction())
        {
            var parent = tx.CreateTree("map-phase");
            foreach (var name in new[] { "a", "b" })
            {
                Slice.From(Allocator, name, out Slice slice);
                var fst = new FixedSizeTree(tx.LowLevelTransaction, parent, slice, valSize: sizeof(long));
                for (long i = 0; i < 100; i++)
                    fst.Add(i, BitConverter.GetBytes(i));
            }

            tx.Commit();
        }

        using (var tx = Env.WriteTransaction())
        {
            var parent = tx.ReadTree("map-phase");
            Slice.From(Allocator, "a", out Slice a);
            Slice.From(Allocator, "b", out Slice b);

            var fst = new FixedSizeTree(tx.LowLevelTransaction, parent, a, valSize: sizeof(long), clone: false);
            fst.Add(100, BitConverter.GetBytes(100L));

            FixedSizeTree.TryRepurposeInstance(fst, b, clone: false);
            fst.Add(100, BitConverter.GetBytes(100L));

            tx.Commit();
        }

        using (var tx = Env.ReadTransaction())
        {
            var parent = tx.ReadTree("map-phase");
            foreach (var name in new[] { "a", "b" })
            {
                Slice.From(Allocator, name, out Slice slice);
                var fst = new FixedSizeTree(tx.LowLevelTransaction, parent, slice, valSize: sizeof(long));
                Assert.Equal(101, fst.NumberOfEntries);
            }
        }
    }
}
