using System;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace FastTests.Voron.FixedSize
{
    public class FixedSizeTreeReadAllocations(ITestOutputHelper output) : StorageTest(output)
    {
        [RavenFact(RavenTestCategory.Voron)]
        public unsafe void ReadingAKeyOfALargeTreeDoesNotAllocateManagedMemory()
        {
            // a tree of two levels: a branch page over a number of leaves
            const int entries = 20_000;
            const int valueSize = 16;

            Slice.From(Allocator, "points", out var treeId);
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valueSize);
                var value = new byte[valueSize];
                for (var i = 0; i < entries; i++)
                    fst.Add(i * 3L, value);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valueSize);
                var random = new Random(1);
                var keys = new long[20_000];
                for (var i = 0; i < keys.Length; i++)
                    keys[i] = random.Next(entries) * 3L;

                // warm up (jit, caches)
                foreach (var key in keys)
                    fst.ReadPtr(key, out _);

                // the assertions allocate, so none runs inside the measured loops
                var missing = 0;
                var before = GC.GetAllocatedBytesForCurrentThread();
                foreach (var key in keys)
                {
                    if (fst.ReadPtr(key, out var size) == null || size != valueSize)
                        missing++;
                }

                var perReadPtr = (GC.GetAllocatedBytesForCurrentThread() - before) / (double)keys.Length;

                before = GC.GetAllocatedBytesForCurrentThread();
                foreach (var key in keys)
                {
                    using (fst.Read(key, out var slice))
                    {
                        if (slice.HasValue == false)
                            missing++;
                    }
                }

                var perRead = (GC.GetAllocatedBytesForCurrentThread() - before) / (double)keys.Length;
                Assert.Equal(0, missing);

                Output.WriteLine($"managed bytes per ReadPtr: {perReadPtr:F1}, per Read: {perRead:F1}");
                Assert.True(perReadPtr < 1, $"ReadPtr allocated {perReadPtr:F1} bytes per call");
                Assert.True(perRead < 1, $"Read allocated {perRead:F1} bytes per call");
            }
        }
    }
}
