using System;

namespace Allocations.Benchmark
{
    // The bytes that the current thread allocates. The operations run on the calling thread, so other threads are not counted.
    public static class Measure
    {
        // after warm-up calls, so the JIT and the filling of pools are not counted
        public static double PerOperation(Action operation, int operations = 20_000, int warmup = 1_000)
        {
            for (int i = 0; i < warmup; i++)
                operation();

            return (double)Once(() =>
            {
                for (int i = 0; i < operations; i++)
                    operation();
            }) / operations;
        }

        // a batch of items (documents, orders), after one batch to warm up
        public static double PerItem(Action batch, int itemsPerBatch, int rounds = 3)
        {
            batch();

            return (double)Once(() =>
            {
                for (int i = 0; i < rounds; i++)
                    batch();
            }) / (rounds * itemsPerBatch);
        }

        public static long Once(Action action)
        {
            var before = GC.GetAllocatedBytesForCurrentThread();
            action();
            return GC.GetAllocatedBytesForCurrentThread() - before;
        }

        public static void Report(string name, double bytes, string unit = "op") => Console.WriteLine($"  {name,-52} {bytes,10:F1} bytes/{unit}");
    }
}
