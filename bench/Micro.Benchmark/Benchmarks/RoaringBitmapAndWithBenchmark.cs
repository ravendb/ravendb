using System;
using System.Collections.Generic;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;
using Sparrow.Server;
using Sparrow.Threading;
using Voron.Data.RoaringBitmaps;

namespace Micro.Benchmark.Benchmarks
{
    /// <summary>
    /// Compares RoaringBitmap.AndWith's "sort buffer to entry-ID order, walk container groups, restore order"
    /// design against a plain order-preserving per-element Contains loop, on the SortingMatch.StreamInIndexOrder
    /// hot path. The sort+walk wins when many buffer entries fall in the same dense container; the per-element
    /// loop avoids the sort and index array. The crossover depends on buffer ordering, candidate container shape,
    /// and selectivity. Both benchmarks copy the pristine buffer into a working span first, so the copy is charged equally.
    /// </summary>
    [SimpleJob(RunStrategy.Throughput, RuntimeMoniker.Net10_0, warmupCount: 3, iterationCount: 6)]
    public unsafe class RoaringBitmapAndWithBenchmark
    {
        // Realistic StreamInIndexOrder batch is SortBatchSize = 8192; 1024 models a smaller take/early stop.
        [Params(1024, 8192)]
        public int Count;

        // StreamInIndexOrder feeds entries in SORT-FIELD order (scattered w.r.t. entry ID). "Sorted" is the
        // best case for the sort+walk (sort is near free); "Shuffled" is the realistic streaming case.
        [Params(true, false)]
        public bool Shuffled;

        // Candidate-set container shape. Bitmap = dense (>4096/64K range); Array = sparse sorted; Range = contiguous.
        [Params(Shape.Bitmap, Shape.Array, Shape.Range)]
        public Shape CandidateShape;

        // Fraction of buffer entries present in the candidate set (== kept/count). Low selectivity is the
        // common WHERE-is-selective case; high selectivity is a broad filter.
        [Params(0.05, 0.5, 0.9)]
        public double Selectivity;

        public enum Shape { Bitmap, Array, Range }

        private ByteStringContext _ctx;
        private RoaringBitmap _candidate;
        private long[] _sourceBuffer;   // pristine; never mutated
        private long[] _workingBuffer;  // scratch the methods filter in place

        [GlobalSetup]
        public void Setup()
        {
            _ctx = new ByteStringContext(SharedMultipleUseFlag.None);

            // Build the candidate set (the bitmap that AndWith filters against). We pick a member universe
            // big enough to realise the requested container shape, then draw buffer members/non-members from it.
            long[] members;      // values present in the candidate set
            long nonMemberBase;  // values >= here (and not in members) are guaranteed absent
            switch (CandidateShape)
            {
                case Shape.Range:
                    // One contiguous Range container: [0, 50000).
                    members = Build(0, 50_000, 1);
                    _candidate = FromSortedValues(members);
                    nonMemberBase = 50_000;
                    break;

                case Shape.Array:
                    // Sparse sorted: every 32nd value across 3 64K ranges => 2048/container (<4096) => Array.
                    members = Build(0, 3L * 65_536, 32);
                    _candidate = FromSortedValues(members);
                    nonMemberBase = 3L * 65_536; // outside the populated ranges
                    break;

                default: // Bitmap
                    // Dense but NOT fully contiguous (a single gap defeats the Range fast-path), >4096
                    // values in one 64K range => Bitmap container. ~60000 of 65536 slots set.
                    members = BuildDenseWithGap(0, 60_000);
                    _candidate = FromSortedValues(members);
                    nonMemberBase = 65_536; // next container key, empty => absent
                    break;
            }
            _candidate.PrepareForReading();

            int memberTarget = (int)(Count * Selectivity);
            var rnd = new Random(73101);
            var buffer = new long[Count];

            // Members: sample from the candidate value set.
            for (int i = 0; i < memberTarget; i++)
                buffer[i] = members[rnd.Next(members.Length)];

            // Non-members: values guaranteed absent from the candidate set.
            for (int i = memberTarget; i < Count; i++)
                buffer[i] = nonMemberBase + rnd.Next(0, 1_000_000);

            if (Shuffled)
            {
                // Fisher-Yates: scatter members/non-members and break entry-ID ordering (sort-field order).
                for (int i = Count - 1; i > 0; i--)
                {
                    int j = rnd.Next(i + 1);
                    (buffer[i], buffer[j]) = (buffer[j], buffer[i]);
                }
            }
            else
            {
                Array.Sort(buffer); // entry-ID order: best case for sort+walk.
            }

            _sourceBuffer = buffer;
            _workingBuffer = new long[Count];
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _candidate.Dispose();
            _ctx.Dispose();
        }

        // Current design: sort buffer to entry-ID order (carrying indices), walk container groups, restore order.
        [Benchmark(Baseline = true)]
        public int SortWalk()
        {
            _sourceBuffer.AsSpan().CopyTo(_workingBuffer);
            return _candidate.AndWith(_workingBuffer.AsSpan(), Count);
        }

        // Alternative: order-preserving per-element Contains loop. No sort, no index array.
        [Benchmark]
        public int PerElement()
        {
            _sourceBuffer.AsSpan().CopyTo(_workingBuffer);
            var span = _workingBuffer.AsSpan(0, Count);
            int kept = 0;
            for (int i = 0; i < span.Length; i++)
            {
                if (_candidate.Contains(span[i]))
                    span[kept++] = span[i];
            }
            return kept;
        }

        private static long[] Build(long start, long endExclusive, int step)
        {
            var list = new List<long>();
            for (long v = start; v < endExclusive; v += step)
                list.Add(v);
            return list.ToArray();
        }

        // Dense [start, start+count) with a single hole, so AddRange's contiguous-run check fails and the
        // container is materialised as a Bitmap rather than a Range. count must exceed 4096 (>1 64K range
        // cardinality threshold) for the Bitmap path.
        private static long[] BuildDenseWithGap(long start, int count)
        {
            var list = new List<long>(count);
            long mid = start + count / 2;
            for (long v = start; v < start + count + 1; v++)
            {
                if (v == mid)
                    continue; // the gap
                list.Add(v);
            }
            return list.ToArray();
        }

        // Build via AddRange (not per-value Add) so container types match intent: contiguous => Range,
        // sparse <=4096/range => Array, dense-with-gap >4096 => Bitmap.
        private RoaringBitmap FromSortedValues(long[] sorted)
        {
            var bitmap = new RoaringBitmap(_ctx);
            bitmap.AddRange(sorted);
            return bitmap;
        }
    }
}
