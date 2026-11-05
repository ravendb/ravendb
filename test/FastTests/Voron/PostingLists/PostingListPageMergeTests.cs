using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using Sparrow.Utils;
using Tests.Infrastructure;
using Voron;
using Voron.Data;
using Voron.Data.PostingLists;
using Voron.Impl;
using Voron.Util;
using Voron.Util.PFor;
using Xunit;

namespace FastTests.Voron.PostingLists
{
    public class PostingListPageMergeTests(ITestOutputHelper output) : StorageTest(output)
    {
        public enum Shape
        {
            Dense,
            Sparse,
            SharedLowBits,
            Regions
        }

        private const int RegionSize = 150_000;

        private static long Id(Shape shape, Random random, long index)
        {
            switch (shape)
            {
                case Shape.Dense:
                    return index << 1;
                case Shape.Sparse:
                    return random.NextInt64(1, 1L << 40) & ~1L;
                case Shape.Regions:
                    // regions share their low bits, so their pages pack with a shared prefix
                    return index << 10 | (index / RegionSize % 500) << 1;
                default:
                    // a region of ids shares its low bits
                    long region = index / 3000;
                    return index << 10 | (region * 37 % 512) << 1;
            }
        }

        private static unsafe PostingListState Apply(LowLevelTransaction llt, PostingListState state, long[] additions, long[] removals)
        {
            Array.Sort(additions);
            long[] removalMarks = removals.Select(x => x | 1).OrderBy(x => x).ToArray();

            var encoder = new FastPForEncoder(llt.Allocator);
            var decoder = new FastPForDecoder(llt.Allocator);
            var tempList = new ContextBoundNativeList<long>(llt.Allocator);
            try
            {
                if (state.RootObjectType != RootObjectType.Set)
                    PostingList.Create(llt, ref state);

                fixed (long* a = additions)
                fixed (long* r = removalMarks)
                {
                    PostingList.Update(llt, ref state, a, additions.Length, r, removalMarks.Length, encoder, ref tempList, ref decoder);
                }
            }
            finally
            {
                tempList.Dispose();
                decoder.Dispose();
                encoder.Dispose();
            }

            return state;
        }

        private static List<long> Values(LowLevelTransaction llt, PostingListState state)
        {
            using var list = new PostingList(llt, Slices.Empty, state);
            return list.DumpAllValues();
        }

        // pages are named by position in the tree, not by page number
        private static unsafe byte[] StructureOf(LowLevelTransaction llt, PostingListState state)
        {
            using var list = new PostingList(llt, Slices.Empty, state);
            using var sha = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            sha.AppendData(BitConverter.GetBytes(state.Depth));
            sha.AppendData(BitConverter.GetBytes(state.BranchPages));
            sha.AppendData(BitConverter.GetBytes(state.LeafPages));
            sha.AppendData(BitConverter.GetBytes(state.NumberOfEntries));

            List<long> pages = list.AllPages();
            var positions = new Dictionary<long, int>();
            foreach (long pageNumber in pages)
                positions[pageNumber] = positions.Count;

            foreach (long pageNumber in pages)
            {
                Page page = llt.GetPage(pageNumber);
                var header = (PostingListLeafPageHeader*)page.Pointer;
                if (header->PageType == ExtendedPageType.PostingListLeaf)
                {
                    // bytes behind SizeUsed are not compared
                    AppendHeader(sha, page);
                    sha.AppendData(new ReadOnlySpan<byte>(page.DataPointer, header->SizeUsed));
                    continue;
                }

                AppendHeader(sha, page);
                foreach ((long key, long child) in new PostingListBranchPage(page).GetDebugOutput())
                {
                    sha.AppendData(BitConverter.GetBytes(key));
                    sha.AppendData(BitConverter.GetBytes(positions[child]));
                }
            }

            return sha.GetHashAndReset();
        }

        // without page number and checksum
        private static unsafe void AppendHeader(IncrementalHash sha, Page page)
        {
            int checksum = PageHeader.ChecksumOffset;
            sha.AppendData(new ReadOnlySpan<byte>(page.Pointer + sizeof(long), checksum - sizeof(long)));
            sha.AppendData(new ReadOnlySpan<byte>(page.Pointer + checksum + sizeof(ulong), PageHeader.SizeOf - checksum - sizeof(ulong)));
        }

        private sealed class Model(Shape shape, int seed, int size)
        {
            private readonly Random _random = new(seed);
            private readonly SortedSet<long> _present = new();
            private long _nextIndex;

            public SortedSet<long> Present => _present;

            private long[] NewIdsFast(int count)
            {
                var set = new HashSet<long>();
                while (set.Count < count)
                {
                    long id = shape == Shape.Sparse ? Id(shape, _random, 0) : Id(shape, _random, _nextIndex++);
                    if (_present.Contains(id) == false)
                        set.Add(id);
                }

                return set.ToArray();
            }

            public (long[] Additions, long[] Removals) NextOperation(int round)
            {
                if (round == 0 || shape == Shape.Regions && round < 4)
                    return (Commit(NewIdsFast(shape == Shape.Regions ? RegionSize : size)), Array.Empty<long>());

                long[] present = _present.ToArray();
                switch (_random.Next(6))
                {
                    case 0: // remove random entries
                        return (Array.Empty<long>(), Remove(present.Where(_ => _random.NextDouble() < 0.1 + _random.NextDouble() * 0.7).ToArray()));
                    case 1: // remove groups of neighbours
                        return (Array.Empty<long>(), Remove(RemoveGroups(present)));
                    case 2: // remove most of a range
                        {
                            int from = _random.Next(Math.Max(1, present.Length));
                            int count = _random.Next(present.Length - from + 1);
                            return (Array.Empty<long>(), Remove(present.Skip(from).Take(count).Where(_ => _random.NextDouble() < 0.97).ToArray()));
                        }
                    case 3: // add
                        return (Commit(NewIdsFast(_random.Next(1, size / 2 + 2))), Array.Empty<long>());
                    case 4: // add and remove in one update
                        {
                            long[] removals = present.Where(_ => _random.NextDouble() < 0.5).ToArray();
                            long[] additions = NewIdsFast(_random.Next(1, size / 4 + 2));
                            Remove(removals);
                            return (Commit(additions), removals);
                        }
                    default: // a few entries
                        return (Commit(NewIdsFast(_random.Next(1, 20))), Remove(present.Where(_ => _random.NextDouble() < 0.001).ToArray()));
                }
            }

            private long[] RemoveGroups(long[] present)
            {
                var removals = new List<long>();
                int i = 0;
                while (i < present.Length)
                {
                    int groupLength = _random.Next(1, Math.Max(3000, size / 4));
                    if (_random.NextDouble() < 0.6)
                        removals.AddRange(present.Skip(i).Take(groupLength));
                    i += groupLength;
                }

                return removals.ToArray();
            }

            private long[] Commit(long[] additions)
            {
                foreach (long id in additions)
                    _present.Add(id);
                return additions;
            }

            private long[] Remove(long[] removals)
            {
                foreach (long id in removals)
                    _present.Remove(id);
                return removals;
            }
        }

        private byte[] RunModel(Shape shape, int seed, int size, int rounds, bool check)
        {
            using var sha = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            var model = new Model(shape, seed, size);
            var state = new PostingListState();
            for (int round = 0; round < rounds; round++)
            {
                var (additions, removals) = model.NextOperation(round);
                using (var wtx = Env.WriteTransaction())
                {
                    state = Apply(wtx.LowLevelTransaction, state, additions, removals);
                    if (check)
                        Assert.Equal(model.Present.Count, state.NumberOfEntries);
                    wtx.Commit();
                }

                using (var rtx = Env.ReadTransaction())
                {
                    if (check)
                        Assert.Equal(model.Present, Values(rtx.LowLevelTransaction, state));
                    sha.AppendData(StructureOf(rtx.LowLevelTransaction, state));
                }
            }

            return sha.GetHashAndReset();
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(Shape.Dense, 1, 100_000)]
        [InlineData(Shape.Dense, 2, 20_000)]
        [InlineData(Shape.Dense, 3, 700)]
        [InlineData(Shape.Sparse, 4, 60_000)]
        [InlineData(Shape.Sparse, 5, 3_000)]
        [InlineData(Shape.Sparse, 6, 40)]
        [InlineData(Shape.SharedLowBits, 7, 80_000)]
        [InlineData(Shape.SharedLowBits, 8, 9_000)]
        [InlineData(Shape.SharedLowBits, 9, 300)]
        [InlineData(Shape.Regions, 10, 150_000)]
        [InlineData(Shape.Regions, 101, 150_000)]
        [InlineData(Shape.Regions, 102, 150_000)]
        public void RandomUpdatesMatchASortedSet(Shape shape, int seed, int size)
        {
            RunModel(shape, seed, size, rounds: 25, check: true);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void PagesWrittenByRandomUpdatesAreByteIdenticalToTheKnownOnes()
        {
            // Checksums of the tree after each round of updates, from the code before the buffers were shared.
            var actual = new List<string>();
            foreach (var (shape, seed, size) in GoldenCases)
                actual.Add(Convert.ToHexString(RunModel(shape, seed, size, rounds: 25, check: false)));

            output.WriteLine(string.Join(Environment.NewLine, actual.Select(x => $"\"{x}\",")));
            Assert.Equal(ExpectedChecksums, actual);
        }

        private static readonly (Shape, int, int)[] GoldenCases =
        [
            (Shape.Dense, 11, 100_000),
            (Shape.Dense, 12, 8_000),
            (Shape.Sparse, 13, 60_000),
            (Shape.Sparse, 14, 2_000),
            (Shape.SharedLowBits, 15, 80_000),
            (Shape.SharedLowBits, 16, 5_000),
            (Shape.Regions, 101, 150_000),
            (Shape.Regions, 17, 150_000),
        ];

        private static readonly string[] ExpectedChecksums =
        [
            "42552B3EA4683967219D4292B38F9C24B1CC3090715E3F50BAB3FECB8D125ADA",
            "B2BFEC570056E047BF62084E65F56C5806040C42FEB2A003B51CB714E7DF95E7",
            "7F9EF4B9DE000C789FA3382A54C8C322BF01C02FDD10204A19709BE8DBC6CD0D",
            "46BEA8B0CF5009AA219A8A4193B65CA58850857D501381A024901B98D9B05272",
            "66F60238D6F26BFFA45CF5A29A98CDFC33AC073D113B780C653EDEE3346D5D9E",
            "CE5F83B8B63ECDC435D2DA46CC1E71DF97F10591B45ECFF4485124F544588ABE",
            "858BD48BEC251AA4609F3FC057DC17B2F3A3496B0DE52A6D2D3916F49649C6CF",
            "8DD4314315FE0BAFBF48EF64EA3929E64B510562EEA3E0DAB352A20DCC55DA6E",
        ];

        [RavenFact(RavenTestCategory.Voron)]
        public void ManyPageMergesInOneTransactionDoNotKeepMemoryForEachMerge()
        {
            // Many merges in one transaction must not keep memory for each merge.
            const int lists = 8;
            const int size = 375_000;
            var states = new PostingListState[lists];
            var removals = new long[lists][];

            using (var wtx = Env.WriteTransaction())
            {
                for (int l = 0; l < lists; l++)
                {
                    // equal low bits pack densely, so a leaf page holds many entries
                    long[] ids = Enumerable.Range(1, size).Select(i => (long)i << 10).ToArray();
                    states[l] = Apply(wtx.LowLevelTransaction, default, ids, Array.Empty<long>());
                    // runs of neighbours go away, which leaves pages under half full
                    removals[l] = ids.Where((_, i) => i / 5_000 % 5 != 0).ToArray();
                }

                wtx.Commit();
            }

            using (var wtx = Env.WriteTransaction())
            {
                int leafPagesBefore = states.Sum(x => x.LeafPages);
                long before = NativeMemory.CurrentThreadStats.TotalAllocated;
                for (int l = 0; l < lists; l++)
                    states[l] = Apply(wtx.LowLevelTransaction, states[l], Array.Empty<long>(), removals[l]);
                long held = NativeMemory.CurrentThreadStats.TotalAllocated - before;

                int merged = leafPagesBefore - states.Sum(x => x.LeafPages);
                output.WriteLine($"merged pages: {merged}, native memory held by the transaction: {held / 1024} KB");
                Assert.True(merged > 30, $"the test needs many merges, got {merged}");
                Assert.True(held < MemoryBoundInBytes, $"{held / 1024} KB held for {merged} merged pages");
                wtx.Commit();
            }
        }

        private const long MemoryBoundInBytes = 16L * 1024 * 1024;
    }
}
