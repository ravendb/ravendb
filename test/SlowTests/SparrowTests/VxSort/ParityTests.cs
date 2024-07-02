using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Sparrow.Server.Utils.VxSort;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

using static SlowTests.SparrowTests.VxSort.DataGeneration;
using DataGenerator = System.Func<(int[] data, int[] sortedData)>;

namespace SlowTests.SparrowTests.VxSort
{
    public class ParityTests : NoDisposalNeeded
    {
        static int NumCycles => int.Parse(Environment.GetEnvironmentVariable("NUM_CYCLES") ?? "10");

        static readonly int[] ArraySizes =
        {
            10,
            100,
            BitonicSort.MaxBitonicLength<int>() - 1,
            BitonicSort.MaxBitonicLength<int>(),
            BitonicSort.MaxBitonicLength<int>() + 1,
            1_000,
            10_000,
            100_000,
            1_000_000
        };

        static readonly int[] ConstantSeeds = { 666, 333, 999, 314159 };

        public ParityTests(ITestOutputHelper output) : base(output)
        {
        }

        public static IEnumerable<object[]> PreSorted =>
            from size in ArraySizes
            from i in Enumerable.Range(0, NumCycles)
            let realSize = size + i
            let generator = new DataGenerator(() =>
            (
                Enumerable.Range(0, realSize).ToArray(),
                Enumerable.Range(0, realSize).ToArray()
            ))
            select new object[] { generator.Labeled($"S{realSize:0000000}") };

        public static IEnumerable<object[]> ReverseSorted =>
            from size in ArraySizes
            from i in Enumerable.Range(0, NumCycles)
            let realSize = size + i
            let generator = new DataGenerator(
                () =>
                (
                    Enumerable.Range(0, realSize).Reverse().ToArray(),
                    Enumerable.Range(0, realSize).ToArray()
                )
            )
            select new object[] { generator.Labeled($"Ƨ{realSize:0000000}") };

        public static IEnumerable<object[]> HalfMinValue =>
            from size in ArraySizes
            from seed in ConstantSeeds
            from i in Enumerable.Range(0, NumCycles)
            let realSize = size + i
            let generator = new DataGenerator(
                () => GenerateData(realSize, seed, int.MinValue, 0.5)
            )
            select new object[] { generator.Labeled($"{realSize:0000000}/{seed}/0.5min") };

        public static IEnumerable<object[]> HalfMaxValue =>
            from size in ArraySizes
            from seed in ConstantSeeds
            from i in Enumerable.Range(0, NumCycles)
            let realSize = size + i
            let generator = new DataGenerator(
                () => GenerateData(realSize, seed, int.MaxValue, 0.5)
            )
            select new object[] { generator.Labeled($"{realSize:0000000}/{seed}/0.5max") };

        public static IEnumerable<object[]> AllOnes =>
            from size in ArraySizes
            from i in Enumerable.Range(0, NumCycles)
            let realSize = size + i
            let generator = new DataGenerator(
                () =>
                (
                    Enumerable.Repeat(1, realSize).ToArray(),
                    Enumerable.Repeat(1, realSize).ToArray()
                )
            )
            select new object[] { generator.Labeled($"1:{realSize:0000000}") };

        public static IEnumerable<object[]> ConstantSeed =>
            from size in ArraySizes
            from seed in ConstantSeeds
            from i in Enumerable.Range(0, NumCycles)
            let realSize = size + i
            let generator = new DataGenerator(
                () => GenerateData(realSize, seed)
            )
            select new object[] { generator.Labeled($"{realSize:0000000}/{seed}") };

        public static IEnumerable<object[]> TimeSeed =>
            from size in ArraySizes
            from i in Enumerable.Range(0, NumCycles)
            let realSize = size + i
            let seed = ((int)DateTime.Now.Ticks + i * 666) % int.MaxValue
            let generator = new DataGenerator(
                () => GenerateData(realSize, seed)
            )
            select new object[] { generator.Labeled($"{realSize:0000000}/R{i}") };



        [RavenMultiplatformTheory(RavenTestCategory.Intrinsics, RavenIntrinsics.Sse)]
        [MemberData(nameof(PreSorted))]
        [MemberData(nameof(ReverseSorted))]
        [MemberData(nameof(HalfMinValue))]
        [MemberData(nameof(HalfMaxValue))]
        [MemberData(nameof(AllOnes))]
        [MemberData(nameof(ConstantSeed))]
        [MemberData(nameof(TimeSeed))]
        public void VxSortPrimitiveUnstable(RavenTestWithLabel<DataGenerator> t)
        {
            var (randomData, sortedData) = t.Data();
            Sort.Run(randomData);

            Assert.Equal(randomData, sortedData);
        }
    }
}
