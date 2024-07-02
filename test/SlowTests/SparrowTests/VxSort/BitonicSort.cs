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
    public class BitonicSortTests : NoDisposalNeeded
    {
        public BitonicSortTests(ITestOutputHelper output) : base(output) {}

        static readonly int[] BitonicSizes =
        {
            8, 16, 24, 32, 40, 48, 56, 64, 72, 80, 88, 96, 104, 112, 120, 128
        };

        public static IEnumerable<object[]> PreSorted =>
            from size in BitonicSizes
            let generator = new DataGenerator(() =>
            (
                Enumerable.Range(0, size).ToArray(),
                Enumerable.Range(0, size).ToArray()
            ))
            select new object[] { generator.Labeled($"{size:000}/S") };

        public static IEnumerable<object[]> ReverseSorted =>
            from size in BitonicSizes
            let generator = new DataGenerator(() =>
            (
                Enumerable.Range(0, size).Reverse().ToArray(),
                Enumerable.Range(0, size).ToArray()
            ))
            select new object[] { generator.Labeled($"Ƨ{size:0000000}") };

        public static IEnumerable<object[]> HalfMinValue =>
            from size in BitonicSizes
            from seed in new[] { 666, 333, 999, 314159 }
            let generator = new DataGenerator(
                () => GenerateData(size, seed, 0, 0.5))
            select new object[] { generator.Labeled($"{size:000}/{seed}/0.5min") };

        public static IEnumerable<object[]> HalfMaxValue =>
            from size in BitonicSizes
            from seed in new[] { 666, 333, 999, 314159 }
            let generator = new DataGenerator(
                () => GenerateData(size, seed, int.MaxValue, 0.5))
            select new object[] { generator.Labeled($"{size:000}/{seed}/0.5max") };

        public static IEnumerable<object[]> AllOnes =>
            from size in BitonicSizes
            let generator = new DataGenerator(
                () =>
                (
                    Enumerable.Repeat(1, size).ToArray(),
                    Enumerable.Repeat(1, size).ToArray()
                )
            )
            select new object[] { generator.Labeled($"1:{size:0000000}") };

        public static IEnumerable<object[]> ConstantSeed =>
            from size in BitonicSizes
            from seed in new[] { 666, 333, 999, 314159 }
            let generator = new DataGenerator(
                () => GenerateData(size, seed, modulo: 100))
            select new object[] { generator.Labeled($"{size:000}/{seed}") };

        public static IEnumerable<object[]> TimeSeed =>
            from size in BitonicSizes
            let numIterations = int.Parse(Environment.GetEnvironmentVariable("NUM_CYCLES") ?? "100")
            from i in Enumerable.Range(0, numIterations)
            let seed = ((int)DateTime.Now.Ticks + i * 666) % int.MaxValue
            let generator = new DataGenerator(
                () => GenerateData(size, seed))
            select new object[] { generator.Labeled($"{size:000}/R{i}") };

        [RavenMultiplatformTheory(RavenTestCategory.Intrinsics, RavenIntrinsics.Avx256)]
        [MemberData(nameof(PreSorted))]
        [MemberData(nameof(HalfMinValue))]
        [MemberData(nameof(HalfMaxValue))]
        [MemberData(nameof(AllOnes))]
        [MemberData(nameof(ConstantSeed))]
        [MemberData(nameof(TimeSeed))]
        public unsafe void BitonicSortIntTest(RavenTestWithLabel<DataGenerator> t)
        {
            var (randomData, sortedData) = t.Data();

            int maxIntBitonicSize = BitonicSort.MaxBitonicLength<int>();
            if (randomData.Length > maxIntBitonicSize)
                return;

            fixed (int* p = &randomData[0])
            {
                BitonicSort.Sort(p, randomData.Length);
            }

            Assert.Equal(randomData, sortedData);
        }

        [RavenMultiplatformTheory(RavenTestCategory.Intrinsics, RavenIntrinsics.Avx256)]
        [MemberData(nameof(PreSorted))]
        [MemberData(nameof(HalfMinValue))]
        [MemberData(nameof(HalfMaxValue))]
        [MemberData(nameof(ConstantSeed))]
        [MemberData(nameof(TimeSeed))]
        public unsafe void BitonicSortLongTest(RavenTestWithLabel<DataGenerator> t)
        {
            var (randomIntData, sortedIntData) = t.Data();

            int maxLongBitonicSize = BitonicSort.MaxBitonicLength<long>();
            if (randomIntData.Length > maxLongBitonicSize)
                return;

            long[] randomData = new long[randomIntData.Length];
            long[] sortedData = new long[sortedIntData.Length];
            for (int i = 0; i < randomIntData.Length; i++)
            {
                randomData[i] = randomIntData[i];
                sortedData[i] = sortedIntData[i];
            }

            fixed (long* p = &randomData[0])
            {
                BitonicSort.Sort(p, randomData.Length);
            }

            Assert.Equal(randomData, sortedData);
        }

        [RavenMultiplatformTheory(RavenTestCategory.Intrinsics, RavenIntrinsics.Avx256)]
        [MemberData(nameof(PreSorted))]
        [MemberData(nameof(HalfMinValue))]
        [MemberData(nameof(HalfMaxValue))]
        [MemberData(nameof(ConstantSeed))]
        [MemberData(nameof(TimeSeed))]
        public unsafe void BitonicSortULongTest(RavenTestWithLabel<DataGenerator> t)
        {
            var (randomIntData, sortedIntData) = t.Data();

            int maxLongBitonicSize = BitonicSort.MaxBitonicLength<ulong>();
            if (randomIntData.Length > maxLongBitonicSize)
                return;

            ulong[] randomData = new ulong[randomIntData.Length];
            ulong[] sortedData = new ulong[sortedIntData.Length];
            for (int i = 0; i < randomIntData.Length; i++)
            {
                randomData[i] = (ulong)randomIntData[i];
                sortedData[i] = (ulong)sortedIntData[i];
            }

            fixed (ulong* p = &randomData[0])
            {
                BitonicSort.Sort(p, randomData.Length);
            }

            Assert.Equal(randomData, sortedData);
        }

        [RavenMultiplatformTheory(RavenTestCategory.Intrinsics, RavenIntrinsics.Avx256)]
        [MemberData(nameof(PreSorted))]
        [MemberData(nameof(HalfMinValue))]
        [MemberData(nameof(HalfMaxValue))]
        [MemberData(nameof(ConstantSeed))]
        [MemberData(nameof(TimeSeed))]
        public unsafe void BitonicSortUIntTest(RavenTestWithLabel<DataGenerator> t)
        {
            var (randomIntData, sortedIntData) = t.Data();

            int maxIntBitonicSize = BitonicSort.MaxBitonicLength<int>();
            if (randomIntData.Length > maxIntBitonicSize)
                return;

            uint[] randomData = new uint[randomIntData.Length];
            uint[] sortedData = new uint[sortedIntData.Length];
            for (int i = 0; i < randomIntData.Length; i++)
            {
                randomData[i] = (uint)randomIntData[i];
                sortedData[i] = (uint)sortedIntData[i];
            }

            fixed (uint* p = &randomData[0])
            {
                BitonicSort.Sort(p, randomData.Length);
            }

            Assert.Equal(randomData, sortedData);
        }

        [RavenMultiplatformTheory(RavenTestCategory.Intrinsics, RavenIntrinsics.Avx256)]
        [MemberData(nameof(PreSorted))]
        [MemberData(nameof(HalfMinValue))]
        [MemberData(nameof(HalfMaxValue))]
        [MemberData(nameof(ConstantSeed))]
        [MemberData(nameof(TimeSeed))]
        public unsafe void BitonicSortFloatTest(RavenTestWithLabel<DataGenerator> t)
        {
            var (randomIntData, sortedIntData) = t.Data();

            int maxFloatBitonicSize = BitonicSort.MaxBitonicLength<float>();
            if (randomIntData.Length > maxFloatBitonicSize)
                return;

            float[] randomData = new float[randomIntData.Length];
            float[] sortedData = new float[sortedIntData.Length];
            for (int i = 0; i < randomIntData.Length; i++)
            {
                randomData[i] = randomIntData[i];
                sortedData[i] = sortedIntData[i];
            }

            fixed (float* p = &randomData[0])
            {
                BitonicSort.Sort(p, randomData.Length);
            }

            Assert.Equal(randomData, sortedData);
        }

        [RavenMultiplatformTheory(RavenTestCategory.Intrinsics, RavenIntrinsics.Avx256)]
        [MemberData(nameof(PreSorted))]
        [MemberData(nameof(HalfMinValue))]
        [MemberData(nameof(HalfMaxValue))]
        [MemberData(nameof(ConstantSeed))]
        [MemberData(nameof(TimeSeed))]
        public unsafe void BitonicSortDoubleTest(RavenTestWithLabel<DataGenerator> t)
        {
            var (randomIntData, sortedIntData) = t.Data();

            int maxDoubleBitonicSize = BitonicSort.MaxBitonicLength<double>();
            if (randomIntData.Length > maxDoubleBitonicSize)
                return;

            double[] randomData = new double[randomIntData.Length];
            double[] sortedData = new double[sortedIntData.Length];
            for (int i = 0; i < randomIntData.Length; i++)
            {
                randomData[i] = randomIntData[i];
                sortedData[i] = sortedIntData[i];
            }

            fixed (double* p = &randomData[0])
            {
                BitonicSort.Sort(p, randomData.Length);
            }

            Assert.Equal(randomData, sortedData);
        }
    }
}
