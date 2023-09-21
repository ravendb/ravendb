using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public class SortingTests : NoDisposalNeeded
    {
        private readonly struct NumericComparer : IComparer<long>, IComparer<int>, IComparer<uint>, IComparer<ulong>
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(long x, long y)
            {
                return Math.Sign(x - y);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(int x, int y)
            {
                return x - y;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(ulong x, ulong y)
            {
                // We need to use branching here because without sign flags we can overflow and return wrong values.
                return x == y ? 0 : x > y ? 1 : -1;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(uint x, uint y)
            {
                // We need to use branching here because without sign flags we can overflow and return wrong values.
                return x == y ? 0 : x > y ? 1 : -1;
            }
        }

        public SortingTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Small()
        {
            int[] values = { 9, 7, 6, 5, 4, 3, 2, 1 };
            int[] result = new int[values.Length];
            Array.Copy(values, result, values.Length);
            Array.Sort(result);

            var sorter = new Sorter<int, NumericComparer>();
            sorter.Sort(values);

            Assert.Equal(values, result);
        }

        [Fact]
        public void SmallWithDuplicates()
        {
            int[] values = { 9, 9, 9, 9, 2, 2, 2, 1 };
            int[] result = new int[values.Length];
            Array.Copy(values, result, values.Length);
            Array.Sort(result);


            var sorter = new Sorter<int, NumericComparer>();
            sorter.Sort(values);

            Assert.Equal(values, result);
        }

        private static int[] Generator(int size)
        {
            var gen = new Random(size);

            var result = new int[size];
            for (int i = 0; i < size; i++)
                result[i] = gen.Next();
            return result;
        }

        private static int[] GeneratorWithDuplicates(int size)
        {
            var gen = new Random(size);

            var result = new int[size];
            for (int i = 0; i < size; i++)
                result[i] = gen.Next(0, size / 4);
            return result;
        }

        public static IEnumerable<int> Size => new[]
        {
           100,
           200,
           1000,
           4096,
           10000,
           1024 * 512
        };

        [Fact]
        public void DifferentSizesWithValues()
        {
            foreach (int size in Size)
            {
                int[] keys = Generator(size);
                int[] values = (int[])keys.Clone();
                int[] result = (int[])keys.Clone();

                Array.Sort(result);

                var sorter = new Sorter<int, int, NumericComparer>();
                sorter.Sort(keys, values);

                Assert.Equal(keys, result);
                Assert.Equal(keys, values);
            }
        }

        [Fact]
        public void DifferentSizes()
        {
            foreach (int size in Size)
            {
                int[] keys = Generator(size);
                int[] result = (int[])keys.Clone();

                Array.Sort(result);

                var sorter = new Sorter<int, NumericComparer>();
                sorter.Sort(keys);

                Assert.Equal(keys, result);
            }
        }

        [Fact]
        public void IncreasingSizes()
        {
            for (int i = 0; i < 512; i++)
            {
                int[] keys = Generator(i);
                int[] result = (int[])keys.Clone();

                Array.Sort(result);

                var sorter = new Sorter<int, NumericComparer>();
                sorter.Sort(keys);

                Assert.Equal(keys, result);
            }
        }

        [Fact]
        public void IncreasingSizesWithValues()
        {
            for (int i = 0; i < 512; i++)
            {
                int[] keys = Generator(i);
                int[] values = (int[])keys.Clone();
                int[] result = (int[])keys.Clone();
                
                Array.Sort(result);

                var sorter = new Sorter<int, int, NumericComparer>();
                sorter.Sort(keys, values);

                Assert.Equal(keys, result);
                Assert.Equal(keys, values);
            }
        }
    }
}
