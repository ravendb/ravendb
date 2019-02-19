using System;
using System.Collections.Generic;
using Sparrow;
using Sparrow.Server.Utils;
using Xunit;

namespace FastTests.Sparrow
{
    public unsafe class DiffPagesTests : NoDisposalNeeded
    {
        [Fact]
        public void CanComputeNoDifference()
        {

            var fst = new byte[4096];
            var sec = new byte[4096];

            new Random().NextBytes(fst);
            Buffer.BlockCopy(fst, 0, sec, 0, fst.Length);

            fixed (byte* one = fst)
            fixed (byte* two = sec)
            fixed (byte* tmp = new byte[4096])
            {
                var diffPages = new DiffPages
                {
                    Output = tmp,
                };

                diffPages.ComputeDiff(one, two, 4096);

                Assert.True(diffPages.IsDiff);
                Assert.Equal(0, diffPages.OutputSize);
            }
        }

        [Fact]
        public void CanComputeSmallDifference()
        {
            var fst = new byte[4096];
            var sec = new byte[4096];

            new Random().NextBytes(fst);
            Buffer.BlockCopy(fst, 0, sec, 0, fst.Length);

            sec[12] ++;
            sec[433]++;

            fixed (byte* one = fst)
            fixed (byte* two = sec)
            fixed (byte* tmp = new byte[4096])
            {
                var diffPages = new DiffPages
                {
                    Output = tmp,
                };

                diffPages.ComputeDiff(one, two, 4096);

                Assert.Equal(96, diffPages.OutputSize);
            }
        }

        [Fact]
        public void CanComputeSmallDifferenceFromNew()
        {
            var fst = new byte[4096];


            fst[12]++;
            fst[433]++;

            fixed (byte* one = fst)
            fixed (byte* tmp = new byte[4096])
            {
                var diffPages = new DiffPages
                {
                    Output = tmp,
                };

                diffPages.ComputeNew(one, 4096);
                Assert.True(diffPages.IsDiff);

                Assert.Equal(96, diffPages.OutputSize);
            }
        }

        [Fact]
        public void CanComputeSmallDifference_AndThenApplyit()
        {
            var fst = new byte[4096];
            var sec = new byte[4096];
            var trd = new byte[4096];

            new Random().NextBytes(fst);
            Buffer.BlockCopy(fst, 0, sec, 0, fst.Length);

            sec[12]++;
            sec[433]++;

            fixed (byte* one = fst)
            fixed (byte* two = sec)
            fixed (byte* tri = trd)
            fixed (byte* tmp = new byte[4096])
            {
                var diffPages = new DiffPages
                {
                    Output = tmp,
                };

                diffPages.ComputeDiff(one, two, 4096);
                Assert.True(diffPages.IsDiff);

                Memory.Copy(tri, one, 4096);
                new DiffApplier
                {
                    Destination = tri,
                    Diff = tmp,
                    Size = 4096,
                    DiffSize = diffPages.OutputSize
                }.Apply(false);

                var result = Memory.Compare(tri, two, 4096, out int position);
                Assert.Equal(0, result);
            }
        }

        public static IEnumerable<object[]> ChangedBytes
        {
            get
            {
                return new[]
                {
                    new object[] { 0 },
                    new object[] { (4096 * 4 - 1) },
                    new object[] { 513 },
                    new object[] { 1023 },
                    new object[] { 1024 },
                    new object[] { 1025 },
                    new object[] { (4096 * 4 - 2) },
                    new object[] { 1 },
                    new object[] { 65 },
                    new object[] { 63 },
                    new object[] { 64 },
                };
            }
        }

        [Theory]
        [MemberData(nameof(ChangedBytes))]
        public void CanComputeSmallDifference_AndThenApplyOnBig(int value)
        {
            var fst = new byte[4096 * 4];
            var sec = new byte[4096 * 4];
            var trd = new byte[4096 * 4];

            new Random().NextBytes(fst);
            Buffer.BlockCopy(fst, 0, sec, 0, fst.Length);

            sec[value]++;

            fixed (byte* one = fst)
            fixed (byte* two = sec)
            fixed (byte* tri = trd)
            fixed (byte* tmp = new byte[4096])
            {
                var diffPages = new DiffPages
                {
                    Output = tmp,
                };

                diffPages.ComputeDiff(one, two, 4096);
                Assert.True(diffPages.IsDiff);

                Memory.Copy(tri, one, 4096);
                new DiffApplier
                {
                    Destination = tri,
                    Diff = tmp,
                    Size = 4096,
                    DiffSize = diffPages.OutputSize
                }.Apply(false);

                var result = Memory.Compare(tri, two, 4096, out int position);
                Assert.Equal(0, result);
            }
        }

        [Fact]
        public void ComputeAndThenApplyRandomized()
        {
            const int Size = 4096 * 4;

            var fst = new byte[Size];
            var sec = new byte[Size];
            var trd = new byte[Size];

            var rnd = new Random(1337);
            rnd.NextBytes(fst);
            Buffer.BlockCopy(fst, 0, sec, 0, fst.Length);

            fixed (byte* one = fst)
            fixed (byte* two = sec)
            fixed (byte* tri = trd)
            fixed (byte* tmp = new byte[Size])
            {                
                for (int i = 0; i < 4096; i++)
                {
                    // We are going to change one byte at a time and try to reconstruct. 
                    int idx = rnd.Next(Size);
                    sec[idx]++;

                    var diffPages = new DiffPages
                    {
                        Output = tmp,
                    };

                    Memory.Set(tri, 0, Size);
                    Memory.Set(tmp, 0, Size);

                    diffPages.ComputeDiff(one, two, Size);
                    if (!diffPages.IsDiff)
                        return;

                    Memory.Copy(tri, one, Size);
                    new DiffApplier
                    {
                        Destination = tri,
                        Diff = tmp,
                        Size = Size,
                        DiffSize = diffPages.OutputSize
                    }.Apply(false);

                    var result = Memory.Compare(tri, two, Size, out int position);
                    if ( result != 0 )
                        Console.WriteLine($"The position at fault is '{position}'");
                    Assert.Equal(0, result);
                }
            }
        }

        [Fact]
        public void CompletelyDifferent()
        {
            var fst = new byte[4096];
            var sec = new byte[4096];

            new Random(1).NextBytes(fst);
            new Random(2).NextBytes(sec);

            fixed (byte* one = fst)
            fixed (byte* two = sec)
            fixed (byte* tmp = new byte[4096])
            {
                var diffPages = new DiffPages
                {
                    Output = tmp,
                };

                diffPages.ComputeDiff(one, two, 4096);

                Assert.False(diffPages.IsDiff);
            }
        }

        [Fact]
        public void Applying_diff_calculated_as_new_multiple_times()
        {
            const int size = 4096;

            var fst = new byte[size];
            var sec = new byte[size];

            var r = new Random(1);

            for (int i = 0; i < 10; i++)
            {
                fst[i] = (byte)r.Next(0, 255);
                sec[(size - 1) - i] = (byte)r.Next(0, 255);
            }

            var result = new byte[size];

            foreach (var page in new[] { fst, sec })
            {
                fixed (byte* ptr = page)
                fixed (byte* diffPtr = new byte[size])
                fixed (byte* resultPtr = result)
                {
                    var diffPages = new DiffPages
                    {
                        Output = diffPtr,
                    };

                    diffPages.ComputeNew(ptr, size);

                    Assert.True(diffPages.IsDiff);

                    new DiffApplier
                    {
                        Destination = resultPtr,
                        Diff = diffPtr,
                        Size = page.Length,
                        DiffSize = diffPages.OutputSize
                    }.Apply(true);
                }
            }

            Assert.Equal(sec, result);
        }
    }
}
