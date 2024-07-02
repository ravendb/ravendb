using System;
using System.Collections.Generic;
using System.Runtime.Intrinsics.X86;
using Corax.Querying.Matches.Meta;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax
{
    public unsafe class PrimitivesTests : RavenTestBase
    {
        public PrimitivesTests(ITestOutputHelper output) : base(output)
        {
        }


        [RavenMultiplatformFact(RavenTestCategory.Corax, RavenIntrinsics.Vector256)]
        public void SmallForVectorized()
        {
            long* aS = stackalloc long[256];
            long* bS = stackalloc long[256];
            long* dS = stackalloc long[256];

            long* aV = stackalloc long[256];
            long* bV = stackalloc long[256];
            long* dV = stackalloc long[256];

            aS[0] = 10;
            aS[1] = 12;
            bS[0] = 10;
            bS[1] = 11;
            bS[2] = 12;

            aV[0] = 10;
            aV[1] = 12;
            bV[0] = 10;
            bV[1] = 11;
            bV[2] = 12;

            new Span<long>(dS, 256).Fill(0);
            new Span<long>(dV, 256).Fill(0);

            int rS = MergeHelper.AndScalar(dS, 256, aS, 1, bS, 1);
            int rv = MergeHelper.AndVectorized(dV, 256, aV, 1, bV, 1);
            Assert.Equal(rS, rv);
            Assert.Equal(dS[0], dV[0]);
            Assert.Equal(dS[1], dV[1]);
            Assert.Equal(10, dV[0]);

            new Span<long>(dS, 256).Fill(0);
            new Span<long>(dV, 256).Fill(0);
            rS = MergeHelper.AndScalar(dS, 256, aS, 1, bS, 0);
            rv = MergeHelper.AndVectorized(dV, 256, aV, 1, bV, 0);
            Assert.Equal(rS, rv);
            Assert.Equal(dS[0], dV[0]);
            Assert.Equal(dS[1], dV[1]);
            Assert.Equal(0, dV[0]);

            new Span<long>(dS, 256).Fill(0);
            new Span<long>(dV, 256).Fill(0);
            rS = MergeHelper.AndScalar(dS, 256, aS, 0, bS, 1);
            rv = MergeHelper.AndVectorized(dV, 256, aV, 0, bV, 1);
            Assert.Equal(rS, rv);
            Assert.Equal(dS[0], dV[0]);
            Assert.Equal(dS[1], dV[1]);
            Assert.Equal(0, dV[0]);

            new Span<long>(dS, 256).Fill(0);
            new Span<long>(dV, 256).Fill(0);
            rS = MergeHelper.AndScalar(dS, 256, aS, 0, bS, 0);
            rv = MergeHelper.AndVectorized(dV, 256, aV, 0, bV, 0);
            Assert.Equal(rS, rv);
            Assert.Equal(dS[0], dV[0]);
            Assert.Equal(dS[1], dV[1]);
            Assert.Equal(0, dV[0]);

            new Span<long>(dS, 256).Fill(0);
            new Span<long>(dV, 256).Fill(0);
            rS = MergeHelper.AndScalar(dS, 256, aS, 2, bS, 3);
            rv = MergeHelper.AndVectorized(dV, 256, aV, 2, bV, 3);
            Assert.Equal(rS, rv);
            Assert.Equal(dS[0], dV[0]);
            Assert.Equal(dS[1], dV[1]);
            Assert.Equal(10, dV[0]);
            Assert.Equal(12, dV[1]);

            new Span<long>(dS, 256).Fill(0);
            new Span<long>(dV, 256).Fill(0);
            rS = MergeHelper.AndScalar(dS, 256, aS, 1, bS, 3);
            rv = MergeHelper.AndVectorized(dV, 256, aV, 1, bV, 3);
            Assert.Equal(rS, rv);
            Assert.Equal(dS[0], dV[0]);
            Assert.Equal(dS[1], dV[1]);
            Assert.Equal(10, dV[0]);
        }

        [RavenMultiplatformTheory(RavenTestCategory.Corax, RavenIntrinsics.Vector256)]
        [InlineData(125, 89, 90, 22)]
        [InlineData(125, 1, 24, 1)]
        [InlineData(125, 24, 1, 1)]
        public void OutputCompatibilityForVectorizedSingleRun(int seed, int ai, int bi, int ss )
        {
            Span<long> aS = stackalloc long[256];
            Span<long> bS = stackalloc long[256];
            Span<long> dS = stackalloc long[256];

            Span<long> aV = stackalloc long[256];
            Span<long> bV = stackalloc long[256];
            Span<long> dV = stackalloc long[256];

            var rnd = new Random(125);

            HashSet<long> alreadySeen = new HashSet<long>();

            var sharedValues = new long[4096];
            CreateNumbersSet(rnd, alreadySeen, sharedValues);
            var aValues = new long[4096];
            CreateNumbersSet(rnd, alreadySeen, aValues);
            var bValues = new long[4096];
            CreateNumbersSet(rnd, alreadySeen, bValues);

            aS.Fill(0); bS.Fill(0); dS.Fill(0);
            aV.Fill(0); bV.Fill(0); dV.Fill(0);

            var currentShared = sharedValues.AsSpan(0, ss);
            currentShared.CopyTo(aS);
            currentShared.CopyTo(bS);

            aValues.AsSpan(0, ai - ss).CopyTo(aS.Slice(ss));
            bValues.AsSpan(0, bi - ss).CopyTo(bS.Slice(ss));

            // We are going to sort all the arrays, as they are needed for And to work.                                                                        
            var currentAS = aS.Slice(0, ai);
            var currentBS = bS.Slice(0, bi);
            MemoryExtensions.Sort(currentAS);
            MemoryExtensions.Sort(currentBS);

            var currentAV = aV.Slice(0, ai);
            var currentBV = bV.Slice(0, bi);
            currentAS.CopyTo(currentAV);
            currentBS.CopyTo(currentBV);

            int rS = MergeHelper.AndScalar(dS, currentAS, currentBS);
            int rv = MergeHelper.AndVectorized(dV, currentAV, currentBV);
            Assert.Equal(rS, rv);

            for (int i = 0; i < rS; i++)
                Assert.Equal(dS[i], dV[i]);

            for (int i = rS; i < dS.Length; i++)
            {
                Assert.Equal(0, dS[i]);
                Assert.Equal(0, dV[i]);
            }
        }

        [RavenMultiplatformTheory(RavenTestCategory.Corax, RavenIntrinsics.Vector256, Skip="Only used to find runs that could be problematic.")]
        [InlineData(1337)]
        public void OutputCompatibilityForVectorizedExhaustive(int seed)
        {
            Span<long> aS = stackalloc long[256];
            Span<long> bS = stackalloc long[256];
            Span<long> dS = stackalloc long[256];

            Span<long> aV = stackalloc long[256];
            Span<long> bV = stackalloc long[256];
            Span<long> dV = stackalloc long[256];

            var rnd = new Random(seed);
    
            HashSet<long> alreadySeen = new HashSet<long>();

            var sharedValues = new long[4096];
            CreateNumbersSet(rnd, alreadySeen, sharedValues);
            var aValues = new long[4096];
            CreateNumbersSet(rnd, alreadySeen, aValues);
            var bValues = new long[4096];
            CreateNumbersSet(rnd, alreadySeen, bValues);

            // For every single possible size of the A sequence.
            for (int ai = 0; ai < 256; ai++)
            {
                // For every single possible size of the B sequence.
                for (int bi = 0; bi < 256; bi++)
                {
                    int maxSharedCount = Math.Min(ai, bi);

                    // We should have exactly ss shared values between the 2. 
                    for (int ss = 0; ss <= maxSharedCount; ss++)
                    {                        
                        aS.Fill(0); bS.Fill(0); dS.Fill(0);
                        aV.Fill(0); bV.Fill(0); dV.Fill(0);

                        var currentShared = sharedValues.AsSpan(0, ss);
                        currentShared.CopyTo(aS);
                        currentShared.CopyTo(bS);

                        aValues.AsSpan(0, ai - ss).CopyTo(aS.Slice(ss));
                        bValues.AsSpan(0, bi - ss).CopyTo(bS.Slice(ss));

                        // We are going to sort all the arrays, as they are needed for And to work.                                                                        
                        var currentAS = aS.Slice(0, ai);
                        var currentBS = bS.Slice(0, bi);
                        MemoryExtensions.Sort(currentAS);
                        MemoryExtensions.Sort(currentBS);                        

                        var currentAV = aV.Slice(0, ai);                        
                        var currentBV = bV.Slice(0, bi);
                        currentAS.CopyTo(currentAV);
                        currentBS.CopyTo(currentBV);

                        int rS = MergeHelper.AndScalar(dS, currentAS, currentBS);
                        int rv = MergeHelper.AndVectorized(dV, currentAV, currentBV);
                        Assert.Equal(rS, rv);

                        for (int i = 0; i < rS; i++)
                            Assert.Equal(dS[i], dV[i]);

                        for (int i = rS; i < dS.Length; i++)
                        {
                            Assert.Equal(0, dS[i]);
                            Assert.Equal(0, dV[i]);
                        }
                    }
                }
            }
        }
        static void CreateNumbersSet(Random rnd, HashSet<long> alreadySeen, long[] values)
        {
            int i = 0;
            while (i < values.Length)
            {
                int value = rnd.Next(0, 50000);
                if (alreadySeen.Contains(value))
                    continue;

                alreadySeen.Add(value);
                values[i] = value;
                i++;
            };
        }
    }
}
