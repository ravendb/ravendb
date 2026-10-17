using System;
using FastTests;
using Sparrow;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.SparrowTests
{
    public unsafe class MemoryTests : NoDisposalNeeded
    {
        public MemoryTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed]
        public void LoopDifferencesWithRandomData(int seed)
        {
            var s1 = new byte[1024];
            var s2 = new byte[1024];
            
            var rnd = new Random(seed);
            rnd.NextBytes(s1);
            s1.CopyTo(s2.AsSpan());

            for (int size = 0; size < 1024; size++)
            {
                for (int i = 0; i < s1.Length; i++)
                {
                    s2[i] = (byte)rnd.Next(0, byte.MaxValue);

                    var s1s2Reference = Math.Sign(s1.AsSpan().SequenceCompareTo(s2.AsSpan()));
                    var s2s1Reference = Math.Sign(s2.AsSpan().SequenceCompareTo(s1.AsSpan()));

                    fixed (byte* s1Ptr = s1)
                    fixed (byte* s2Ptr = s2)
                    {
                        try
                        {
                            // We set the particular place to fit

                            Assert.Equal(s1s2Reference, Math.Sign(Memory.CompareInline(s1Ptr, s2Ptr, s1.Length)));
                            Assert.Equal(s2s1Reference, Math.Sign(Memory.CompareInline(s2Ptr, s1Ptr, s1.Length)));
                            
                            Assert.Equal(s1s2Reference, Math.Sign(Memory.CompareInlineNet6OorLesser(s1Ptr, s2Ptr, s1.Length)));
                            Assert.Equal(s2s1Reference, Math.Sign(Memory.CompareInlineNet6OorLesser(s2Ptr, s1Ptr, s1.Length)));

                            Assert.Equal(s1s2Reference, Math.Sign(Memory.CompareSmallInlineNet7(ref s1[0], ref s2[0], s1.Length)));
                            Assert.Equal(s2s1Reference, Math.Sign(Memory.CompareSmallInlineNet7(ref s2[0], ref s1[0], s1.Length)));

                            if (AdvInstructionSet.X86.IsSupportedAvx256)
                            {
                                Assert.Equal(s1s2Reference, Math.Sign(Memory.CompareAvx256(s1Ptr, s2Ptr, s1.Length)));
                                Assert.Equal(s2s1Reference, Math.Sign(Memory.CompareAvx256(s2Ptr, s1Ptr, s1.Length)));

                                Assert.Equal(s1s2Reference, Math.Sign(Memory.CompareAvx256(ref s1[0], ref s2[0], s1.Length)));
                                Assert.Equal(s2s1Reference, Math.Sign(Memory.CompareAvx256(ref s2[0], ref s1[0], s1.Length)));
                            }
                        }
                        catch
                        {
                            Console.WriteLine($"{size} - {i}");
                            throw;
                        }
                    };

                    s2[i] = s1[i];
                }
            }
        }
    }
}
