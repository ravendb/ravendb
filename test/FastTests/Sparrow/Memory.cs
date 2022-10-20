using System;
using System.Runtime.Intrinsics.X86;
using Sparrow;
using Sparrow.Server;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public unsafe class MemoryTests : NoDisposalNeeded
    {
        public MemoryTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void LongRoundedSize()
        {
            var s1 = new byte[8] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
            var s2 = new byte[8] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };

            for (int i = 0; i < s1.Length; i++)
            {
                fixed (byte* s1Ptr = s1)
                fixed (byte* s2Ptr = s2)
                {
                    // We set the particular place to fit
                    s1Ptr[i] = 0x10;
                    s2Ptr[i] = 0x01;

                    Assert.True(Memory.CompareInline(s1Ptr, s2Ptr, s1.Length) > 0);
                    Assert.True(Memory.CompareInline(s2Ptr, s1Ptr, s1.Length) < 0);

                    Assert.True(AdvMemory.CompareSmall(s1Ptr, s2Ptr, s1.Length) > 0);
                    Assert.True(AdvMemory.CompareSmall(s2Ptr, s1Ptr, s1.Length) < 0);

                    // We reset the state to zero
                    s1Ptr[i] = 0x00;
                    s2Ptr[i] = 0x00;

                    Assert.True(Memory.CompareInline(s1Ptr, s2Ptr, s1.Length) == 0);
                    Assert.True(AdvMemory.CompareSmall(s1Ptr, s2Ptr, s1.Length) == 0);
                };
            }
        }

        [Fact]
        public void SmallerThanBigLoop()
        {
            for (int size = 1; size < 8; size++)
            {
                var s1 = new byte[size];
                var s2 = new byte[size];

                for (int i = 0; i < s1.Length; i++)
                {
                    fixed (byte* s1Ptr = s1)
                    fixed (byte* s2Ptr = s2)
                    {
                        s1[i] = 0x10;
                        s2[i] = 0x01;

                        // We set the particular place to fit
                        TestCompatibilityDifference(s1Ptr, s2Ptr, s1.Length);

                        // We reset the state to zero
                        s1Ptr[i] = 0x00;
                        s2Ptr[i] = 0x00;

                        // We set the particular place to fit
                        TestCompatibilityEquality(s1Ptr, s2Ptr, s1.Length);
                    };
                }

            }
        }

        [Theory]
        [InlineData(6, 5)]
        [InlineData(33, 32)]
        public void TestCompatibility(int size, int i)
        {
            var s1 = new byte[size];
            var s2 = new byte[size];

            fixed (byte* s1Ptr = s1)
            fixed (byte* s2Ptr = s2)
            {
                s1[i] = 0x10;
                s2[i] = 0x01;

                // We set the particular place to fit
                TestCompatibilityDifference(s1Ptr, s2Ptr, s1.Length);

                // We reset the state to zero
                s1Ptr[i] = 0x00;
                s2Ptr[i] = 0x00;

                // We set the particular place to fit
                TestCompatibilityEquality(s1Ptr, s2Ptr, s1.Length);
            };
        }

        private static void TestCompatibilityDifference(byte* s1Ptr, byte* s2Ptr, int length)
        {
            Assert.True(Memory.CompareInline(s1Ptr, s2Ptr, length) > 0);
            Assert.True(Memory.CompareInline(s2Ptr, s1Ptr, length) < 0);

            Assert.True(AdvMemory.CompareSmall(s1Ptr, s2Ptr, length) > 0);
            Assert.True(AdvMemory.CompareSmall(s2Ptr, s1Ptr, length) < 0);

            Assert.True(AdvMemory.CompareSmallInlineNet6OorLesser(s1Ptr, s2Ptr, length) > 0);
            Assert.True(AdvMemory.CompareSmallInlineNet6OorLesser(s2Ptr, s1Ptr, length) < 0);

            Assert.True(AdvMemory.CompareSmallInlineNet7(s1Ptr, s2Ptr, length) > 0);
            Assert.True(AdvMemory.CompareSmallInlineNet7(s2Ptr, s1Ptr, length) < 0);

            if (Avx2.IsSupported)
            {
                Assert.True(AdvMemory.CompareAvx2(s1Ptr, s2Ptr, length) > 0);
                Assert.True(AdvMemory.CompareAvx2(s2Ptr, s1Ptr, length) < 0);
            }
        }

        private static void TestCompatibilityEquality(byte* s1Ptr, byte* s2Ptr, int length)
        {
            Assert.True(Memory.CompareInline(s1Ptr, s2Ptr, length) == 0);
            Assert.True(AdvMemory.CompareSmall(s1Ptr, s2Ptr, length) == 0);
            Assert.True(AdvMemory.CompareSmallInlineNet6OorLesser(s1Ptr, s2Ptr, length) == 0);
            Assert.True(AdvMemory.CompareSmallInlineNet7(s1Ptr, s2Ptr, length) == 0);

            if (Avx2.IsSupported)
            {
                Assert.True(AdvMemory.CompareAvx2(s1Ptr, s2Ptr, length) == 0);
            }
        }

        [Fact]
        public void IncreasingSizeForLoop()
        {
            for (int size = 0; size < 1024; size++)
            {
                var s1 = new byte[size];
                var s2 = new byte[size];

                for (int i = 0; i < s1.Length; i++)
                {
                    fixed (byte* s1Ptr = s1)
                    fixed (byte* s2Ptr = s2)
                    {
                        try
                        {
                            // We set the particular place to fit
                            s1Ptr[i] = 0x10;
                            s2Ptr[i] = 0x01;

                            Assert.True(Memory.CompareInline(s1Ptr, s2Ptr, s1.Length) > 0);
                            Assert.True(Memory.CompareInline(s2Ptr, s1Ptr, s1.Length) < 0);

                            Assert.True(AdvMemory.CompareSmall(s1Ptr, s2Ptr, s1.Length) > 0);
                            Assert.True(AdvMemory.CompareSmall(s2Ptr, s1Ptr, s1.Length) < 0);

                            Assert.True(AdvMemory.CompareSmallInlineNet6OorLesser(s1Ptr, s2Ptr, s1.Length) > 0);
                            Assert.True(AdvMemory.CompareSmallInlineNet6OorLesser(s2Ptr, s1Ptr, s1.Length) < 0);

                            Assert.True(AdvMemory.CompareSmallInlineNet7(s1Ptr, s2Ptr, s1.Length) > 0);
                            Assert.True(AdvMemory.CompareSmallInlineNet7(s2Ptr, s1Ptr, s1.Length) < 0);

                            if (Avx2.IsSupported)
                            {
                                Assert.True(AdvMemory.CompareAvx2(s1Ptr, s2Ptr, s1.Length) > 0);
                                Assert.True(AdvMemory.CompareAvx2(s2Ptr, s1Ptr, s1.Length) < 0);
                            }

                            // We reset the state to zero
                            s1Ptr[i] = 0x00;
                            s2Ptr[i] = 0x00;

                            Assert.True(Memory.CompareInline(s1Ptr, s2Ptr, s1.Length) == 0);
                            Assert.True(AdvMemory.CompareSmall(s1Ptr, s2Ptr, s1.Length) == 0);
                            Assert.True(AdvMemory.CompareSmallInlineNet6OorLesser(s1Ptr, s2Ptr, s1.Length) == 0);
                            Assert.True(AdvMemory.CompareSmallInlineNet7(s1Ptr, s2Ptr, s1.Length) == 0);

                            if (Avx2.IsSupported)
                            {
                                Assert.True(AdvMemory.CompareAvx2(s1Ptr, s2Ptr, s1.Length) == 0);
                            }
                        }
                        catch
                        {
                            Console.WriteLine($"{size} - {i}");
                            throw;
                        }

                    };
                }
            }
        }
    }
}
