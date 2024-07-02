using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
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

                    Assert.True(Memory.CompareInline(s1Ptr, s2Ptr, s1.Length) > 0);
                    Assert.True(Memory.CompareInline(s2Ptr, s1Ptr, s1.Length) < 0);

                    // We reset the state to zero
                    s1Ptr[i] = 0x00;
                    s2Ptr[i] = 0x00;

                    Assert.True(Memory.CompareInline(s1Ptr, s2Ptr, s1.Length) == 0);
                    Assert.True(Memory.CompareInline(s1Ptr, s2Ptr, s1.Length) == 0);
                }
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
                    }

                    ;
                }

            }
        }


        [Fact]
        public void EnsureProperBehaviorWhenUnaligned()
        {
            var s1 = new byte[4 * Vector256<byte>.Count];
            var s2 = new byte[4 * Vector256<byte>.Count];

            fixed (byte* s1Fixed = s1)
            fixed (byte* s2Fixed = s2)
            {
                int alignment = 0;
                while ((long)(s1Fixed + alignment) % Vector256<byte>.Count != 0)
                    alignment++;

                byte* s1Ptr = s1Fixed + alignment;
                byte* s2Ptr = s2Fixed + alignment;

                int length = 2 * Vector256<byte>.Count;
                for (int size = 1; size < length; size++)
                {
                    for (int i = 0; i < length; i++)
                    {
                        s1Ptr[i] = 0x10;
                        s1Ptr[i] = 0x01;

                        // We set the particular place to fit
                        TestCompatibilityDifference(s1Ptr, s2Ptr, length);

                        // We reset the state to zero
                        s1Ptr[i] = 0x00;
                        s2Ptr[i] = 0x00;

                        // We set the particular place to fit
                        TestCompatibilityEquality(s1Ptr, s2Ptr, length);
                    }
                }
            }
        }

        [Theory]
        [InlineData(6, 5)]
        [InlineData(33, 32)]
        [InlineData(32, 0)]
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
            }

            ;
        }

        private static void TestCompatibilityDifference(byte* s1Ptr, byte* s2Ptr, int length)
        {
            Assert.True(Memory.CompareInline(s1Ptr, s2Ptr, length) > 0);
            Assert.True(Memory.CompareInline(s2Ptr, s1Ptr, length) < 0);

            Assert.True(Memory.CompareInlineNet6OorLesser(s1Ptr, s2Ptr, length) > 0);
            Assert.True(Memory.CompareInlineNet6OorLesser(s2Ptr, s1Ptr, length) < 0);

            Assert.True(Memory.CompareSmallInlineNet7(ref Unsafe.AsRef<byte>(s1Ptr), ref Unsafe.AsRef<byte>(s2Ptr), length) > 0);
            Assert.True(Memory.CompareSmallInlineNet7(ref Unsafe.AsRef<byte>(s2Ptr), ref Unsafe.AsRef<byte>(s1Ptr), length) < 0);

            if (AdvInstructionSet.X86.IsSupportedAvx256)
            {
                Assert.True(Memory.CompareAvx256(s1Ptr, s2Ptr, length) > 0);
                Assert.True(Memory.CompareAvx256(s2Ptr, s1Ptr, length) < 0);

                Assert.True(Memory.CompareAvx256(ref Unsafe.AsRef<byte>(s1Ptr), ref Unsafe.AsRef<byte>(s2Ptr), length) > 0);
                Assert.True(Memory.CompareAvx256(ref Unsafe.AsRef<byte>(s2Ptr), ref Unsafe.AsRef<byte>(s1Ptr), length) < 0);
            }
        }

        private static void TestCompatibilityEquality(byte* s1Ptr, byte* s2Ptr, int length)
        {
            Assert.True(Memory.CompareInline(s1Ptr, s2Ptr, length) == 0);
            Assert.True(Memory.CompareInlineNet6OorLesser(s1Ptr, s2Ptr, length) == 0);
            Assert.True(Memory.CompareSmallInlineNet7(ref Unsafe.AsRef<byte>(s1Ptr), ref Unsafe.AsRef<byte>(s2Ptr), length) == 0);

            if (AdvInstructionSet.X86.IsSupportedAvx256)
            {
                Assert.True(Memory.CompareAvx256(s1Ptr, s2Ptr, length) == 0);
                Assert.True(Memory.CompareAvx256(ref Unsafe.AsRef<byte>(s1Ptr), ref Unsafe.AsRef<byte>(s2Ptr), length) == 0);
            }
        }

        [Fact]
        public void TestFirstByteSmallerThanRest()
        {
            byte[] first = Convert.FromBase64String("DeVgA5+9xzBvKwAc8tdM0A==");
            byte[] second = Convert.FromBase64String("uOYOy45g3lYAOeWumaA=");
            var length = Math.Min(first.Length, second.Length);

            var reference = Math.Sign(first.AsSpan().SequenceCompareTo(second));

            fixed (byte* firstPtr = first)
            fixed (byte* secondPtr = second)
            {
                Assert.Equal(reference, Math.Sign(Memory.CompareInline(firstPtr, secondPtr, length)));
                Assert.Equal(reference, Math.Sign(Memory.CompareInlineNet6OorLesser(firstPtr, secondPtr, length)));

                if (AdvInstructionSet.X86.IsSupportedAvx256)
                {
                    Assert.Equal(reference, Math.Sign(Memory.CompareAvx256(firstPtr, secondPtr, length)));
                }
            }

            if (AdvInstructionSet.X86.IsSupportedAvx256)
                Assert.Equal(reference, Math.Sign(Memory.CompareAvx256(ref first[0], ref second[0], length)));

            Assert.Equal(reference, Math.Sign(Memory.CompareSmallInlineNet7(ref first[0], ref second[0], length)));
        }

        public static IEnumerable<object[]> RandomSeeds => new[] { new object[] { Random.Shared.Next() } };

        [Theory]
        [MemberData(nameof(RandomSeeds))]
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

                            Assert.True(Memory.CompareInline(s1Ptr, s2Ptr, s1.Length) > 0);
                            Assert.True(Memory.CompareInline(s2Ptr, s1Ptr, s1.Length) < 0);

                            Assert.True(Memory.CompareInlineNet6OorLesser(s1Ptr, s2Ptr, s1.Length) > 0);
                            Assert.True(Memory.CompareInlineNet6OorLesser(s2Ptr, s1Ptr, s1.Length) < 0);

                            Assert.True(Memory.CompareSmallInlineNet7(ref s1[0], ref s2[0], s1.Length) > 0);
                            Assert.True(Memory.CompareSmallInlineNet7(ref s2[0], ref s1[0], s1.Length) < 0);

                            if (AdvInstructionSet.X86.IsSupportedAvx256)
                            {
                                Assert.True(Memory.CompareAvx256(s1Ptr, s2Ptr, s1.Length) > 0);
                                Assert.True(Memory.CompareAvx256(s2Ptr, s1Ptr, s1.Length) < 0);

                                Assert.True(Memory.CompareAvx256(ref s1[0], ref s2[0], s1.Length) > 0);
                                Assert.True(Memory.CompareAvx256(ref s2[0], ref s1[0], s1.Length) < 0);
                            }

                            // We reset the state to zero
                            s1Ptr[i] = 0x00;
                            s2Ptr[i] = 0x00;

                            Assert.True(Memory.CompareInline(s1Ptr, s2Ptr, s1.Length) == 0, "Memory.CompareInline(s1Ptr, s2Ptr, s1.Length) == 0");
                            Assert.True(Memory.CompareInlineNet6OorLesser(s1Ptr, s2Ptr, s1.Length) == 0, "Memory.CompareInlineNet6OorLesser(s1Ptr, s2Ptr, s1.Length) == 0");
                            Assert.True(Memory.CompareSmallInlineNet7(ref s1[0], ref s2[0], s1.Length) == 0, "Memory.CompareSmallInlineNet7(ref s1[0], ref s2[0], s1.Length) == 0");

                            if (AdvInstructionSet.X86.IsSupportedAvx256)
                            {
                                Assert.True(Memory.CompareAvx256(s1Ptr, s2Ptr, s1.Length) == 0, "Memory.CompareAvx2(s1Ptr, s2Ptr, s1.Length) == 0");
                                Assert.True(Memory.CompareAvx256(ref s1[0], ref s2[0], s1.Length) == 0, "Memory.CompareAvx2(ref s1[0], ref s2[0], s1.Length) == 0");
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
