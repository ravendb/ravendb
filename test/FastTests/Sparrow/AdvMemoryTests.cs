using System;
using System.Runtime.Intrinsics.X86;
using Xunit;
using Xunit.Abstractions;
using Sparrow;
using Tests.Infrastructure;

namespace FastTests.Sparrow
{
    public unsafe class AdvMemoryTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.Core)]
        public void ExhaustiveIsEqualForEveryCombination()
        {
            for (int size = 1; size < 64; size++)
            {
                var s1 = new byte[size];
                var s2 = new byte[size];

                var s2Span = s2.AsSpan();

                for (int i = 1; i < s1.Length; i++)
                {
                    fixed (byte* s1Ptr = s1)
                    {
                        // We set the particular place to fit
                        s1Ptr[i] = 0x10;
                        s2[i] = 0x01;

                        Assert.False(Memory.IsEqualConstant(s2Span, s1Ptr, size));
                        Assert.False(Memory.IsEqualConstantVector128(ref s2Span[0], s1Ptr, size));

                        if (AdvInstructionSet.X86.IsSupportedAvx256)
                        {
                            Assert.False(Memory.IsEqualConstantAvx256(ref s2Span[0], s1Ptr, size));
                        }

                        // We reset the state to zero
                        s1Ptr[i] = 0x00;
                        s2[i] = 0x00;


                        Assert.True(Memory.IsEqualConstant(s2Span, s1Ptr, size));
                        Assert.True(Memory.IsEqualConstantVector128(ref s2Span[0], s1Ptr, size));

                        if (AdvInstructionSet.X86.IsSupportedAvx256)
                        {
                            Assert.True(Memory.IsEqualConstantAvx256(ref s2Span[0], s1Ptr, size));
                        }
                    };
                }
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void EnsureItThrowsWhenSizeIsNotSupported_WithoutTouchingTheParameters()
        {
            Assert.Throws<NotSupportedException>(() => Memory.IsEqualConstant(new byte[64], null));
            Assert.Throws<NotSupportedException>(() => Memory.IsEqualConstant(new byte[64], null, 0));
            Assert.Throws<NotSupportedException>(() => Memory.IsEqualConstant(null, 64, null));
        }
    }
}
