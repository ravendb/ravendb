using System;
using System.Runtime.Intrinsics.X86;
using Sparrow.Server.Strings;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public unsafe class StringTests : NoDisposalNeeded
    {
        public StringTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ExhaustiveCompareForEveryCombination()
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

                        Assert.False(StringsExtensions.CompareConstant(s2Span, s1Ptr, size));
                        Assert.False(StringsExtensions.CompareConstantVector128(ref s2Span[0], s1Ptr, size));

                        if (Avx2.IsSupported)
                        {
                            Assert.False(StringsExtensions.CompareConstantAvx2(ref s2Span[0], s1Ptr, size));
                        }

                        // We reset the state to zero
                        s1Ptr[i] = 0x00;
                        s2[i] = 0x00;


                        Assert.True(StringsExtensions.CompareConstant(s2Span, s1Ptr, size));
                        Assert.True(StringsExtensions.CompareConstantVector128(ref s2Span[0], s1Ptr, size));

                        if (Avx2.IsSupported)
                        {
                            Assert.True(StringsExtensions.CompareConstantAvx2(ref s2Span[0], s1Ptr, size));
                        }
                    };
                }
            }
        }
    }
}
