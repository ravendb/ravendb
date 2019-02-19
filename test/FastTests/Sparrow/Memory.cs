using Sparrow;
using Xunit;

namespace FastTests.Sparrow
{
    public unsafe class MemoryTests : NoDisposalNeeded
    {
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

                    // We reset the state to zero
                    s1Ptr[i] = 0x00;
                    s2Ptr[i] = 0x00;

                    Assert.True(Memory.CompareInline(s1Ptr, s2Ptr, s1.Length) == 0);
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
                        // We set the particular place to fit
                        s1Ptr[i] = 0x10;
                        s2Ptr[i] = 0x01;

                        Assert.True(Memory.CompareInline(s1Ptr, s2Ptr, s1.Length) > 0);
                        Assert.True(Memory.CompareInline(s2Ptr, s1Ptr, s1.Length) < 0);

                        // We reset the state to zero
                        s1Ptr[i] = 0x00;
                        s2Ptr[i] = 0x00;

                        Assert.True(Memory.CompareInline(s1Ptr, s2Ptr, s1.Length) == 0);
                    };
                }

            }
        }

        [Fact]
        public void IncreasingSizeForLoop()
        {
            for (int size = 1; size < 1024; size++)
            {
                var s1 = new byte[size];
                var s2 = new byte[size];

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

                        // We reset the state to zero
                        s1Ptr[i] = 0x00;
                        s2Ptr[i] = 0x00;

                        Assert.True(Memory.CompareInline(s1Ptr, s2Ptr, s1.Length) == 0);
                    };
                }

            }
        }

    }
}
