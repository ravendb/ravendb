using Sparrow.Binary;
using Xunit;

namespace FastTests.Sparrow
{
    public class BitsTests : NoDisposalNeeded
    {
        [Fact]
        public void Bits_MostSignificativeBit()
        {
            Assert.Equal(0, Bits.MostSignificantBit(0));
            Assert.Equal(0, Bits.MostSignificantBit(1));
            Assert.Equal(1, Bits.MostSignificantBit(2));
            Assert.Equal(1, Bits.MostSignificantBit(3));
            Assert.Equal(2, Bits.MostSignificantBit(4));
            Assert.Equal(15, Bits.MostSignificantBit(0x0000FF00));
            Assert.Equal(30, Bits.MostSignificantBit(0x40B79DF0));

            Assert.Equal(0, Bits.MostSignificantBit((uint)0));
            Assert.Equal(0, Bits.MostSignificantBit((uint)1));
            Assert.Equal(1, Bits.MostSignificantBit((uint)2));
            Assert.Equal(1, Bits.MostSignificantBit((uint)3));
            Assert.Equal(2, Bits.MostSignificantBit((uint)4));
            Assert.Equal(15, Bits.MostSignificantBit((uint)0x0000FF00));
            Assert.Equal(31, Bits.MostSignificantBit((uint)0xFFFF0000));
            Assert.Equal(30, Bits.MostSignificantBit((uint)0x40B79DF0));

            Assert.Equal(0, Bits.MostSignificantBit((long)0));
            Assert.Equal(0, Bits.MostSignificantBit((long)1));
            Assert.Equal(1, Bits.MostSignificantBit((long)2));
            Assert.Equal(1, Bits.MostSignificantBit((long)3));
            Assert.Equal(2, Bits.MostSignificantBit((long)4));
            Assert.Equal(15, Bits.MostSignificantBit((long)0x0000FF00));
            Assert.Equal(31, Bits.MostSignificantBit((long)0xFFFF0000));
            Assert.Equal(30, Bits.MostSignificantBit((long)0x40B79DF0));
            Assert.Equal(62, Bits.MostSignificantBit((long)0x40B79DF0FFFF0000));

            Assert.Equal(0, Bits.MostSignificantBit((ulong)0));
            Assert.Equal(0, Bits.MostSignificantBit((ulong)1));
            Assert.Equal(1, Bits.MostSignificantBit((ulong)2));
            Assert.Equal(1, Bits.MostSignificantBit((ulong)3));
            Assert.Equal(2, Bits.MostSignificantBit((ulong)4));
            Assert.Equal(15, Bits.MostSignificantBit((ulong)0x0000FF00));
            Assert.Equal(31, Bits.MostSignificantBit((ulong)0xFFFF0000));
            Assert.Equal(30, Bits.MostSignificantBit((ulong)0x40B79DF0));
            Assert.Equal(62, Bits.MostSignificantBit((ulong)0x40B79DF0FFFF0000));

        }

        [Fact]
        public void Bits_LeadingZeroes()
        {
            Assert.Equal(32, Bits.LeadingZeroes(0));
            Assert.Equal(31, Bits.LeadingZeroes(1));
            Assert.Equal(30, Bits.LeadingZeroes(2));
            Assert.Equal(30, Bits.LeadingZeroes(3));
            Assert.Equal(29, Bits.LeadingZeroes(4));
            Assert.Equal(16, Bits.LeadingZeroes(0x0000FF00));
            Assert.Equal(1, Bits.LeadingZeroes(0x40B79DF0));

            Assert.Equal(32, Bits.LeadingZeroes((uint)0));
            Assert.Equal(31, Bits.LeadingZeroes((uint)1));
            Assert.Equal(30, Bits.LeadingZeroes((uint)2));
            Assert.Equal(30, Bits.LeadingZeroes((uint)3));
            Assert.Equal(29, Bits.LeadingZeroes((uint)4));
            Assert.Equal(16, Bits.LeadingZeroes((uint)0x0000FF00));
            Assert.Equal(1, Bits.LeadingZeroes((uint)0x40B79DF0));
            Assert.Equal(0, Bits.LeadingZeroes((uint)0xFFFF0000));
        }

        [Fact]
        public void Bits_Ceil2Log()
        {
            Assert.Equal(0, Bits.CeilLog2(0));
            Assert.Equal(0, Bits.CeilLog2(1));
            Assert.Equal(1, Bits.CeilLog2(2));
            Assert.Equal(2, Bits.CeilLog2(3));
            Assert.Equal(2, Bits.CeilLog2(4));
            Assert.Equal(16, Bits.CeilLog2(0x0000FF00));
            Assert.Equal(31, Bits.CeilLog2(0x40B79DF0));

            Assert.Equal(0, Bits.CeilLog2((uint)0));
            Assert.Equal(0, Bits.CeilLog2((uint)1));
            Assert.Equal(1, Bits.CeilLog2((uint)2));
            Assert.Equal(2, Bits.CeilLog2((uint)3));
            Assert.Equal(2, Bits.CeilLog2((uint)4));
            Assert.Equal(16, Bits.CeilLog2((uint)0x0000FF00));
            Assert.Equal(32, Bits.CeilLog2((uint)0xFFFF0000));
            Assert.Equal(31, Bits.CeilLog2((uint)0x40B79DF0));
        }

        [Fact]
        public void Bits_NextPowerOf2()
        {
            for (int i = 1; i < 31; i++)
            {
                int v = 1 << i;
                Assert.Equal(v << 1, Bits.PowerOf2(v + 1));
            }

            for (int i = 1; i < 62; i++)
            {
                long v = 1L << i;
                Assert.Equal(v << 1, Bits.PowerOf2(v + 1));
            }
        }


        [Fact]
        public void Bits_PowerOf2Fixed()
        {
            Assert.Equal(1, Bits.PowerOf2(1));
            Assert.Equal(2, Bits.PowerOf2(2));
            Assert.Equal(4, Bits.PowerOf2(3));
            Assert.Equal(4, Bits.PowerOf2(4));
            Assert.Equal(256, Bits.PowerOf2(129));
            Assert.Equal(256, Bits.PowerOf2(255));

            Assert.Equal(1, Bits.PowerOf2((long)1));
            Assert.Equal(2, Bits.PowerOf2((long)2));
            Assert.Equal(4, Bits.PowerOf2((long)3));
            Assert.Equal(4, Bits.PowerOf2((long)4));
            Assert.Equal(256, Bits.PowerOf2((long)129));
            Assert.Equal(256, Bits.PowerOf2((long)255));
        }

        [Fact]
        public void Bits_TrailingAndLeadingZeroes()
        {
            long number = 210;

            Assert.Equal(56 - 32, Bits.LeadingZeroes((int)number));
            Assert.Equal(0, Bits.TrailingZeroesInBytes((int)number));

            Assert.Equal(56 - 32, Bits.LeadingZeroes((uint)number));
            Assert.Equal(0, Bits.TrailingZeroesInBytes((uint)number));

            Assert.Equal(56, Bits.LeadingZeroes(number));
            Assert.Equal(0, Bits.TrailingZeroesInBytes(number));

            Assert.Equal(56, Bits.LeadingZeroes((ulong)number));
            Assert.Equal(0, Bits.TrailingZeroesInBytes((ulong)number));

            // Binary Representation: ‭0110 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000‬
            // Hexadecimal Representation: 0x‭6000000000000000‬
            number = 6917529027641081856;

            Assert.Equal(1, Bits.LeadingZeroes(number));
            Assert.Equal(7, Bits.TrailingZeroesInBytes(number));

            Assert.Equal(1, Bits.LeadingZeroes((ulong)number));
            Assert.Equal(7, Bits.TrailingZeroesInBytes((ulong)number));

            number = 170;

            Assert.Equal(0, Bits.TrailingZeroesInBytes((int)number));
            Assert.Equal(0, Bits.TrailingZeroesInBytes((uint)number));
            Assert.Equal(0, Bits.TrailingZeroesInBytes(number));
            Assert.Equal(0, Bits.TrailingZeroesInBytes((ulong)number));
        }
    }
}
