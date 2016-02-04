using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Sparrow.Tests
{
    public class BitsTests
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
                Assert.Equal(v << 1, Bits.NextPowerOf2(v + 1));
            }

            for (int i = 1; i < 62; i++)
            {
                long v = 1L << i;
                Assert.Equal(v << 1, Bits.NextPowerOf2(v + 1));
            }
        }
    }
}
