using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace FastTests.Sparrow
{
    public unsafe class PtrBitVectorTests
    {
        [Fact(Skip = "We will skip compatibility for now, but we want this to happen before RTM")]
        public void Compatibility()
        {
            var original = BitVector.Of(0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00);
            fixed (ulong* storage = original.Bits)
            {
                var ptr = new PtrBitVector((byte*)storage, 64);
                for (int i = 0; i < 64; i++)
                    Assert.Equal(original[i], ptr[i]);
            };
        }


        [Fact]
        public void SetIndex()
        {
            var original = new byte[8] { 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00 };
            fixed (byte* storage = original)
            {
                var ptr = new PtrBitVector(storage, 64);
                int idx = ptr.FindLeadingOne();
                Assert.Equal(false, ptr[idx - 1]);
                Assert.Equal(true, ptr[idx]);
                Assert.Equal(false, ptr[idx + 1]);

                ptr.Set(idx, false);
                ptr.Set(idx+1, true);

                Assert.Equal(false, ptr[idx - 1]);
                Assert.Equal(false, ptr[idx]);
                Assert.Equal(true, ptr[idx + 1]);
            };
        }
    }
}
