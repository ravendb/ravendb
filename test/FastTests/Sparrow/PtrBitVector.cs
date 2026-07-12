using System;
using Sparrow.Binary;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Sparrow
{
    public unsafe class PtrBitVectorTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenTheory(RavenTestCategory.Memory)]
        [InlineData(32)]
        [InlineData(64)]
        [InlineData(72)]   // crosses the 64-bit word boundary
        [InlineData(256)]
        public void FirstSetBit_IsConsistentWithTheIndexer(int count)
        {
            var buffer = new byte[(count + 7) / 8];
            fixed (byte* p = buffer)
            {
                var vec = new PtrBitVector(p, count);

                Assert.Equal(-1, vec.FirstSetBit());
                Assert.True(vec.AllEmpty());

                for (int idx = 0; idx < count; idx++)
                {
                    buffer.AsSpan().Clear();
                    vec.Set(idx, true);

                    Assert.Equal(idx, vec.FirstSetBit());
                    Assert.True(vec[vec.FirstSetBit()]);
                    Assert.False(vec.AllEmpty());
                }
            }
        }

        [RavenFact(RavenTestCategory.Memory)]
        public void FirstSetBit_ReturnsLowestSetIndex()
        {
            var buffer = new byte[8];
            fixed (byte* p = buffer)
            {
                var vec = new PtrBitVector(p, 64);
                vec.Set(40, true);
                vec.Set(5, true);
                vec.Set(63, true);

                Assert.Equal(5, vec.FirstSetBit());
            }
        }

        [RavenFact(RavenTestCategory.Memory)]
        public void FirstSetBit_IgnoresBitsBeyondCount()
        {
            // Count is not a multiple of 8, so byte 0 has padding bits (indices 5..7) beyond Count.
            var buffer = new byte[1];
            fixed (byte* p = buffer)
            {
                var vec = new PtrBitVector(p, 5);

                // Valid in-range bits are found (index 0 -> 0x80, index 4 -> 0x08).
                buffer[0] = 0x80;
                Assert.Equal(0, vec.FirstSetBit());
                buffer[0] = 0x08;
                Assert.Equal(4, vec.FirstSetBit());

                // A bit in the padding region (index 5 -> 0x04) is ignored: matches v7.2 FindLeadingOne.
                buffer[0] = 0x04;
                Assert.Equal(-1, vec.FirstSetBit());
                Assert.True(vec.AllEmpty());
            }
        }

        [RavenFact(RavenTestCategory.Memory)]
        public void FirstSetBit_IgnoresPaddingHandledByTheFastPathChunk()
        {
            // Count=60 -> lengthInBytes = ceil(60/8) = 8, a multiple of 8, so the last byte (padding indices
            // 60..63) is processed by the 8-byte fast-path loop, not the remainder. The clamp must apply there too.
            var buffer = new byte[8];
            fixed (byte* p = buffer)
            {
                var vec = new PtrBitVector(p, 60);

                // index 59 (byte 7, mask 0x10) is the highest valid bit -> found.
                buffer[7] = 0x10;
                Assert.Equal(59, vec.FirstSetBit());

                // index 61 (byte 7, mask 0x04) is padding (>= 60) -> ignored.
                buffer[7] = 0x04;
                Assert.Equal(-1, vec.FirstSetBit());
                Assert.True(vec.AllEmpty());
            }
        }
    }
}
