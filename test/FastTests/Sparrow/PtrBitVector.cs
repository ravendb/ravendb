using Sparrow.Binary;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public unsafe class PtrBitVectorTests : NoDisposalNeeded
    {
        public PtrBitVectorTests(ITestOutputHelper output) : base(output)
        {
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
                ptr.Set(idx + 1, true);

                Assert.Equal(false, ptr[idx - 1]);
                Assert.Equal(false, ptr[idx]);
                Assert.Equal(true, ptr[idx + 1]);
            };
        }
    }
}
