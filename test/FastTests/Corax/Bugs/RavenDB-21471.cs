using System;
using System.Runtime.InteropServices;
using Corax.Queries.Meta;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs
{
    public class RavenDB_21471 : RavenTestBase
    {
        public RavenDB_21471(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void VectorizedAndWillNotAccessMemoryOutOfRange()
        {
            const int size = 6;
            var buffer = new byte[3 * sizeof(long) * size];
            var longBuffer = MemoryMarshal.Cast<byte, long>(buffer);

            var left = longBuffer.Slice(size, size);
            left[0] = 5708;
            left[1] = 5709;

            var right = longBuffer.Slice(0, size);
            (new long[] {763, 764, 941, 942, 946, 966}).AsSpan().CopyTo(right);
            var outputBuffer = longBuffer.Slice(2 * size, size);

            var common = MergeHelper.And(outputBuffer, left.Slice(0,2), right);
            
            Assert.Equal(0, common);
        }
    }
}
