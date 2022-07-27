using System;
using Sparrow.Server;
using Sparrow.Threading;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_16942 : NoDisposalNeeded
    {
        public RavenDB_16942(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ByteStringContext_Should_Reuse_When_Large_Allocations_Are_Requested()
        {
            using (var allocator = new ByteStringContext(SharedMultipleUseFlag.None))
            {
                var size = 128 * 1024;
                for (var i = 0; i < 100; i++)
                {
                    using (allocator.Allocate(size, out var buffer))
                    {
                        Assert.Equal(allocator._currentlyAllocated, buffer.Size);
                    }

                    Assert.Equal(0, allocator._currentlyAllocated);
                    Assert.True(allocator._totalAllocated > 0);
                }

                Assert.True(allocator._totalAllocated > 0);
                Assert.True(allocator._totalAllocated < 1024 * 1024, $"{allocator._totalAllocated} < 1024 * 1024");
            }
        }
    }
}
