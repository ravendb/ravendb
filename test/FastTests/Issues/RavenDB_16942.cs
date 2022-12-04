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
                // ByteStringContext uses ByteStringMemoryCache as an allocator, which means that
                // internalCurrent and externalCurrent might be larger than requested 4096 because
                // ByteStringMemoryCache uses a static pool so other tests might affect the actual size here
                var initialTotalAllocated = allocator._totalAllocated;
                var previousTotalAllocated = initialTotalAllocated;

                var size = 128 * 1024;
                for (var i = 0; i < 100; i++)
                {
                    using (allocator.Allocate(size, out var buffer))
                    {
                        Assert.Equal(allocator._currentlyAllocated, buffer.Size);
                    }

                    Assert.Equal(0, allocator._currentlyAllocated);
                    Assert.True(allocator._totalAllocated > 0);

                    if (previousTotalAllocated != allocator._totalAllocated)
                        Output.WriteLine($"{nameof(ByteStringContext_Should_Reuse_When_Large_Allocations_Are_Requested)} {i}: P: {previousTotalAllocated}: C: {allocator._totalAllocated}");

                    previousTotalAllocated = allocator._totalAllocated;
                }

                Assert.True(allocator._totalAllocated - initialTotalAllocated > 0);
                Assert.True(allocator._totalAllocated - initialTotalAllocated < 1024 * 1024, $"{allocator._totalAllocated} < 1024 * 1024");
            }
        }
    }
}
