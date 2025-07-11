using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_16942 : NoDisposalNeeded
    {
        public RavenDB_16942(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public void ByteStringContext_Should_Reuse_When_Large_Allocations_Are_Requested()
        {
            using (var allocator = new ByteStringContext(SharedMultipleUseFlag.None))
            {
                // ByteStringContext uses ByteStringMemoryCache as an allocator, which means that
                // internalCurrent and externalCurrent might be larger than requested 4096 because
                // ByteStringMemoryCache uses a static pool so other tests might affect the actual size here
                var initialTotalAllocated = allocator._totalAllocated;

                // We may have concurrency, such as the following line:
                // which adds to the cache a large segment
                //using (var concurrent = new ByteStringContext(SharedMultipleUseFlag.None))
                //{
                //    concurrent.Allocate(1024 * 512 - 10, out var buffer);
                //}

                var size = 128 * 1024;
                // allocate once, to "capture" a segment, and then we should be static 
                // in term of allocations
                using (allocator.Allocate(size, out var buffer))
                {
                    Assert.Equal(allocator._currentlyAllocated, buffer.Size);
                }
                var previousTotalAllocated = allocator._totalAllocated;

                for (var i = 0; i < 100; i++)
                {
                    using (allocator.Allocate(size, out var buffer))
                    {
                        Assert.Equal(allocator._currentlyAllocated, buffer.Size);
                    }

                    Assert.Equal(0, allocator._currentlyAllocated);
                    Assert.Equal(previousTotalAllocated, allocator._totalAllocated);
                }

                Assert.True(allocator._totalAllocated - initialTotalAllocated > 0);
            }
        }
    }
}

