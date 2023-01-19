using Sparrow;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues;

public class RavenDB_19746 : NoDisposalNeeded
{
    public RavenDB_19746(ITestOutputHelper output) : base(output)
    {
    }

    [MultiplatformFact(RavenPlatform.Windows | RavenPlatform.Linux, RavenArchitecture.AllX64)]
    public void ShouldDefragment()
    {
        using var allocator = new ByteStringContext(new SharedMultipleUseFlag(), allocationBlockSize: 16384);

        for (var i = 0; i < 1000; i++)
        {
            var largeAllocationSizeInBytes = 16384 + i * 128;
            using (allocator.Allocate(largeAllocationSizeInBytes, out _))
            {
            }

            // fragment the memory
            for (var j = 0; j < 1024; j++)
            {
                using (allocator.Allocate(4096 + largeAllocationSizeInBytes / 1024, out _))
                {
                }
            }

            if (i % 5 == 0)
                allocator.DefragmentSegments(force: true);
        }

        var totalAllocated = new Size(allocator._totalAllocated, SizeUnit.Bytes); // it's 110 MB without defragmentation
        var maxTotalAllocated = new Size(10, SizeUnit.Megabytes);
        Assert.True(totalAllocated < maxTotalAllocated, $"{totalAllocated} < {maxTotalAllocated}");
        Assert.True(allocator.NumberOfReadyToUseMemorySegments < 300, $"{allocator.NumberOfReadyToUseMemorySegments} < 300"); // 14k segments without defragmentation
    }
}
