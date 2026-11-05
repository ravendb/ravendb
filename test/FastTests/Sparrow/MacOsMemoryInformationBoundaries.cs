using System;
using Tests.Infrastructure;
using Xunit;
using MemoryInformation = global::Sparrow.Server.LowMemory.MemoryInformation;

namespace FastTests.Sparrow;

public class MacOsMemoryInformationBoundaries(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Core)]
    public void AvailableMemoryOfZeroPagesIsZero()
    {
        Assert.Equal(0, MemoryInformation.MacOsAvailableMemoryInBytes(0, 0, 0, 0, 16384));
        Assert.Equal(0, MemoryInformation.MacOsCommittedMemoryInBytes(0, 0, 16384, 0, 0));
    }

    [RavenTheory(RavenTestCategory.Core)]
    [InlineData(4096UL)]
    [InlineData(16384UL)]
    public void AvailableMemoryJustBelowAndAboveFourGigabytes(ulong pageSize)
    {
        var pagesForFourGb = (uint)(4UL * 1024 * 1024 * 1024 / pageSize);

        Assert.Equal(4L * 1024 * 1024 * 1024 - (long)pageSize, MemoryInformation.MacOsAvailableMemoryInBytes(pagesForFourGb - 1, 0, 0, 0, pageSize));
        // exactly 2^32 bytes is where a 32 bit multiplication wraps to zero
        Assert.Equal(4L * 1024 * 1024 * 1024, MemoryInformation.MacOsAvailableMemoryInBytes(pagesForFourGb, 0, 0, 0, pageSize));
        Assert.Equal(4L * 1024 * 1024 * 1024 + (long)pageSize, MemoryInformation.MacOsAvailableMemoryInBytes(pagesForFourGb + 1, 0, 0, 0, pageSize));
    }

    [RavenFact(RavenTestCategory.Core)]
    public void SumOfThePageCountsDoesNotOverflowWhenEachCountIsNearTheMaximum()
    {
        const ulong pageSize = 16384;
        var available = MemoryInformation.MacOsAvailableMemoryInBytes(uint.MaxValue, uint.MaxValue, uint.MaxValue, uint.MaxValue, pageSize);
        Assert.Equal(4L * uint.MaxValue * 16384, available);

        var committed = MemoryInformation.MacOsCommittedMemoryInBytes(uint.MaxValue, uint.MaxValue, pageSize, 0, 0);
        Assert.Equal(2L * uint.MaxValue * 16384, committed);
    }

    [RavenFact(RavenTestCategory.Core)]
    public void EachCountIsWidenedBeforeTheyAreAdded()
    {
        // uint.MaxValue + 1 would be 0 in 32 bits
        var available = MemoryInformation.MacOsAvailableMemoryInBytes(uint.MaxValue, 1, 0, 0, 1);
        Assert.Equal(uint.MaxValue + 1L, available);

        var committed = MemoryInformation.MacOsCommittedMemoryInBytes(uint.MaxValue, 1, 1, 0, 0);
        Assert.Equal(uint.MaxValue + 1L, committed);
    }

    [RavenFact(RavenTestCategory.Core)]
    public void TerabyteScaleMachinesAreReportedCorrectly()
    {
        const ulong pageSize = 16384;
        // 1 TB of free pages
        const uint pages = (uint)(1024UL * 1024 * 1024 * 1024 / pageSize);
        Assert.Equal(1024L * 1024 * 1024 * 1024, MemoryInformation.MacOsAvailableMemoryInBytes(pages, 0, 0, 0, pageSize));
    }

    [RavenFact(RavenTestCategory.Core)]
    public void CommittedMemoryAddsCompressedMemoryAndSwapOnTopOfThePagesWithoutOverflow()
    {
        const ulong pageSize = 16384;
        const ulong sixGb = 6UL * 1024 * 1024 * 1024;

        var committed = MemoryInformation.MacOsCommittedMemoryInBytes(wiredPages: 0, activePages: 0, pageSize, compressedMemoryInBytes: sixGb, swapUsedInBytes: sixGb);
        Assert.Equal(12L * 1024 * 1024 * 1024, committed);

        committed = MemoryInformation.MacOsCommittedMemoryInBytes(wiredPages: 300_000, activePages: 300_000, pageSize, compressedMemoryInBytes: sixGb, swapUsedInBytes: 0);
        Assert.Equal(600_000L * 16384 + (long)sixGb, committed);
    }

    [RavenFact(RavenTestCategory.Core)]
    public void VmSizeTypeIsEightBytes()
    {
        // host_page_size writes a vm_size_t (8 bytes): a 4 byte parameter overflows the stack slot
        Assert.Equal(8, sizeof(ulong));
        var method = Type.GetType("Sparrow.Server.Platform.Posix.macOS.macSyscall, Sparrow.Server")?.GetMethod("host_page_size");
        Assert.NotNull(method);
        Assert.Equal(typeof(ulong).MakePointerType(), method.GetParameters()[1].ParameterType);
    }
}
