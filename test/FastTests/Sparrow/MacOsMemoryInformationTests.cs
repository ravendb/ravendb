using MemoryInformation = global::Sparrow.Server.LowMemory.MemoryInformation;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Sparrow;

public class MacOsMemoryInformationTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Core)]
    public void AvailableMemoryIsNotTruncatedTo32BitsOnMachinesWithMoreThanFourGigabytesOfReclaimableMemory()
    {
        // 16 KB pages (Apple silicon) with a lot of inactive pages
        const ulong pageSize = 16384;
        var available = MemoryInformation.MacOsAvailableMemoryInBytes(freePages: 9_000, inactivePages: 1_387_000, purgeablePages: 300, speculativePages: 100, pageSize);

        Assert.Equal((9_000L + 1_387_000 + 300 + 100) * 16384, available);
        Assert.True(available > uint.MaxValue);
    }

    [RavenFact(RavenTestCategory.Core)]
    public void CommittedMemoryIsNotTruncatedTo32Bits()
    {
        const ulong pageSize = 16384;
        var committed = MemoryInformation.MacOsCommittedMemoryInBytes(wiredPages: 380_000, activePages: 1_360_000, pageSize, compressedMemoryInBytes: 14_000_000_000, swapUsedInBytes: 1_000_000_000);

        Assert.Equal((380_000L + 1_360_000) * 16384 + 14_000_000_000 + 1_000_000_000, committed);
    }
}
