using Raven.Server.Config;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Platform;
using Sparrow.Server.LowMemory;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues;

public class RavenDB_25578 : NoDisposalNeeded
{
    public RavenDB_25578(ITestOutputHelper output) : base(output)
    {
    }

    [RavenMultiplatformFact(RavenTestCategory.Core, RavenArchitecture.AllX64)]
    public void ForceUsing32BitsPager_should_flow_into_DatabaseConfiguration_and_TransactionMergerConfiguration_defaults()
    {
        var server = RavenConfiguration.CreateForTesting("server", ResourceType.Server);

        server.SetSetting(RavenConfiguration.GetKey(x => x.Storage.ForceUsing32BitsPager), "true");

        server.Initialize();

        Assert.True(server.Storage.ForceUsing32BitsPager);

        // DatabaseConfiguration(forceUsing32BitsPager) should pick the 32-bit defaults
        Assert.Equal(new Size(16, SizeUnit.Megabytes), server.Databases.PulseReadTransactionLimit);

        // TransactionMergerConfiguration(forceUsing32BitsPager) should pick the 32-bit defaults
        Assert.Equal(new Size(4, SizeUnit.Megabytes), server.TransactionMergerConfiguration.MaxTxSize);
    }

    [RavenMultiplatformFact(RavenTestCategory.Core, RavenArchitecture.AllX64)]
    public void ForceUsing32BitsPager_false_should_use_64bit_dependent_defaults()
    {
        Assert.False(PlatformDetails.Is32Bits);

        var server = RavenConfiguration.CreateForTesting("server", ResourceType.Server);

        server.SetSetting(RavenConfiguration.GetKey(x => x.Storage.ForceUsing32BitsPager), "false");

        server.Initialize();

        Assert.False(server.Storage.ForceUsing32BitsPager);

        var totalMem = MemoryInformation.TotalPhysicalMemory;

        Size expectedPulseReadTransactionLimit;
        if (totalMem <= new Size(1, SizeUnit.Gigabytes))
            expectedPulseReadTransactionLimit = new Size(16, SizeUnit.Megabytes);
        else if (totalMem <= new Size(4, SizeUnit.Gigabytes))
            expectedPulseReadTransactionLimit = new Size(32, SizeUnit.Megabytes);
        else if (totalMem <= new Size(16, SizeUnit.Gigabytes))
            expectedPulseReadTransactionLimit = new Size(64, SizeUnit.Megabytes);
        else if (totalMem <= new Size(64, SizeUnit.Gigabytes))
            expectedPulseReadTransactionLimit = new Size(128, SizeUnit.Megabytes);
        else
            expectedPulseReadTransactionLimit = new Size(256, SizeUnit.Megabytes);

        var expectedMaxTxSize = Size.Min(
            new Size(512, SizeUnit.Megabytes),
            totalMem / 10);

        Assert.Equal(expectedPulseReadTransactionLimit, server.Databases.PulseReadTransactionLimit);
        Assert.Equal(expectedMaxTxSize, server.TransactionMergerConfiguration.MaxTxSize);
        Assert.NotEqual(new Size(4, SizeUnit.Megabytes), server.TransactionMergerConfiguration.MaxTxSize);
    }
}
