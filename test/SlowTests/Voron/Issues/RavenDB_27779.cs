using System;
using FastTests.Voron;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Sparrow.Server.Platform;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Voron.Issues;

public class RavenDB_27779 : StorageTest
{
    public RavenDB_27779(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void StorageConfigurationIsAppliedToPalBeforeFirstEnvironment()
    {
        var expected = RavenConfiguration.CreateForTesting("Tests", ResourceType.Server, skipEnvironmentVariables: false);
        expected.Initialize();

        GC.KeepAlive(Env); // may be the first native call in the process

        Assert.Equal(expected.Storage.IoRingQueueSize, PalConfiguration.IoRingQueueSize);
        Assert.Equal(expected.Storage.LowPriorityFlushAndSync, PalConfiguration.LowPriorityIo);

        // for Auto the PAL writes the resolved mode back
        if (expected.Storage.WriteMode != Pal.RvnWriteMode.Auto)
            Assert.Equal(expected.Storage.WriteMode, PalConfiguration.WriteMode);
    }
}
