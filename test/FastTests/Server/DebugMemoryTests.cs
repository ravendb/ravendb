using System;
using Raven.Server.EventListener;
using Raven.Server.Web.System;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server;

public class DebugMemoryTests : NoDisposalNeeded
{
    public DebugMemoryTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void Allocation_Debug_Event()
    {
        Assert.True(Environment.Version.Major == 8 && AllocationsHandler.AllocationEventName == "GCAllocationTick_V4",
            "Check if GCAllocationTick event was updated: https://github.com/dotnet/runtime/blob/main/src/coreclr/gc/gcevents.h");
    }
}
