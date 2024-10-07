using System;
using Raven.Server.EventListener;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server;

public class DebugMemoryTests : NoDisposalNeeded
{
    public DebugMemoryTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Core)]
    public void Debug_Events()
    {
        Assert.True(Environment.Version.Major == 9 && EventListener.Constants.EventNames.GC.GCStart == "GCStart_V2",
            "Check if GCStart event was updated: https://github.com/dotnet/runtime/blob/main/src/coreclr/gc/gcevents.h");

        Assert.True(Environment.Version.Major == 9 && EventListener.Constants.EventNames.GC.GCEnd == "GCEnd_V1",
            "Check if GCEnd event was updated: https://github.com/dotnet/runtime/blob/main/src/coreclr/gc/gcevents.h");

        Assert.True(Environment.Version.Major == 9 && EventListener.Constants.EventNames.GC.GCSuspendBegin == "GCSuspendEEBegin_V1",
            "Check if GCSuspendBegin event was updated: https://github.com/dotnet/runtime/blob/main/src/coreclr/gc/gcevents.h");

        Assert.True(Environment.Version.Major == 9 && EventListener.Constants.EventNames.GC.GCSuspendEnd == "GCSuspendEEEnd_V1",
            "Check if GCSuspendEnd event was updated: https://github.com/dotnet/runtime/blob/main/src/coreclr/gc/gcevents.h");

        Assert.True(Environment.Version.Major == 9 && EventListener.Constants.EventNames.GC.GCRestartBegin == "GCRestartEEBegin_V1",
            "Check if GCRestartBegin event was updated: https://github.com/dotnet/runtime/blob/main/src/coreclr/gc/gcevents.h");

        Assert.True(Environment.Version.Major == 9 && EventListener.Constants.EventNames.GC.GCRestartEnd == "GCRestartEEEnd_V1",
            "Check if GCRestartEnd event was updated: https://github.com/dotnet/runtime/blob/main/src/coreclr/gc/gcevents.h");

        Assert.True(Environment.Version.Major == 9 && EventListener.Constants.EventNames.GC.GCFinalizersBegin == "GCFinalizersBegin_V1",
            "Check if GCFinalizersBegin event was updated: https://github.com/dotnet/runtime/blob/main/src/coreclr/gc/gcevents.h");

        Assert.True(Environment.Version.Major == 9 && EventListener.Constants.EventNames.GC.GCFinalizersEnd == "GCFinalizersEnd_V1",
            "Check if GCFinalizersEnd event was updated: https://github.com/dotnet/runtime/blob/main/src/coreclr/gc/gcevents.h");

        Assert.True(Environment.Version.Major == 9 && EventListener.Constants.EventNames.Allocations.Allocation == "GCAllocationTick_V4",
            "Check if Allocation event was updated: https://github.com/dotnet/runtime/blob/main/src/coreclr/gc/gcevents.h");

        Assert.True(Environment.Version.Major == 9 && EventListener.Constants.EventNames.Contention.ContentionStart == "ContentionStart",
            "Check if ContentionStart event was updated: https://github.com/dotnet/runtime/blob/main/src/coreclr/gc/gcevents.h");

        Assert.True(Environment.Version.Major == 9 && EventListener.Constants.EventNames.Contention.ContentionStop == "ContentionStop",
            "Check if ContentionStop event was updated: https://github.com/dotnet/runtime/blob/main/src/coreclr/gc/gcevents.h");
    }
}
