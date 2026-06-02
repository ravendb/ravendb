using System.Collections.Generic;
using System.IO;
using Raven.Server.Web.Studio;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues;

public class RavenDB_26704 : RavenTestBase
{
    public RavenDB_26704(ITestOutputHelper output) : base(output)
    {
    }

    // Regression test for the macOS DriveInfo.GetDrives() AccessViolationException (dotnet/runtime#122634,
    // fix dotnet/runtime#122637 not yet backported to .NET 10). On macOS the folder browser must NOT call
    // DriveInfo.GetDrives() (it reaches the racy, non-thread-safe getmntinfo()); it enumerates the filesystem
    // root plus the mounted volumes instead. We drive that logic with a temp "volumes" directory so the test
    // is deterministic. Gated to macOS so it only runs on the macOS CI agent, where the fix matters.
    [RavenMultiplatformFact(RavenTestCategory.Studio, RavenPlatform.OsX)]
    public void GetAvailableDrivesOnMacOs_ReturnsRootPlusMountedVolumes()
    {
        var volumesPath = NewDataPath(forceCreateDir: true);
        var volumeA = Directory.CreateDirectory(Path.Combine(volumesPath, "VolumeA")).FullName;
        var volumeB = Directory.CreateDirectory(Path.Combine(volumesPath, "VolumeB")).FullName;

        var drives = FolderPath.GetAvailableDrivesOnMacOs(volumesPath);

        Assert.Contains("/", drives);
        Assert.Contains(volumeA, drives);
        Assert.Contains(volumeB, drives);
        Assert.Equal(3, drives.Count);
    }

    [RavenMultiplatformFact(RavenTestCategory.Studio, RavenPlatform.OsX)]
    public void GetAvailableDrivesOnMacOs_WhenVolumesDirectoryMissing_ReturnsRootOnly()
    {
        var missing = Path.Combine(NewDataPath(forceCreateDir: true), "does-not-exist");

        var drives = FolderPath.GetAvailableDrivesOnMacOs(missing);

        Assert.Equal(new List<string> { "/" }, drives);
    }
}