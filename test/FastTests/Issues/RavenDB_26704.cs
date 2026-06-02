using System.Threading.Tasks;
using Raven.Server.Config;
using Raven.Server.ServerWide;
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
    // fix dotnet/runtime#122637 not yet backported to .NET 10). DriveInfo.GetDrives() reaches the racy,
    // non-thread-safe getmntinfo(); two concurrent calls race on its shared libc buffer and one thread can
    // over-read freed memory, producing a process-fatal AccessViolationException on macOS Tahoe.
    // FolderPath now serializes the call on macOS, so the folder browser can be hit concurrently without
    // crashing. We hammer GetOptions() (its empty-path branch calls DriveInfo.GetDrives()) from many threads;
    // before the fix this would tear down the test process on Tahoe. Gated to macOS, where the fix matters.
    [RavenMultiplatformFact(RavenTestCategory.Studio, RavenPlatform.OsX)]
    public async Task ConcurrentFolderBrowserCalls_DoNotCrashOnMacOs()
    {
        var configuration = RavenConfiguration.CreateForTesting("foo", ResourceType.Database);
        configuration.Initialize();

        const int parallelism = 32;
        const int iterations = 50;

        var tasks = new Task[parallelism];
        for (var t = 0; t < parallelism; t++)
        {
            tasks[t] = Task.Run(() =>
            {
                for (var i = 0; i < iterations; i++)
                {
                    var options = FolderPath.GetOptions(path: string.Empty, isBackupFolder: false, configuration);
                    Assert.NotNull(options);
                }
            });
        }

        await Task.WhenAll(tasks);
    }
}