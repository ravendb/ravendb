using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Sparrow;
using Tests.Infrastructure;
using Voron;
using Voron.Impl.Paging;
using Xunit;
using NativeMemory = Sparrow.Utils.NativeMemory;

namespace FastTests.Voron;

// Growing a file replaces its pager state, and the old state is closed by its finalizer. Disposing the environment must not
// leave that to the finalizer and the background disposal: when Dispose returns, nothing of the file may stay mapped (and
// on Windows, locked).
public class PagerStateDisposalTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public void NothingOfTheDataFileStaysMappedOnceTheEnvironmentIsDisposed()
    {
        var path = Path.Combine(Path.GetTempPath(), $"{nameof(PagerStateDisposalTests)}-{Guid.NewGuid():N}");
        using var finalizerBlocked = new ManualResetEventSlim();
        using var releaseFinalizer = new ManualResetEventSlim();
        try
        {
            string dataFile;
            using (var options = StorageEnvironmentOptions.ForPathForTests(path))
            {
                options.ManualFlushing = true;
                var env = new StorageEnvironment(options);
                dataFile = env.DataPager.FileName;

                // a busy finalizer thread - as on a loaded machine - leaves the replaced states unreachable but not yet closed
                BlockTheFinalizerThread(finalizerBlocked, releaseFinalizer);

                GrowTheDataFile(env);
                GC.Collect();
                Assert.True(finalizerBlocked.Wait(TimeSpan.FromSeconds(30)), "the finalizer thread must be blocked for the test to mean anything");

                env.Dispose();
            }

            Assert.False(NativeMemory.FileMapping.TryGetValue(dataFile, out var mapping) && mapping.Value.Info.IsEmpty == false,
                DescribeMappings(dataFile));
        }
        finally
        {
            releaseFinalizer.Set();
            GC.WaitForPendingFinalizers();
            Pager.State.DrainPendingDisposal();
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
    }

    private static string DescribeMappings(string fileName)
    {
        if (NativeMemory.FileMapping.TryGetValue(fileName, out var mapping) == false || mapping.Value.Info.IsEmpty)
            return "no mappings";

        var sb = new StringBuilder($"{mapping.Value.Info.Count} mappings of '{fileName}' are still open");
        foreach (var (address, size) in mapping.Value.Info)
            sb.Append($"{Environment.NewLine}    0x{address:x} ({new Size(size, SizeUnit.Bytes)})");

        return sb.ToString();
    }

    private static void GrowTheDataFile(StorageEnvironment env)
    {
        var value = new byte[256 * 1024];
        for (var i = 0; i < 64; i++)
        {
            Random.Shared.NextBytes(value);
            using (var tx = env.WriteTransaction())
            {
                tx.CreateTree("grow").Add("key/" + i, new MemoryStream(value));
                tx.Commit();
            }

            env.FlushLogToDataFile(); // the data file grows when the journals are applied to it
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void BlockTheFinalizerThread(ManualResetEventSlim blocked, ManualResetEventSlim release)
    {
        GC.KeepAlive(new FinalizerBlocker(blocked, release));// ensure it is not optimized away
    }

    private sealed class FinalizerBlocker(ManualResetEventSlim blocked, ManualResetEventSlim release)
    {
        ~FinalizerBlocker()
        {
            blocked.Set();
            release.Wait(TimeSpan.FromSeconds(30));
        }
    }
}
