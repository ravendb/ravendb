using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Server.Platform;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Sparrow;

public unsafe class PalConcurrentDirectoryTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public void ThreadsThatSyncOrCreateDirectoriesDoNotSeeEachOthersPaths()
    {
        const int threads = 16;
        const int iterations = 30;
        string root = Path.Combine(Path.GetTempPath(), "PalConcurrentDirectory-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        var failures = new ConcurrentQueue<string>();
        using var start = new Barrier(threads);

        try
        {
            var tasks = new Task[threads];
            for (int t = 0; t < threads; t++)
            {
                int thread = t;
                tasks[t] = Task.Factory.StartNew(() =>
                {
                    byte[] header = new byte[4096];
                    start.SignalAndWait();
                    for (int i = 0; i < iterations; i++)
                    {
                        string directory = Path.Combine(root, $"thread-{thread}", $"directory-{i}");
                        Directory.CreateDirectory(directory);

                        PalFlags.FailCodes rc;
                        int errorCode;
                        fixed (byte* p = header)
                            rc = Pal.rvn_write_header(Path.Combine(directory, "headers.one"), p, header.Length, out errorCode);
                        if (rc != PalFlags.FailCodes.Success)
                            failures.Enqueue($"rvn_write_header {directory}: {rc} errno {errorCode}");

                        string file = Path.Combine(directory, "Indexes", "Raven.voron");
                        rc = Pal.rvn_create_and_mmap64_file(file, 64 * 1024, PalFlags.MmapOptions.None, out var handle, out _, out _, out errorCode);
                        if (rc != PalFlags.FailCodes.Success)
                            failures.Enqueue($"rvn_create_and_mmap64_file {file}: {rc} errno {errorCode}");
                        else
                            handle.Dispose();

                        if (Directory.Exists(Path.Combine(directory, "Indexes")) == false)
                            failures.Enqueue($"no directory was created for {file}");
                    }
                }, TaskCreationOptions.LongRunning);
            }

            Task.WaitAll(tasks);

            Assert.True(failures.IsEmpty, $"{failures.Count} failures, first: {string.Join(Environment.NewLine, failures.ToArray()[..Math.Min(5, failures.Count)])}");
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch (IOException) { }
        }
    }
}
