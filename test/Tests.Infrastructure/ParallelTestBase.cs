using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Util;
using Sparrow.Server.Debugging;
using Sparrow.Threading;
using Sparrow.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Infrastructure;

public class ParallelTestBase : LinuxRaceConditionWorkAround, IAsyncLifetime
{
    private static readonly object Locker = new();

    private static readonly bool WriteToFile;

    private static readonly string FileName;

    private const string XunitConfigurationFile = "xunit.runner.json";

    private static readonly SemaphoreSlim ConcurrentTestsSemaphore;
    private readonly MultipleUseFlag _concurrentTestsSemaphoreTaken = new();

    static ParallelTestBase()
    {
        DebugStuff.Attach();

        var maxNumberOfConcurrentTests = Math.Max(ProcessorInfo.ProcessorCount / 2, 2);

        if (int.TryParse(Environment.GetEnvironmentVariable("RAVEN_MAX_RUNNING_TESTS"), out var maxRunningTests))
            maxNumberOfConcurrentTests = maxRunningTests;
        else
        {
            var fileInfo = new FileInfo(XunitConfigurationFile);
            if (fileInfo.Exists)
            {
                using (var file = File.OpenRead(XunitConfigurationFile))
                using (var sr = new StreamReader(file))
                {
                    var json = JObject.Parse(sr.ReadToEnd());

                    if (json.TryGetValue("maxRunningTests", out var testsToken))
                        maxNumberOfConcurrentTests = testsToken.Value<int>();
                    else if (json.TryGetValue("maxParallelThreads", out var threadsToken))
                        maxNumberOfConcurrentTests = threadsToken.Value<int>();
                }
            }
        }

        Console.WriteLine("Max number of concurrent tests is: " + maxNumberOfConcurrentTests);
        ConcurrentTestsSemaphore = new SemaphoreSlim(maxNumberOfConcurrentTests, maxNumberOfConcurrentTests);

        if (bool.TryParse(Environment.GetEnvironmentVariable("RAVEN_WRITE_RUNNING_TESTS_TO_FILE"), out var writeToFile))
        {
            WriteToFile = writeToFile;
        }

        if (WriteToFile)
        {
            FileName = $"RunningTests-{Guid.NewGuid():N}.txt";
            Console.WriteLine($"Writing running tests to '{FileName}'.");
        }
    }

    public ParallelTestBase(ITestOutputHelper output, [CallerFilePath] string sourceFile = "")
        : base(output, sourceFile)
    {
    }

    public Task InitializeAsync()
    {
        return ConcurrentTestsSemaphore.WaitAsync()
            .ContinueWith(x =>
            {
                if (WriteToFile)
                {
                    lock (Locker)
                    {
                        File.AppendAllText(FileName, $"[{SystemTime.UtcNow}] Running: '{Context.UniqueTestName}'.{Environment.NewLine}");
                    }
                }

                return _concurrentTestsSemaphoreTaken.Raise();
            });
    }

    public Task DisposeAsync()
    {
        if (WriteToFile)
        {
            lock (Locker)
            {
                File.AppendAllText(FileName, $"[{SystemTime.UtcNow}] Finished: '{Context.UniqueTestName}'.{Environment.NewLine}");
            }
        }

        return Task.CompletedTask;
    }

    public override void Dispose()
    {
        base.Dispose();

        if (_concurrentTestsSemaphoreTaken.Lower())
            ConcurrentTestsSemaphore.Release();
    }
}
