using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.Platform;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.ExtensionPoints
{
    public class DatabaseEventExecShutdownTests : RavenTestBase
    {
        public DatabaseEventExecShutdownTests(ITestOutputHelper output) : base(output)
        {
        }

        private static string LongSleepScript => PlatformDetails.RunningOnPosix
            ? "#!/bin/bash\necho $$ > \"$1\"\nsleep 300\n"
            : @"
param([string]$outputPath)
Set-Content -LiteralPath $outputPath -Value $PID
Start-Sleep -Seconds 300";

        [RavenFact(RavenTestCategory.Configuration)]
        public async Task OnDatabaseDelete_WhenServerShutsDownMidExecution_ChildProcessIsKilled()
        {
            string scriptExtension = PlatformDetails.RunningOnPosix ? ".sh" : ".ps1";
            string scriptFile = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), scriptExtension));
            string pidFile = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".txt"));

            try
            {
                File.WriteAllText(scriptFile, LongSleepScript);

                Logger logger = LoggingSource.Instance.GetLogger("test", nameof(DatabaseEventExecShutdownTests));

                var parameters = new DatabaseExecUtils.OnDatabaseDeleteParameters
                {
                    Executable = PlatformDetails.RunningOnPosix ? "bash" : "powershell",
                    Arguments = PlatformDetails.RunningOnPosix ? $"{scriptFile} {pidFile}" : $"-NoProfile {scriptFile} {pidFile}",
                    Timeout = TimeSpan.FromMinutes(5),
                    MaxRetryDuration = TimeSpan.FromMinutes(10),
                    DatabaseName = "ShutdownDb",
                    HardDelete = true,
                    Logger = logger
                };

                using var shutdown = new CancellationTokenSource();

                Task hook = DatabaseExecUtils.ExecuteOnDatabaseDeleteAsync(parameters, shutdown.Token);

                // wait until the script is actually running and has told us its pid
                int childPid = await WaitForChildPidAsync(pidFile);
                Assert.True(childPid > 0, "The hook script did not start.");
                Assert.True(IsAlive(childPid), $"Child {childPid} should be running before shutdown.");

                // this is what ServerShutdown does to an in-flight hook
                shutdown.Cancel();

                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => hook);

                bool died = await WaitForConditionAsync(() => IsAlive(childPid) == false, TimeSpan.FromSeconds(10));

                Assert.True(died,
                    $"The hook's child process (pid {childPid}) was still alive 10s after the shutdown token was cancelled. " +
                    "Requirement: 'Retries and the child process stop when the server shuts down.'");
            }
            finally
            {
                TryDelete(scriptFile);
                TryDelete(pidFile);
            }
        }

        [RavenFact(RavenTestCategory.Configuration)]
        public async Task OnDatabaseDelete_TotalRetryBudget_IsNotExceededByAnInFlightAttempt()
        {
            string scriptExtension = PlatformDetails.RunningOnPosix ? ".sh" : ".ps1";
            string scriptFile = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), scriptExtension));
            string pidFile = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".txt"));

            try
            {
                File.WriteAllText(scriptFile, LongSleepScript);

                Logger logger = LoggingSource.Instance.GetLogger("test", nameof(DatabaseEventExecShutdownTests));

                // the per-attempt timeout is deliberately larger than the total budget
                var parameters = new DatabaseExecUtils.OnDatabaseDeleteParameters
                {
                    Executable = PlatformDetails.RunningOnPosix ? "bash" : "powershell",
                    Arguments = PlatformDetails.RunningOnPosix ? $"{scriptFile} {pidFile}" : $"-NoProfile {scriptFile} {pidFile}",
                    Timeout = TimeSpan.FromSeconds(20),
                    MaxRetryDuration = TimeSpan.FromSeconds(5),
                    DatabaseName = "BudgetDb",
                    HardDelete = true,
                    Logger = logger
                };

                var sw = Stopwatch.StartNew();
                await Assert.ThrowsAnyAsync<Exception>(() => DatabaseExecUtils.ExecuteOnDatabaseDeleteAsync(parameters, CancellationToken.None));
                sw.Stop();

                Assert.True(sw.Elapsed < TimeSpan.FromSeconds(8),
                    $"The hook ran for {sw.Elapsed.TotalSeconds:F1}s with a configured total retry budget of " +
                    $"{parameters.MaxRetryDuration.TotalSeconds}s and a per-attempt timeout of {parameters.Timeout.TotalSeconds}s. " +
                    "The budget is only checked between attempts, so an in-flight attempt can overshoot it by a full timeout.");
            }
            finally
            {
                TryDelete(scriptFile);
                TryDelete(pidFile);
            }
        }

        private static async Task<int> WaitForChildPidAsync(string pidFile)
        {
            var sw = Stopwatch.StartNew();
            while (sw.Elapsed < TimeSpan.FromSeconds(30))
            {
                if (File.Exists(pidFile))
                {
                    try
                    {
                        string content = File.ReadAllText(pidFile).Trim();
                        if (int.TryParse(content, out int pid))
                            return pid;
                    }
                    catch (IOException)
                    {
                        // still being written
                    }
                }

                await Task.Delay(100);
            }

            return -1;
        }

        private static async Task<bool> WaitForConditionAsync(Func<bool> condition, TimeSpan timeout)
        {
            var sw = Stopwatch.StartNew();
            while (sw.Elapsed < timeout)
            {
                if (condition())
                    return true;

                await Task.Delay(200);
            }

            return condition();
        }

        private static bool IsAlive(int pid)
        {
            try
            {
                using Process process = Process.GetProcessById(pid);
                return process.HasExited == false;
            }
            catch (ArgumentException)
            {
                return false;
            }
            catch (InvalidOperationException)
            {
                return false;
            }
        }

        private static void TryDelete(string path)
        {
            try
            {
                if (File.Exists(path))
                    File.Delete(path);
            }
            catch (IOException)
            {
            }
        }
    }
}
