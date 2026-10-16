using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Utils;
using Sparrow.Logging;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using static SlowTests.ExtensionPoints.DatabaseEventExecTestHelper;

namespace SlowTests.ExtensionPoints
{
    public class DatabaseEventExecShutdownTests : RavenTestBase
    {
        public DatabaseEventExecShutdownTests(ITestOutputHelper output) : base(output)
        {
        }

        private static string LongSleepScript => Script(
            posix: "#!/bin/bash\necho $$ > \"$1\"\nsleep 300\n",
            windows: @"
param([string]$outputPath)
Set-Content -LiteralPath $outputPath -Value $PID
Start-Sleep -Seconds 300");

        [RavenFact(RavenTestCategory.Configuration)]
        public async Task OnDatabaseDelete_WhenServerShutsDownMidExecution_ChildProcessIsKilled()
        {
            string scriptFile = NewScriptPath();
            string pidFile = NewOutputPath();

            try
            {
                File.WriteAllText(scriptFile, LongSleepScript);

                Logger logger = LoggingSource.Instance.GetLogger("test", nameof(DatabaseEventExecShutdownTests));

                var parameters = new DatabaseExecUtils.OnDatabaseDeleteParameters
                {
                    Executable = Executable,
                    Arguments = Arguments(scriptFile, pidFile),
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

                bool died = await WaitForValueAsync(() => IsAlive(childPid) == false, true, timeout: 10_000, interval: 200);

                Assert.True(died,
                    $"The hook's child process (pid {childPid}) was still alive 10s after the shutdown token was cancelled. " +
                    "Requirement: 'Retries and the child process stop when the server shuts down.'");
            }
            finally
            {
                TryDelete(scriptFile, pidFile);
            }
        }

        [RavenFact(RavenTestCategory.Configuration)]
        public async Task OnDatabaseDelete_WhenServerIsDisposed_InFlightHookIsDrainedBeforeDisposeReturns()
        {
            string scriptFile = NewScriptPath();
            string pidFile = NewOutputPath();

            try
            {
                File.WriteAllText(scriptFile, LongSleepScript);

                var customSettings = new Dictionary<string, string>();
                ConfigureHook(customSettings, scriptFile, pidFile);

                RavenServer server = GetNewServer(new ServerCreationOptions { CustomSettings = customSettings });

                // The hook's last act is this callback, and it is held long enough that the rest of the server
                // shutdown would normally beat it - so it can only have run by the time Dispose() returns if the
                // landlord waited for the hook. Kept well under the 5s drain timeout.
                int hookFinished = 0;
                server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().OnDatabaseDeleteExecCompleted = (_, _) =>
                {
                    Thread.Sleep(TimeSpan.FromSeconds(3));
                    Volatile.Write(ref hookFinished, 1);
                };

                int childPid;
                // the store must not issue a delete of its own on dispose: a repeated delete request may fire the hook again
                using (var store = GetDocumentStore(new Options { Server = server, DeleteDatabaseOnDispose = false }))
                {
                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, hardDelete: true));

                    childPid = await WaitForChildPidAsync(pidFile);
                    Assert.True(childPid > 0, "The hook script did not start.");
                    Assert.True(IsAlive(childPid), $"Child {childPid} should be running before shutdown.");
                }

                server.Dispose();

                Assert.True(Volatile.Read(ref hookFinished) == 1,
                    "The server finished disposing while the database deletion hook was still in flight - the landlord must drain in-flight hooks on dispose.");

                bool died = await WaitForValueAsync(() => IsAlive(childPid) == false, true, timeout: 10_000, interval: 200);
                Assert.True(died, $"The hook's child process (pid {childPid}) outlived the server it was spawned by.");
            }
            finally
            {
                TryDelete(scriptFile, pidFile);
            }
        }

        [RavenFact(RavenTestCategory.Configuration)]
        public async Task OnDatabaseDelete_TotalRetryBudget_IsNotExceededByAnInFlightAttempt()
        {
            string scriptFile = NewScriptPath();
            string pidFile = NewOutputPath();

            try
            {
                File.WriteAllText(scriptFile, LongSleepScript);

                Logger logger = LoggingSource.Instance.GetLogger("test", nameof(DatabaseEventExecShutdownTests));

                // the per-attempt timeout is deliberately larger than the total budget
                var parameters = new DatabaseExecUtils.OnDatabaseDeleteParameters
                {
                    Executable = Executable,
                    Arguments = Arguments(scriptFile, pidFile),
                    Timeout = TimeSpan.FromSeconds(20),
                    MaxRetryDuration = TimeSpan.FromSeconds(5),
                    DatabaseName = "BudgetDb",
                    HardDelete = true,
                    Logger = logger
                };

                var sw = Stopwatch.StartNew();
                await Assert.ThrowsAnyAsync<Exception>(() => DatabaseExecUtils.ExecuteOnDatabaseDeleteAsync(parameters, CancellationToken.None));
                sw.Stop();

                // ~5s budget, plus the output drain and up to 1s of backoff jitter, with headroom for a loaded
                // agent - while a budget checked only between attempts would run for the full 20s timeout
                Assert.True(sw.Elapsed < TimeSpan.FromSeconds(12),
                    $"The hook ran for {sw.Elapsed.TotalSeconds:F1}s with a configured total retry budget of " +
                    $"{parameters.MaxRetryDuration.TotalSeconds}s and a per-attempt timeout of {parameters.Timeout.TotalSeconds}s. " +
                    "The budget is only checked between attempts, so an in-flight attempt can overshoot it by a full timeout.");
            }
            finally
            {
                TryDelete(scriptFile, pidFile);
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
    }
}
