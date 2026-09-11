using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Documents;
using Sparrow.Platform;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.ExtensionPoints
{
    public class DatabaseEventExecTests : RavenTestBase
    {
        public DatabaseEventExecTests(ITestOutputHelper output) : base(output)
        {
        }

        private const int HookTimeoutInSeconds = 60;

        /// <summary>
        /// Writes every hook argument on its own line. bash passes the '--' sentinel through to the
        /// script while PowerShell's parameter binder consumes it, so callers strip a leading '--' line.
        /// </summary>
        private static string EchoArgsScript => PlatformDetails.RunningOnPosix
            ? "#!/bin/bash\nout=$1\nshift\nfor a in \"$@\"; do echo \"$a\" >> \"$out\"; done\nexit 0\n"
            : @"
param([string]$outputPath)
foreach ($a in $args) { Add-Content $outputPath $a }
exit 0";

        private static string FailingScript => PlatformDetails.RunningOnPosix
            ? "#!/bin/bash\necho attempt >> \"$1\"\nexit 3\n"
            : @"
param([string]$outputPath)
Add-Content $outputPath ""attempt""
exit 3";

        private static string HangingScript => PlatformDetails.RunningOnPosix
            ? "#!/bin/bash\necho attempt >> \"$1\"\nsleep 30\n"
            : @"
param([string]$outputPath)
Add-Content $outputPath ""attempt""
Start-Sleep -Seconds 30";

        private static (string ScriptFile, string OutputFile) NewTempPaths()
        {
            string scriptExtension = PlatformDetails.RunningOnPosix ? ".sh" : ".ps1";
            return (Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), scriptExtension)),
                Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".txt")));
        }

        private static IDictionary<string, string> ConfigureHook(string scriptFile, string outputFile, string script)
        {
            File.WriteAllText(scriptFile, script);

            IDictionary<string, string> customSettings = new ConcurrentDictionary<string, string>();

            if (PlatformDetails.RunningOnPosix)
            {
                customSettings[RavenConfiguration.GetKey(x => x.Databases.OnDatabaseDeleteExec)] = "bash";
                customSettings[RavenConfiguration.GetKey(x => x.Databases.OnDatabaseDeleteExecArguments)] = $"{scriptFile} {outputFile}";
            }
            else
            {
                customSettings[RavenConfiguration.GetKey(x => x.Databases.OnDatabaseDeleteExec)] = "powershell";
                customSettings[RavenConfiguration.GetKey(x => x.Databases.OnDatabaseDeleteExecArguments)] = $"-NoProfile {scriptFile} {outputFile}";
            }

            return customSettings;
        }

        private static void CleanupTempPaths(string scriptFile, string outputFile)
        {
            try
            {
                if (File.Exists(scriptFile))
                    File.Delete(scriptFile);
            }
            catch (IOException)
            {
                // a still running script may hold the file on Windows
            }

            try
            {
                if (File.Exists(outputFile))
                    File.Delete(outputFile);
            }
            catch (IOException)
            {
            }
        }

        /// <summary>
        /// Arms the landlord test hooks. Returns a task that completes once the hook has finished, and a
        /// counter of how many times the script was actually attempted.
        /// </summary>
        private (Task Completed, Func<int> Attempts) TrackHook()
        {
            int attempts = 0;
            TaskCompletionSource<object> completed = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

            DatabasesLandlord.TestingStuff testing = Server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly();
            testing.OnDatabaseDeleteExecAttempt = _ => Interlocked.Increment(ref attempts);
            testing.OnDatabaseDeleteExecCompleted = (_, _) => completed.TrySetResult(null);

            return (completed.Task, () => Volatile.Read(ref attempts));
        }

        private static async Task<bool> WaitForHookAsync(Task completed, int timeoutInSeconds = HookTimeoutInSeconds)
        {
            return await Task.WhenAny(completed, Task.Delay(TimeSpan.FromSeconds(timeoutInSeconds))).ConfigureAwait(false) == completed;
        }

        private static string[] ReadHookArguments(string outputFile)
        {
            string[] lines = File.ReadAllLines(outputFile)
                .Select(x => x.Trim())
                .Where(x => string.IsNullOrEmpty(x) == false)
                .ToArray();

            // bash passes '--' through to the script, PowerShell's binder swallows it
            if (lines.Length > 0 && lines[0] == "--")
                lines = lines.Skip(1).ToArray();

            return lines;
        }

        private async Task AssertDatabaseDeletedAsync(Raven.Client.Documents.IDocumentStore store, string databaseName)
        {
            bool gone = await WaitForValueAsync(async () =>
            {
                string[] names = await store.Maintenance.Server.SendAsync(new GetDatabaseNamesOperation(0, 1024));
                return names.Contains(databaseName) == false;
            }, true);

            Assert.True(gone, $"Database '{databaseName}' was not deleted even though the deletion hook failed. The hook must never affect the deletion.");
        }

        [RavenFact(RavenTestCategory.Configuration)]
        public async Task OnDatabaseDelete_SoftDelete_ReportsSoftDeletionKind()
        {
            (string scriptFile, string outputFile) = NewTempPaths();

            try
            {
                UseNewLocalServer(customSettings: ConfigureHook(scriptFile, outputFile, EchoArgsScript));

                (Task completed, Func<int> attempts) = TrackHook();

                string databaseName;
                using (var store = GetDocumentStore())
                {
                    databaseName = store.Database;
                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, hardDelete: false));

                    Assert.True(await WaitForHookAsync(completed), "The deletion hook did not complete in time.");
                }

                string[] arguments = ReadHookArguments(outputFile);

                Assert.Equal(3, arguments.Length);
                Assert.Equal(databaseName, arguments[0]);
                Assert.Equal(Convert.ToBase64String(Encoding.UTF8.GetBytes(databaseName)), arguments[1]);
                Assert.Equal("soft", arguments[2]);
                Assert.Equal(1, attempts());
            }
            finally
            {
                CleanupTempPaths(scriptFile, outputFile);
            }
        }

        [RavenFact(RavenTestCategory.Configuration)]
        public async Task OnDatabaseDelete_DatabaseNameStartingWithDashes_IsPassedAsAValueNotAFlag()
        {
            (string scriptFile, string outputFile) = NewTempPaths();

            // '-' is a legal database name character with no restriction on the leading position, so
            // without the '--' sentinel the script would parse this name as an option
            string dashedName = "--force-" + Guid.NewGuid().ToString("N").Substring(0, 8);

            try
            {
                UseNewLocalServer(customSettings: ConfigureHook(scriptFile, outputFile, EchoArgsScript));

                (Task completed, Func<int> attempts) = TrackHook();

                using (var store = GetDocumentStore(new Options { ModifyDatabaseName = _ => dashedName }))
                {
                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(dashedName, hardDelete: true));

                    Assert.True(await WaitForHookAsync(completed), "The deletion hook did not complete in time.");
                }

                string[] arguments = ReadHookArguments(outputFile);

                Assert.Equal(3, arguments.Length);
                Assert.Equal(dashedName, arguments[0]);
                Assert.Equal(Convert.ToBase64String(Encoding.UTF8.GetBytes(dashedName)), arguments[1]);
                Assert.Equal("hard", arguments[2]);
                Assert.Equal(1, attempts());
            }
            finally
            {
                CleanupTempPaths(scriptFile, outputFile);
            }
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Configuration)]
        public async Task OnDatabaseDelete_ShardedDatabase_FiresOnceWithTheBaseName()
        {
            (string scriptFile, string outputFile) = NewTempPaths();

            try
            {
                UseNewLocalServer(customSettings: ConfigureHook(scriptFile, outputFile, EchoArgsScript));

                (Task completed, Func<int> attempts) = TrackHook();

                string databaseName;
                using (var store = Sharding.GetDocumentStore())
                {
                    databaseName = store.Database;
                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, hardDelete: true));

                    Assert.True(await WaitForHookAsync(completed), "The deletion hook did not complete in time.");
                }

                // give a per-shard firing a chance to show up, if the hook were placed below the fan-out
                await Task.Delay(TimeSpan.FromSeconds(3));

                string[] arguments = ReadHookArguments(outputFile);

                Assert.Equal(3, arguments.Length);
                Assert.Equal(databaseName, arguments[0]);
                Assert.DoesNotContain("$", arguments[0]);
                Assert.Equal("hard", arguments[2]);
                Assert.Equal(1, attempts());
            }
            finally
            {
                CleanupTempPaths(scriptFile, outputFile);
            }
        }

        [RavenFact(RavenTestCategory.Configuration)]
        public async Task OnDatabaseDelete_WhenScriptExitsNonZero_IsRetriedThreeTimes_AndDeletionStillCompletes()
        {
            (string scriptFile, string outputFile) = NewTempPaths();

            try
            {
                IDictionary<string, string> customSettings = ConfigureHook(scriptFile, outputFile, FailingScript);
                customSettings[RavenConfiguration.GetKey(x => x.Databases.OnDatabaseDeleteExecTimeout)] = "10";
                customSettings[RavenConfiguration.GetKey(x => x.Databases.OnDatabaseDeleteExecMaxRetryDuration)] = "120";

                UseNewLocalServer(customSettings: customSettings);

                (Task completed, Func<int> attempts) = TrackHook();

                using (var store = GetDocumentStore())
                {
                    string databaseName = store.Database;
                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, hardDelete: true));

                    // the deletion must not wait for the hook, nor be affected by its failure
                    await AssertDatabaseDeletedAsync(store, databaseName);

                    Assert.True(await WaitForHookAsync(completed), "The deletion hook did not give up in time.");
                }

                Assert.Equal(3, attempts());
                Assert.Equal(3, File.ReadAllLines(outputFile).Count(x => x.Trim() == "attempt"));
            }
            finally
            {
                CleanupTempPaths(scriptFile, outputFile);
            }
        }

        [RavenFact(RavenTestCategory.Configuration)]
        public async Task OnDatabaseDelete_WhenExecutableIsMissing_IsAttemptedOnce_AndDeletionStillCompletes()
        {
            IDictionary<string, string> customSettings = new ConcurrentDictionary<string, string>();
            customSettings[RavenConfiguration.GetKey(x => x.Databases.OnDatabaseDeleteExec)] =
                Path.Combine(Path.GetTempPath(), "raven-no-such-executable-" + Guid.NewGuid().ToString("N"));

            UseNewLocalServer(customSettings: customSettings);

            (Task completed, Func<int> attempts) = TrackHook();

            using (var store = GetDocumentStore())
            {
                string databaseName = store.Database;
                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, hardDelete: true));

                await AssertDatabaseDeletedAsync(store, databaseName);

                Assert.True(await WaitForHookAsync(completed), "The deletion hook did not complete in time.");
            }

            // a missing executable cannot resolve on retry, so it must not be attempted again
            Assert.Equal(1, attempts());
        }

        [RavenFact(RavenTestCategory.Configuration)]
        public async Task OnDatabaseDelete_WhenScriptTimesOut_DeletionStillCompletes()
        {
            (string scriptFile, string outputFile) = NewTempPaths();

            try
            {
                IDictionary<string, string> customSettings = ConfigureHook(scriptFile, outputFile, HangingScript);
                customSettings[RavenConfiguration.GetKey(x => x.Databases.OnDatabaseDeleteExecTimeout)] = "1";
                customSettings[RavenConfiguration.GetKey(x => x.Databases.OnDatabaseDeleteExecMaxRetryDuration)] = "5";

                UseNewLocalServer(customSettings: customSettings);

                (Task completed, Func<int> attempts) = TrackHook();

                using (var store = GetDocumentStore())
                {
                    string databaseName = store.Database;
                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, hardDelete: true));

                    await AssertDatabaseDeletedAsync(store, databaseName);

                    Assert.True(await WaitForHookAsync(completed), "The deletion hook did not give up in time - a timed out script must never block the hook indefinitely.");
                }

                Assert.True(attempts() >= 1, "The script should have been attempted at least once.");
            }
            finally
            {
                CleanupTempPaths(scriptFile, outputFile);
            }
        }

        [RavenFact(RavenTestCategory.Configuration)]
        public async Task OnDatabaseDelete_WhenNotConfigured_HookDoesNotRun()
        {
            UseNewLocalServer();

            (Task completed, Func<int> attempts) = TrackHook();

            using (var store = GetDocumentStore())
            {
                string databaseName = store.Database;
                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, hardDelete: true));

                await AssertDatabaseDeletedAsync(store, databaseName);
            }

            Assert.False(await WaitForHookAsync(completed, timeoutInSeconds: 5), "No hook should have run when the exec setting is not configured.");
            Assert.Equal(0, attempts());
        }
    }
}
