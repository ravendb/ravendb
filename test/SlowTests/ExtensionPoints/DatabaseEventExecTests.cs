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
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Sparrow.Platform;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using static SlowTests.ExtensionPoints.DatabaseEventExecTestHelper;

namespace SlowTests.ExtensionPoints
{
    public class DatabaseEventExecTests : RavenTestBase
    {
        public DatabaseEventExecTests(ITestOutputHelper output) : base(output)
        {
        }

        private const int HookTimeoutInSeconds = 60;

        /// <summary>
        /// Writes every argument after the output file on its own line.
        /// </summary>
        private static string EchoArgsScript => Script(
            posix: "#!/bin/bash\nout=$1\nshift\nfor a in \"$@\"; do echo \"$a\" >> \"$out\"; done\nexit 0\n",
            windows: @"
param([string]$outputPath)
foreach ($a in $args) { Add-Content $outputPath $a }
exit 0");

        /// <summary>
        /// Parses options the way a real script would: on POSIX every leading dash-prefixed argument is an
        /// option until '--', on Windows the binder fills a '-force' switch. Options are written as
        /// 'option:...' lines and the remaining arguments one per line, so a name parsed as a flag shows up.
        /// </summary>
        private static string OptionParsingScript => Script(
            posix: "#!/bin/bash\nout=$1\nshift\n" +
                   "while [ $# -gt 0 ]; do\n" +
                   "  case \"$1\" in\n" +
                   "    --) shift; break ;;\n" +
                   "    -*) echo \"option:$1\" >> \"$out\"; shift ;;\n" +
                   "    *) break ;;\n" +
                   "  esac\n" +
                   "done\n" +
                   "for a in \"$@\"; do echo \"$a\" >> \"$out\"; done\nexit 0\n",
            windows: @"
param([string]$outputPath, [switch]$force)
if ($force) { Add-Content $outputPath ""option:-force"" }
foreach ($a in $args) { Add-Content $outputPath $a }
exit 0");

        private static string FailingScript => Script(
            posix: "#!/bin/bash\necho attempt >> \"$1\"\nexit 3\n",
            windows: @"
param([string]$outputPath)
Add-Content $outputPath ""attempt""
exit 3");

        private static string HangingScript => Script(
            posix: "#!/bin/bash\necho attempt >> \"$1\"\nsleep 30\n",
            windows: @"
param([string]$outputPath)
Add-Content $outputPath ""attempt""
Start-Sleep -Seconds 30");

        private static IDictionary<string, string> ConfigureHook(string scriptFile, string outputFile, string script)
        {
            File.WriteAllText(scriptFile, script);

            IDictionary<string, string> customSettings = new ConcurrentDictionary<string, string>();
            DatabaseEventExecTestHelper.ConfigureHook(customSettings, scriptFile, outputFile);
            return customSettings;
        }

        /// <summary>
        /// Counts how many times the script was attempted and how many hook runs have finished, through
        /// the landlord test hooks.
        /// </summary>
        private sealed class HookTracker
        {
            private int _attempts;
            private int _completions;
            private readonly TaskCompletionSource<object> _firstCompletion = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

            public HookTracker(DatabasesLandlord landlord)
            {
                DatabasesLandlord.TestingStuff testing = landlord.ForTestingPurposesOnly();
                testing.OnDatabaseDeleteExecAttempt = _ => Interlocked.Increment(ref _attempts);
                testing.OnDatabaseDeleteExecCompleted = (_, _) =>
                {
                    Interlocked.Increment(ref _completions);
                    _firstCompletion.TrySetResult(null);
                };
            }

            public int Attempts => Volatile.Read(ref _attempts);

            public int Completions => Volatile.Read(ref _completions);

            public Task FirstCompletion => _firstCompletion.Task;
        }

        private HookTracker TrackHook() => new HookTracker(Server.ServerStore.DatabasesLandlord);

        private static async Task<bool> WaitForHookAsync(Task completed, int timeoutInSeconds = HookTimeoutInSeconds)
        {
            return await Task.WhenAny(completed, Task.Delay(TimeSpan.FromSeconds(timeoutInSeconds))).ConfigureAwait(false) == completed;
        }

        private static string[] ReadLines(string outputFile)
        {
            return File.ReadAllLines(outputFile)
                .Select(x => x.Trim())
                .Where(x => string.IsNullOrEmpty(x) == false)
                .ToArray();
        }

        /// <summary>
        /// Reads what <see cref="EchoArgsScript"/> wrote and returns the hook arguments that follow the '--' sentinel.
        /// </summary>
        private static string[] ReadHookArguments(string outputFile)
        {
            string[] lines = ReadLines(outputFile);

            // PowerShell's binder consumes the sentinel before the script sees it, so on Windows it is pinned
            // only by the dashed-name test, where its absence would bind the name to the '-force' switch
            if (PlatformDetails.RunningOnPosix == false)
                return lines;

            AssertSentinelPresent(lines);
            return lines.Skip(1).ToArray();
        }

        private static void AssertSentinelPresent(string[] lines)
        {
            // bash hands '--' to the script verbatim. Assert it rather than strip it when present: a reader that
            // tolerated its absence would keep every positional assertion green with the sentinel removed.
            Assert.True(lines.Length > 0 && lines[0] == "--",
                $"Expected the '--' sentinel ahead of the hook arguments, got: [{string.Join(", ", lines)}]");
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

        private void AssertFailureAlert(string databaseName)
        {
            string id = AlertRaised.GetKey(AlertType.DatabaseEventExecFailure, databaseName);

            using (Server.ServerStore.NotificationCenter.Storage.Read(id, out NotificationTableValue alert))
            using (alert)
            {
                Assert.True(alert != null, $"No server-wide '{AlertType.DatabaseEventExecFailure}' alert keyed by database '{databaseName}' was raised.");

                alert.Json.TryGet(nameof(AlertRaised.AlertType), out string alertType);
                alert.Json.TryGet(nameof(AlertRaised.Severity), out string severity);
                alert.Json.TryGet(nameof(AlertRaised.Key), out string key);
                alert.Json.TryGet(nameof(AlertRaised.Database), out string database);

                Assert.Equal(nameof(AlertType.DatabaseEventExecFailure), alertType);
                Assert.Equal(nameof(NotificationSeverity.Error), severity);
                Assert.Equal(databaseName, key);

                // the deletion drops the database's own notification storage, so the alert has to be server-scoped
                Assert.Null(database);
            }
        }

        [RavenFact(RavenTestCategory.Configuration)]
        public async Task OnDatabaseDelete_SoftDelete_ReportsSoftDeletionKind()
        {
            string scriptFile = NewScriptPath();
            string outputFile = NewOutputPath();

            try
            {
                UseNewLocalServer(customSettings: ConfigureHook(scriptFile, outputFile, EchoArgsScript));

                HookTracker tracker = TrackHook();

                string databaseName;
                using (var store = GetDocumentStore())
                {
                    databaseName = store.Database;
                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, hardDelete: false));

                    Assert.True(await WaitForHookAsync(tracker.FirstCompletion), "The deletion hook did not complete in time.");
                }

                string[] arguments = ReadHookArguments(outputFile);

                Assert.Equal(4, arguments.Length);
                Assert.True(long.TryParse(arguments[3], out long raftIndex) && raftIndex > 0, $"Expected a positive raft index as the fourth argument, got '{arguments[3]}'.");
                Assert.Equal(databaseName, arguments[0]);
                Assert.Equal(Convert.ToBase64String(Encoding.UTF8.GetBytes(databaseName)), arguments[1]);
                Assert.Equal("soft", arguments[2]);
                Assert.Equal(1, tracker.Attempts);
            }
            finally
            {
                TryDelete(scriptFile, outputFile);
            }
        }

        [RavenFact(RavenTestCategory.Configuration)]
        public async Task OnDatabaseDelete_DatabaseNameStartingWithDashes_IsPassedAsAValueNotAFlag()
        {
            string scriptFile = NewScriptPath();
            string outputFile = NewOutputPath();

            // '-' is a legal database name character with no restriction on the leading position, so
            // without the '--' sentinel the script would parse this name as its '-force' option
            const string dashedName = "-force";

            try
            {
                UseNewLocalServer(customSettings: ConfigureHook(scriptFile, outputFile, OptionParsingScript));

                HookTracker tracker = TrackHook();

                using (var store = GetDocumentStore(new Options { ModifyDatabaseName = _ => dashedName }))
                {
                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(dashedName, hardDelete: true));

                    Assert.True(await WaitForHookAsync(tracker.FirstCompletion), "The deletion hook did not complete in time.");
                }

                // the script consumes the sentinel itself, so there is none to skip here
                string[] arguments = ReadLines(outputFile);

                Assert.DoesNotContain(arguments, x => x.StartsWith("option:", StringComparison.Ordinal));
                Assert.Equal(4, arguments.Length);
                Assert.True(long.TryParse(arguments[3], out long raftIndex) && raftIndex > 0, $"Expected a positive raft index as the fourth argument, got '{arguments[3]}'.");
                Assert.Equal(dashedName, arguments[0]);
                Assert.Equal(Convert.ToBase64String(Encoding.UTF8.GetBytes(dashedName)), arguments[1]);
                Assert.Equal("hard", arguments[2]);
                Assert.Equal(1, tracker.Attempts);
            }
            finally
            {
                TryDelete(scriptFile, outputFile);
            }
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Configuration)]
        public async Task OnDatabaseDelete_ShardedDatabase_FiresOnceWithTheBaseName()
        {
            string scriptFile = NewScriptPath();
            string outputFile = NewOutputPath();

            try
            {
                UseNewLocalServer(customSettings: ConfigureHook(scriptFile, outputFile, EchoArgsScript));

                HookTracker tracker = TrackHook();

                string databaseName;
                using (var store = Sharding.GetDocumentStore())
                {
                    databaseName = store.Database;
                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, hardDelete: true));

                    Assert.True(await WaitForHookAsync(tracker.FirstCompletion), "The deletion hook did not complete in time.");
                }

                // give a per-shard firing a chance to show up, if the hook were placed below the fan-out
                await Task.Delay(TimeSpan.FromSeconds(3));

                string[] arguments = ReadHookArguments(outputFile);

                Assert.Equal(4, arguments.Length);
                Assert.True(long.TryParse(arguments[3], out long raftIndex) && raftIndex > 0, $"Expected a positive raft index as the fourth argument, got '{arguments[3]}'.");
                Assert.Equal(databaseName, arguments[0]);
                Assert.DoesNotContain("$", arguments[0]);
                Assert.Equal("hard", arguments[2]);
                Assert.Equal(1, tracker.Attempts);
            }
            finally
            {
                TryDelete(scriptFile, outputFile);
            }
        }

        [RavenFact(RavenTestCategory.Configuration)]
        public async Task OnDatabaseDelete_DeletionDoesNotWaitForTheHook()
        {
            string scriptFile = NewScriptPath();
            string outputFile = NewOutputPath();

            try
            {
                // a script that is still running long after the deletion has been acknowledged
                IDictionary<string, string> customSettings = ConfigureHook(scriptFile, outputFile, HangingScript);
                customSettings[RavenConfiguration.GetKey(x => x.Databases.OnDatabaseDeleteExecTimeout)] = "120";
                customSettings[RavenConfiguration.GetKey(x => x.Databases.OnDatabaseDeleteExecMaxRetryDuration)] = "300";

                UseNewLocalServer(customSettings: customSettings);

                HookTracker tracker = TrackHook();

                using (var store = GetDocumentStore())
                {
                    string databaseName = store.Database;
                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, hardDelete: true));

                    // were the hook awaited before the raft index notification, the delete could only be
                    // acknowledged once the script had exited, and the hook would be complete by now
                    Assert.False(tracker.FirstCompletion.IsCompleted, "The deletion was acknowledged only after the hook had finished - the hook must be fire-and-forget.");

                    Assert.Equal(1, await WaitForValueAsync(() => tracker.Attempts, 1));
                    Assert.False(tracker.FirstCompletion.IsCompleted, "The hook should still be in flight after the deletion was acknowledged.");

                    await AssertDatabaseDeletedAsync(store, databaseName);
                }
            }
            finally
            {
                TryDelete(scriptFile, outputFile);
            }
        }

        [RavenFact(RavenTestCategory.Configuration)]
        public async Task OnDatabaseDelete_WhenScriptExitsNonZero_IsRetriedThreeTimes_AndDeletionStillCompletes()
        {
            string scriptFile = NewScriptPath();
            string outputFile = NewOutputPath();

            try
            {
                IDictionary<string, string> customSettings = ConfigureHook(scriptFile, outputFile, FailingScript);
                customSettings[RavenConfiguration.GetKey(x => x.Databases.OnDatabaseDeleteExecTimeout)] = "10";
                customSettings[RavenConfiguration.GetKey(x => x.Databases.OnDatabaseDeleteExecMaxRetryDuration)] = "120";

                UseNewLocalServer(customSettings: customSettings);

                HookTracker tracker = TrackHook();

                string databaseName;
                using (var store = GetDocumentStore())
                {
                    databaseName = store.Database;
                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, hardDelete: true));

                    // the deletion must not wait for the hook, nor be affected by its failure
                    await AssertDatabaseDeletedAsync(store, databaseName);

                    Assert.True(await WaitForHookAsync(tracker.FirstCompletion), "The deletion hook did not give up in time.");
                }

                Assert.Equal(3, tracker.Attempts);
                Assert.Equal(3, File.ReadAllLines(outputFile).Count(x => x.Trim() == "attempt"));
                AssertFailureAlert(databaseName);
            }
            finally
            {
                TryDelete(scriptFile, outputFile);
            }
        }

        [RavenFact(RavenTestCategory.Configuration)]
        public async Task OnDatabaseDelete_WhenExecutableIsMissing_IsAttemptedOnce_AndRaisesOneAlertPerDatabase()
        {
            IDictionary<string, string> customSettings = new ConcurrentDictionary<string, string>();
            customSettings[RavenConfiguration.GetKey(x => x.Databases.OnDatabaseDeleteExec)] =
                Path.Combine(Path.GetTempPath(), "raven-no-such-executable-" + Guid.NewGuid().ToString("N"));

            UseNewLocalServer(customSettings: customSettings);

            HookTracker tracker = TrackHook();

            // two failing deletions: the alerts are server-scoped, so one keyed by alert type alone would
            // have the second failure overwrite the first
            List<string> databaseNames = new List<string>();
            for (int i = 0; i < 2; i++)
            {
                using (var store = GetDocumentStore())
                {
                    string databaseName = store.Database;
                    databaseNames.Add(databaseName);

                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, hardDelete: true));

                    await AssertDatabaseDeletedAsync(store, databaseName);
                }
            }

            Assert.Equal(2, await WaitForValueAsync(() => tracker.Completions, 2, timeout: HookTimeoutInSeconds * 1000));

            // a missing executable cannot resolve on retry, so it must not be attempted again
            Assert.Equal(2, tracker.Attempts);

            foreach (string databaseName in databaseNames)
                AssertFailureAlert(databaseName);
        }

        [RavenFact(RavenTestCategory.Configuration)]
        public async Task OnDatabaseDelete_WhenScriptTimesOut_DeletionStillCompletes()
        {
            string scriptFile = NewScriptPath();
            string outputFile = NewOutputPath();

            try
            {
                IDictionary<string, string> customSettings = ConfigureHook(scriptFile, outputFile, HangingScript);
                customSettings[RavenConfiguration.GetKey(x => x.Databases.OnDatabaseDeleteExecTimeout)] = "1";
                customSettings[RavenConfiguration.GetKey(x => x.Databases.OnDatabaseDeleteExecMaxRetryDuration)] = "5";

                UseNewLocalServer(customSettings: customSettings);

                HookTracker tracker = TrackHook();

                using (var store = GetDocumentStore())
                {
                    string databaseName = store.Database;
                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, hardDelete: true));

                    await AssertDatabaseDeletedAsync(store, databaseName);

                    Assert.True(await WaitForHookAsync(tracker.FirstCompletion), "The deletion hook did not give up in time - a timed out script must never block the hook indefinitely.");
                }

                Assert.True(tracker.Attempts >= 1, "The script should have been attempted at least once.");
            }
            finally
            {
                TryDelete(scriptFile, outputFile);
            }
        }

        [RavenFact(RavenTestCategory.Configuration)]
        public async Task OnDatabaseDelete_WhenNotConfigured_HookDoesNotRun()
        {
            UseNewLocalServer();

            HookTracker tracker = TrackHook();

            using (var store = GetDocumentStore())
            {
                string databaseName = store.Database;
                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, hardDelete: true));

                await AssertDatabaseDeletedAsync(store, databaseName);
            }

            Assert.False(await WaitForHookAsync(tracker.FirstCompletion, timeoutInSeconds: 5), "No hook should have run when the exec setting is not configured.");
            Assert.Equal(0, tracker.Attempts);
        }
    }
}
