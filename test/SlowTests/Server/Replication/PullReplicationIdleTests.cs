using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Replication
{
    public class PullReplicationIdleTests : ReplicationTestBase
    {
        public PullReplicationIdleTests(ITestOutputHelper output) : base(output)
        {
        }

        [NightlyBuildFact]
        public async Task HubToSink_LocalCvEmpty_OnConnect_HubStaysIdle_AndWakesUpOnlyOnLocalChanges()
        {
            using var context = new PullReplicationTestContext(this);
            await context.Initialize(PullReplicationMode.HubToSink);

            // 1. Initial State:
            // Sink (Initiator) -> Must stay awake to poll.
            // Hub (Passive) -> Can sleep if no changes.
            await using (var monitor = PullReplicationTestContext.Monitor())
            {
                monitor.ExpectWakeup(context.SinkServer, context.SinkDbName);
                monitor.ExpectIdle(context.HubServer, context.HubDbName);
            }

            // 2. Action: Write to Hub
            // Expectation: Hub wakes up to serve the request. Sink continues polling and gets the doc.
            await using (var monitor = PullReplicationTestContext.Monitor())
            {
                monitor.ExpectWakeup(context.HubServer, context.HubDbName);

                // Sink is already awake, but we assert it remains stable (no flickering/sleeping)
                monitor.ExpectWakeup(context.SinkServer, context.SinkDbName);

                using (var session = context.HubStore.OpenSession())
                {
                    session.Store(new User { Name = "HubAwake" }, "users/1");
                    session.SaveChanges();
                }
            }

            Assert.True(WaitForDocument(context.SinkStore, "users/1", timeout: (int)TimeSpan.FromSeconds(15).TotalMilliseconds),
                "Document failed to replicate from Hub to Sink after Hub wakeup.");
        }

        [NightlyBuildFact]
        public async Task HubToSink_LocalCvPopulated_OnConnectWithSinkAhead_HubCapsVector_AndStaysIdle()
        {
            // Validates "Capping" logic:
            // In HubToSink mode, if Sink reports having [Hub:100] but Hub only has [Hub:10],
            // Hub must CAP the Sink's vector to [Hub:10] before comparison.
            // Result: "AlreadyMerged" (Idle).
            // Without capping, logic sees Sink > Hub (Conflict/Update) -> WakeUp.

            using var context = new PullReplicationTestContext(this);
            await context.Initialize(PullReplicationMode.HubToSink);

            // 1. Establish initial state with some data (Hub:1)
            using (var s = context.HubStore.OpenSession())
            {
                s.Store(new User(), "marker");
                s.SaveChanges();
            }
            Assert.True(WaitForDocument(context.SinkStore, "marker"));

            // 2. Wait for Hub to become Idle
            await using (var monitor = PullReplicationTestContext.Monitor())
                monitor.ExpectIdle(context.HubServer, context.HubDbName);

            // 3. Artificially advance Sink's vector (Simulating split-brain or Hub restore)
            // Sink now has [Hub:1, Sink:1]. Hub has [Hub:1].
            // Strict comparison says Sink > Hub.
            // Correct HubToSink logic says: "I don't care what extra data you have, I have nothing NEW for you."
            using (var s = context.SinkStore.OpenSession())
            {
                s.Store(new User(), "sink-only-doc");
                s.SaveChanges();
            }

            await using (var monitor = PullReplicationTestContext.Monitor())
            {
                // Hub should remain IDLE.
                monitor.ExpectIdle(context.HubServer, context.HubDbName);
            }
        }

        [NightlyBuildFact]
        public async Task HubToSink_LocalCvPopulated_OnSinkRestart_HubWakesUp_ToSendNewLocalChanges()
        {
            using var context = new PullReplicationTestContext(this);
            await context.Initialize(PullReplicationMode.HubToSink);

            // 1. Initial State: Hub is Idle
            await using (var monitor = PullReplicationTestContext.Monitor())
                monitor.ExpectIdle(context.HubServer, context.HubDbName);

            // 2. Kill the Sink
            var serverDisposeResult = await DisposeServerAndWaitForFinishOfDisposalAsync(context.SinkServer);

            // 3. Generate changes on Hub while Sink is offline
            // We expect Hub to wake up for write, but then go back to IDLE because Sink is dead (no one is pulling).
            await using (var monitor = PullReplicationTestContext.Monitor())
            {
                monitor.ExpectIdle(context.HubServer, context.HubDbName);

                using (var session = context.HubStore.OpenSession())
                {
                    session.Store(new User { Name = "PendingChange" }, "users/waiting");
                    session.SaveChanges();
                }
            }

            // 4. Resurrect Sink
            // Expectation: As soon as Sink comes online, it connects to Hub. Hub must wake up.
            using (var resurrectedSink = ResurrectServer(serverDisposeResult, context.Certificates))
            using (var newSinkStore = OpenStoreForResurrectedServer(resurrectedSink, context.SinkDbName, context.Certificates))
            {
                await using (var monitor = PullReplicationTestContext.Monitor())
                {
                    monitor.ExpectWakeup(context.HubServer, context.HubDbName);

                    // Just ensure Sink is up and running, triggering the connection
                    WaitForDocument(newSinkStore, "users/waiting", timeout: 5_000);
                }

                // 5. Final Verification
                Assert.True(WaitForDocument(newSinkStore, "users/waiting", timeout: 30_000),
                    "Replication did not resume or Hub did not serve the pending document after Sink resurrection.");
            }
        }

        [NightlyBuildFact]
        public async Task SinkToHub_LocalCvEmpty_OnConnect_SinkStaysAwake_HubStaysIdle_AndHubWakesUpOnSinkChanges()
        {
            using var context = new PullReplicationTestContext(this);
            await context.Initialize(PullReplicationMode.SinkToHub);

            // 1. Initial State:
            // Sink -> Must stay awake: any Sink Pull Replication configuration prevents idle,
            //         because the Hub may come back online while Sink is asleep and nobody would wake it.
            // Hub (Passive) -> Can sleep.
            await using (var monitor = PullReplicationTestContext.Monitor())
            {
                monitor.ExpectWakeup(context.SinkServer, context.SinkDbName);
                monitor.ExpectIdle(context.HubServer, context.HubDbName);
            }

            // 2. Action: Write to Sink
            // Expectation: Hub wakes up to receive incoming replication from Sink.
            await using (var monitor = PullReplicationTestContext.Monitor())
            {
                monitor.ExpectWakeup(context.HubServer, context.HubDbName);

                using (var session = context.SinkStore.OpenSession())
                {
                    session.Store(new User { Name = "SinkAwake" }, "users/1");
                    session.SaveChanges();
                }
            }

            Assert.True(WaitForDocument(context.HubStore, "users/1", timeout: 15_000),
                "Document failed to replicate from Sink to Hub.");
        }

        [NightlyBuildFact]
        public async Task TwoWay_LocalCvEmpty_OnConnect_HubStaysIdle_WhileSinkStaysAwake_AndHubWakesUpOnlyOnSinkChanges()
        {
            using var context = new PullReplicationTestContext(this);
            // Initialize with TwoWay mode (HubToSink | SinkToHub)
            await context.Initialize(PullReplicationMode.HubToSink | PullReplicationMode.SinkToHub);

            // 1. Initial State:
            // Sink (Active Pull) -> Must stay awake.
            // Hub (Passive) -> Can sleep.
            await using (var monitor = PullReplicationTestContext.Monitor())
            {
                monitor.ExpectWakeup(context.SinkServer, context.SinkDbName);
                monitor.ExpectIdle(context.HubServer, context.HubDbName);
            }

            // 2. Scenario A: Write to Hub (HubToSink flow) -> Hub should wake up
            await using (var monitor = PullReplicationTestContext.Monitor())
            {
                monitor.ExpectWakeup(context.SinkServer, context.SinkDbName);
                monitor.ExpectWakeup(context.HubServer, context.HubDbName);

                using (var s = context.HubStore.OpenSession())
                {
                    s.Store(new User { Name = "HubChange" }, "users/1");
                    s.SaveChanges();
                }
            }

            Assert.True(WaitForDocument(context.SinkStore, "users/1", timeout: 15_000), "HubToSink replication failed in TwoWay mode.");

            // 3. Wait for Hub to go back to sleep (to verify independence of operations)
            // We need Hub to be Idle to properly test the next wake-up trigger
            await using (var monitor = PullReplicationTestContext.Monitor())
                monitor.ExpectIdle(context.HubServer, context.HubDbName);

            // 4. Scenario B: Write to Sink (SinkToHub flow) -> Hub should wake up to receive data
            await using (var monitor = PullReplicationTestContext.Monitor())
            {
                monitor.ExpectWakeup(context.HubServer, context.HubDbName);

                using (var s = context.SinkStore.OpenSession())
                {
                    s.Store(new User { Name = "SinkChange" }, "users/2");
                    s.SaveChanges();
                }
            }

            Assert.True(WaitForDocument(context.HubStore, "users/2", timeout: 15_000), "SinkToHub replication failed in TwoWay mode.");
        }

        [NightlyBuildFact]
        public async Task TwoWay_LocalCvPopulated_AfterHubWritesNewData_HubGoesIdleAgain_WithoutFlickering()
        {
            // After Hub writes and Sink receives the data, Sink's CV contains Hub's entries.
            // Hub must stay idle when Sink re-polls — Sink's Hub-origin entries must not trigger a wakeup.
            using var context = new PullReplicationTestContext(this);
            await context.Initialize(PullReplicationMode.HubToSink | PullReplicationMode.SinkToHub);

            // 1. Write to Hub and wait for Sink to receive it
            using (var s = context.HubStore.OpenSession())
            {
                s.Store(new User { Name = "HubChange" }, "users/hub/1");
                s.SaveChanges();
            }

            Assert.True(WaitForDocument(context.SinkStore, "users/hub/1", timeout: 15_000),
                "Document failed to replicate from Hub to Sink.");

            // 2. Hub must go idle and STAY idle — Sink's CV now contains [HubID:1] which must not cause a wakeup
            await using (var monitor = PullReplicationTestContext.Monitor())
                monitor.ExpectIdle(context.HubServer, context.HubDbName);
        }

        [NightlyBuildFact]
        public async Task TwoWay_LocalCvPopulated_AfterSinkSendsNewData_HubGoesIdleAgain_WithoutFlickering()
        {
            // After Hub receives Sink's data and goes idle, Sink re-polls with the same CV.
            // Hub must stay idle — the already-replicated entry must be excluded from the SinkToHub check.
            using var context = new PullReplicationTestContext(this);
            await context.Initialize(PullReplicationMode.HubToSink | PullReplicationMode.SinkToHub);

            // 1. Write to Sink and wait for Hub to receive it
            using (var s = context.SinkStore.OpenSession())
            {
                s.Store(new User { Name = "SinkChange" }, "users/sink/1");
                s.SaveChanges();
            }

            Assert.True(WaitForDocument(context.HubStore, "users/sink/1", timeout: 15_000),
                "Document failed to replicate from Sink to Hub.");

            // 2. Hub must go idle and STAY idle — Sink's CV [SinkID:1] is now in Hub's ReplicationInfo
            await using (var monitor = PullReplicationTestContext.Monitor())
                monitor.ExpectIdle(context.HubServer, context.HubDbName);
        }

        [NightlyBuildFact]
        public async Task TwoWay_LocalCvEmpty_OnConnect_HubConstructsFallbackIdentity_StaysIdle_AndWakesUpOnlyOnSinkChanges()
        {
            // Validates "Identity Crisis" fix:
            // 1. Hub identifies itself via Topology ID (ignoring echoes).
            // 2. But verifies it NOT blind to actual new data from Sink.

            using var context = new PullReplicationTestContext(this);
            await context.Initialize(PullReplicationMode.HubToSink | PullReplicationMode.SinkToHub);

            await using (var monitor = PullReplicationTestContext.Monitor())
            {
                // 1. Verify Silence first (Identity Crisis Handled)
                monitor.ExpectIdle(context.HubServer, context.HubDbName);
                monitor.ExpectWakeup(context.SinkServer, context.SinkDbName);
            }

            // 2. Action: Write to SINK (generating strictly new remote data)
            await using (var monitor = PullReplicationTestContext.Monitor())
            {
                // Hub MUST wake up now because Sink has [Sink:1], which is NOT filtered out by ID check
                monitor.ExpectWakeup(context.HubServer, context.HubDbName);

                using (var s = context.SinkStore.OpenSession())
                {
                    s.Store(new User { Name = "SinkChange" }, "users/sink/1");
                    s.SaveChanges();
                }
            }

            Assert.True(WaitForDocument(context.HubStore, "users/sink/1", timeout: 15_000),
                "Hub failed to replicate document from Sink after correctly handling identity crisis.");
        }

        [NightlyBuildFact]
        public async Task SinkToHub_HubOffline_SinkWritesWhileHubDown_HubReceivesDataOnReturn()
        {
            using var context = new PullReplicationTestContext(this);
            await context.Initialize(PullReplicationMode.SinkToHub);

            // 1. Initial state: Sink is awake (config prevents idle), Hub is idle
            await using (var monitor = PullReplicationTestContext.Monitor())
            {
                monitor.ExpectWakeup(context.SinkServer, context.SinkDbName);
                monitor.ExpectIdle(context.HubServer, context.HubDbName);
            }

            // 2. Kill the Hub
            var hubDisposeResult = await DisposeServerAndWaitForFinishOfDisposalAsync(context.HubServer);

            // 3. Write to Sink while Hub is offline
            using (var session = context.SinkStore.OpenSession())
            {
                session.Store(new User { Name = "BufferedWhileHubDown" }, "users/buffered");
                session.SaveChanges();
            }

            // 4. Sink MUST stay awake despite Hub being unreachable.
            // The anti-flicker check waits MaxIdleTime+3s to confirm Sink never goes idle.
            await using (var monitor = PullReplicationTestContext.Monitor())
            {
                monitor.ExpectWakeup(context.SinkServer, context.SinkDbName);
            }

            // 5. Resurrect Hub
            using (var resurrectedHub = ResurrectServer(hubDisposeResult, context.Certificates))
            using (var newHubStore = OpenStoreForResurrectedServer(resurrectedHub, context.HubDbName, context.Certificates))
            {
                // 6. Sink was awake → reconnects → data flows to Hub
                Assert.True(WaitForDocument(newHubStore, "users/buffered", timeout: 30_000),
                    "Buffered Sink data was not replicated to Hub after it came back online. " +
                    "Sink may have gone idle while Hub was down and failed to wake up on Hub's return.");
            }
        }

        [NightlyBuildFact]
        public async Task TwoWay_HubOffline_SinkWritesWhileHubDown_HubReceivesDataOnReturn()
        {
            using var context = new PullReplicationTestContext(this);
            await context.Initialize(PullReplicationMode.HubToSink | PullReplicationMode.SinkToHub);

            await using (var monitor = PullReplicationTestContext.Monitor())
            {
                monitor.ExpectWakeup(context.SinkServer, context.SinkDbName);
                monitor.ExpectIdle(context.HubServer, context.HubDbName);
            }

            var hubDisposeResult = await DisposeServerAndWaitForFinishOfDisposalAsync(context.HubServer);

            using (var session = context.SinkStore.OpenSession())
            {
                session.Store(new User { Name = "BufferedTwoWay" }, "users/buffered-twoway");
                session.SaveChanges();
            }

            await using (var monitor = PullReplicationTestContext.Monitor())
            {
                monitor.ExpectWakeup(context.SinkServer, context.SinkDbName);
            }

            using (var resurrectedHub = ResurrectServer(hubDisposeResult, context.Certificates))
            using (var newHubStore = OpenStoreForResurrectedServer(resurrectedHub, context.HubDbName, context.Certificates))
            {
                Assert.True(WaitForDocument(newHubStore, "users/buffered-twoway", timeout: 30_000),
                    "TwoWay: Buffered Sink data was not replicated to Hub after it came back online.");
            }
        }

        [NightlyBuildFact]
        public async Task SinkToHub_DisabledConfiguration_SinkCanGoIdle()
        {
            using var context = new PullReplicationTestContext(this);
            await context.Initialize(PullReplicationMode.SinkToHub);

            // 1. Initially Sink stays awake (active config prevents idle)
            await using (var monitor = PullReplicationTestContext.Monitor())
                monitor.ExpectWakeup(context.SinkServer, context.SinkDbName);

            // 2. Disable the pull replication task
            await context.SinkStore.Maintenance.SendAsync(
                new Raven.Client.Documents.Operations.OngoingTasks.ToggleOngoingTaskStateOperation(
                    context.SinkTaskId,
                    Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType.PullReplicationAsSink,
                    disable: true));

            // 3. Sink is now free to go idle (no active replication config)
            await using (var monitor = PullReplicationTestContext.Monitor())
                monitor.ExpectIdle(context.SinkServer, context.SinkDbName);
        }

        #region Helpers

        private class PullReplicationTestContext : IDisposable
        {
            private readonly PullReplicationIdleTests _testBase;
            private static readonly TimeSpan MaxIdleTime = TimeSpan.FromSeconds(10);

            public RavenServer HubServer { get; private set; }
            public RavenServer SinkServer { get; private set; }
            public DocumentStore HubStore { get; private set; }
            public DocumentStore SinkStore { get; private set; }
            public TestCertificatesHolder Certificates { get; private set; }

            public string HubDbName => HubStore.Database;
            public string SinkDbName => SinkStore.Database;
            public long SinkTaskId { get; private set; }

            public PullReplicationTestContext(PullReplicationIdleTests testBase)
            {
                _testBase = testBase;
            }

            public async Task Initialize(PullReplicationMode pullReplicationMode, Dictionary<string, string> customSettings = null, [CallerMemberName] string caller = null)
            {
                var settings = customSettings ?? new Dictionary<string, string>();
                settings[RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = MaxIdleTime.TotalSeconds.ToString(CultureInfo.InvariantCulture);
                settings[RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "1";
                settings[RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false";

                Certificates = _testBase.Certificates.SetupServerAuthentication(customSettings: settings);

                HubServer = _testBase.GetNewServer(new ServerCreationOptions
                {
                    CustomSettings = settings,
                    RegisterForDisposal = false,
                    NodeTag = "Hub"
                });



                _testBase.Certificates.RegisterClientCertificate(
                    Certificates.ServerCertificate.Value,
                    Certificates.ClientCertificate1.Value,
                    new Dictionary<string, DatabaseAccess>(),
                    SecurityClearance.ClusterAdmin,
                    server: HubServer);

                HubStore = _testBase.GetDocumentStore(new Options
                {
                    Server = HubServer,
                    ModifyDatabaseName = s => $"HubDB_{s}",
                    ClientCertificate = Certificates.ServerCertificate.Value,
                    AdminCertificate = Certificates.ClientCertificate1.Value,
                    RunInMemory = false
                });

                SinkServer = _testBase.GetNewServer(new ServerCreationOptions
                {
                    CustomSettings = settings,
                    RegisterForDisposal = false,
                    NodeTag = "Sink"
                });

                _testBase.Certificates.RegisterClientCertificate(
                    Certificates.ServerCertificate.Value,
                    Certificates.ClientCertificate1.Value,
                    new Dictionary<string, DatabaseAccess>(),
                    SecurityClearance.ClusterAdmin,
                    server: SinkServer);

                SinkStore = _testBase.GetDocumentStore(new Options
                {
                    Server = SinkServer,
                    CreateDatabase = true,
                    ModifyDatabaseName = s => $"SinkDB_{s}",
                    ClientCertificate = Certificates.ServerCertificate.Value,
                    AdminCertificate = Certificates.ClientCertificate1.Value,
                    RunInMemory = false
                });

                EnableIdleForTesting(HubServer);
                EnableIdleForTesting(SinkServer);

                await SetupPullReplicationAsync($"{caller}-pull-replication-task", pullReplicationMode);
            }

            public async Task SetupPullReplicationAsync(string pullName, PullReplicationMode mode)
            {
                // 1. Define Hub
                await HubStore.Maintenance.ForDatabase(HubStore.Database).SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition(pullName)
                {
                    Mode = mode
                }));

                // 2. Register Access
                await HubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(pullName, new ReplicationHubAccess
                {
                    Name = "SinkUser",
                    CertificateBase64 = Convert.ToBase64String(Certificates.ClientCertificate2.Value.Export(X509ContentType.Cert))
                }));

                // 3. Define Connection String on Sink
                const string connectionStringName = "ConnectToHub";
                await SinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = connectionStringName,
                    Database = HubStore.Database,
                    TopologyDiscoveryUrls = HubStore.Urls
                }));

                // 4. Define Sink Task
                var sinkResult = await SinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
                {
                    ConnectionStringName = connectionStringName,
                    HubName = pullName,
                    Mode = mode,
                    CertificateWithPrivateKey = Convert.ToBase64String(Certificates.ClientCertificate2.Value.Export(X509ContentType.Pfx))
                }));
                SinkTaskId = sinkResult.TaskId;
            }

            /// <summary>
            /// Creates a monitor to assert database states in parallel.
            /// Usage: await using (var monitor = context.Monitor()) { monitor.Expect...; Action(); }
            /// </summary>
            public static ReplicationActivityMonitor Monitor() => new();

            public class ReplicationActivityMonitor : IAsyncDisposable
            {
                private readonly List<Func<Task>> _verifications = [];
                private readonly List<Action> _cleanups = [];

                public void ExpectWakeup(RavenServer server, string dbName)
                {
                    var wakeupEvent = new ManualResetEventSlim(false);

                    // Subscribe to the global action, but filter specifically for our database
                    Action<string> onWakeup = name =>
                    {
                        if (string.Equals(name, dbName, StringComparison.OrdinalIgnoreCase))
                            wakeupEvent.Set();
                    };

                    server.ServerStore.DatabaseIdleManager.ForTestingPurposesOnly().AfterDatabaseRemovedFromIdle += onWakeup;
                    _cleanups.Add(() => server.ServerStore.DatabaseIdleManager.ForTestingPurposesOnly().AfterDatabaseRemovedFromIdle -= onWakeup);

                    _verifications.Add(async () =>
                    {
                        // 1. Ensure it reaches Active state
                        var state = WaitForValue(() => server.ServerStore.DatabaseIdleManager.GetActivityState(dbName),
                            expectedVal: DatabaseIdleManager.DatabaseActivityState.Active,
                            timeout: (int)TimeSpan.FromSeconds(30).TotalMilliseconds,
                            interval: 500);

                        Assert.True(state == DatabaseIdleManager.DatabaseActivityState.Active,
                            $"Database `{dbName}` is expected to be 'Active' but was '{state}' after timeout.");

                        // 1.5 Workaround test framework race: Wait for the AfterDatabaseRemovedFromIdle
                        // event to be fired by the task continuation if it just became Active
                        await Task.Delay(2000);

                        // 2. Anti-Flicker check: Ensure it STAYS active (doesn't trigger wakeup again)
                        // If AfterDatabaseRemovedFromIdle fires again, it means it went to sleep and woke up.
                        wakeupEvent.Reset();

                        // We wait slightly longer than MaxIdleTime to prove stability
                        var waitTime = MaxIdleTime.Add(TimeSpan.FromSeconds(3));
                        var wasFlickered = wakeupEvent.Wait(waitTime);

                        Assert.False(wasFlickered,
                            $"Database `{dbName}` flickered! It went back to sleep and woke up again immediately.");
                    });
                }

                public void ExpectIdle(RavenServer server, string dbName)
                {
                    var wakeupEvent = new ManualResetEventSlim(false);
                    Action<string> onWakeup = name =>
                    {
                        if (string.Equals(name, dbName, StringComparison.OrdinalIgnoreCase))
                            wakeupEvent.Set();
                    };

                    server.ServerStore.DatabaseIdleManager.ForTestingPurposesOnly().AfterDatabaseRemovedFromIdle += onWakeup;
                    _cleanups.Add(() => server.ServerStore.DatabaseIdleManager.ForTestingPurposesOnly().AfterDatabaseRemovedFromIdle -= onWakeup);

                    _verifications.Add(() => Task.Run(() =>
                    {
                        // 1. Ensure it reaches Idle state
                        var state = WaitForValue(() => server.ServerStore.DatabaseIdleManager.GetActivityState(dbName),
                            expectedVal: DatabaseIdleManager.DatabaseActivityState.Idle,
                            timeout: (int)TimeSpan.FromSeconds(60).TotalMilliseconds,
                            interval: 500);

                        Assert.True(state == DatabaseIdleManager.DatabaseActivityState.Idle,
                             $"Database `{dbName}` is expected to be 'Idle' but was '{state}' after timeout.");

                        // 2. Ensure it STAYS idle (doesn't trigger wakeup)
                        wakeupEvent.Reset();

                        var waitTime = MaxIdleTime.Add(TimeSpan.FromSeconds(3));
                        var wasWokenUp = wakeupEvent.Wait(waitTime);

                        Assert.False(wasWokenUp,
                            $"Database `{dbName}` woke up unexpectedly immediately after going idle.");
                    }));
                }

                public async ValueTask DisposeAsync()
                {
                    try
                    {
                        // Execute all verifications in parallel
                        await Task.WhenAll(_verifications.Select(func => func()));
                    }
                    finally
                    {
                        foreach (var cleanup in _cleanups)
                            cleanup();
                    }
                }
            }

            public void Dispose()
            {
                HubStore?.Dispose();
                SinkStore?.Dispose();

                if (HubServer is { Disposed: false })
                    DisposeServer(HubServer);

                if (SinkServer is { Disposed: false })
                    DisposeServer(SinkServer);
            }
        }

        private static void EnableIdleForTesting(RavenServer server)
        {
            server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().SkipIncreasingLastWorkTimeBasedOnDatabaseSize = true;
            server.ServerStore.DatabaseIdleManager.ForTestingPurposesOnly().SkipShouldContinueDisposeCheck = true;
        }

        private RavenServer ResurrectServer((string DataDirectory, string Url, string NodeTag) serverDisposeResult, TestCertificatesHolder certs)
        {
            var settings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = serverDisposeResult.Url,
                [RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = certs.ServerCertificatePath,
                [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "3",
                [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "1",
                [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false"
            };

            var server = GetNewServer(new ServerCreationOptions
            {
                DeletePrevious = false,
                DataDirectory = serverDisposeResult.DataDirectory,
                CustomSettings = settings,
                RegisterForDisposal = false,
                NodeTag = serverDisposeResult.NodeTag
            });

            EnableIdleForTesting(server);

            return server;
        }

        private DocumentStore OpenStoreForResurrectedServer(RavenServer server, string dbName, TestCertificatesHolder certs)
        {
            return GetDocumentStore(new Options
            {
                Server = server,
                CreateDatabase = false, // DB exists on disk
                ModifyDatabaseName = _ => dbName,
                ClientCertificate = certs.ServerCertificate.Value,
                AdminCertificate = certs.ClientCertificate1.Value,
            });
        }

        #endregion
    }
}
