using System;
using System.Collections.Generic;
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

        [RavenFact(RavenTestCategory.Replication)]
        public async Task HubToSink_SinkShouldStayAwake_HubShouldGoIdle_AndWakeUpOnChanges()
        {
            using var context = new PullReplicationTestContext(this);
            await context.Initialize(PullReplicationMode.HubToSink);

            // 1. Verify Sink behavior (Initiator) -> Should NOT sleep
            var sinkWakeupEvent = new ManualResetEventSlim(initialState: false);
            context.SinkServer.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().AfterDatabaseRemovedFromIdle = sinkWakeupEvent;

            // Wait > MaxIdleTime (3s) to see if it tries to sleep or flip states.
            var isSinkWokeUp = sinkWakeupEvent.Wait(TimeSpan.FromSeconds(5));
            Assert.False(isSinkWokeUp, "Sink database triggered 'AfterDatabaseRemovedFromIdle', meaning it went to idle and woke up. It should have stayed awake.");
            Assert.False(context.SinkServer.ServerStore.IdleDatabases.ContainsKey(context.SinkDbName), "Sink DB is found in IdleDatabases. It should be awake.");

            // 2. Verify Hub behavior (Target) -> CAN sleep
            PullReplicationTestContext.WaitAndAssertDatabaseIsIdle(context.HubServer, context.HubDbName);

            // 3. Trigger change on Hub
            using (var s = context.HubStore.OpenSession())
            {
                s.Store(new User { Name = "HubAwake" }, "users/1");
                s.SaveChanges();
            }

            // 4. Verify Hub wakes up and replicates
            PullReplicationTestContext.WaitAndAssertDatabaseIsWakeUp(context.HubServer, context.HubDbName);

            Assert.True(WaitForDocument(context.SinkStore, "users/1", timeout: (int)TimeSpan.FromSeconds(10).TotalMilliseconds),
                "Document failed to replicate from Hub to Sink after Hub wakeup.");
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task SinkToHub_BothShouldGoIdle_AndWakeUpOnSinkChanges()
        {
            using var context = new PullReplicationTestContext(this);
            await context.Initialize(PullReplicationMode.SinkToHub);

            // 1. Verify Sink behavior (Initiator, but Push mode) -> CAN sleep
            PullReplicationTestContext.WaitAndAssertDatabaseIsIdle(context.SinkServer, context.SinkDbName);

            // 2. Verify Hub behavior (Target) -> CAN sleep
            PullReplicationTestContext.WaitAndAssertDatabaseIsIdle(context.HubServer, context.HubDbName);

            // 3. Trigger change on Sink
            // This starts the chain reaction: Sink Wakes -> Establishes Connection -> Hub Wakes
            using (var s = context.SinkStore.OpenSession())
            {
                s.Store(new User { Name = "SinkAwake" }, "users/1");
                s.SaveChanges();
            }

            // 4. Verify Sink wakes up
            PullReplicationTestContext.WaitAndAssertDatabaseIsWakeUp(context.SinkServer, context.SinkDbName);

            // 5. Verify Hub wakes up (triggered by incoming replication batch)
            PullReplicationTestContext.WaitAndAssertDatabaseIsWakeUp(context.HubServer, context.HubDbName);

            Assert.True(WaitForDocument(context.HubStore, "users/1", timeout: (int)TimeSpan.FromSeconds(10).TotalMilliseconds),
                "Document failed to replicate from Sink to Hub after waking up.");
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task HubToSink_WhenSinkRestarts_AndHubHasChanges_HubWakesUp()
        {
            using var context = new PullReplicationTestContext(this);
            await context.Initialize(PullReplicationMode.HubToSink);

            // 1. Initial State: Hub is Idle
            PullReplicationTestContext.WaitAndAssertDatabaseIsIdle(context.HubServer, context.HubDbName);

            // 2. Kill the Sink
            var serverDisposeResult = await DisposeServerAndWaitForFinishOfDisposalAsync(context.SinkServer);

            // 3. Generate changes on Hub while Sink is offline
            // This will wake the Hub, but it should go back to sleep because no one is pulling.
            using (var s = context.HubStore.OpenSession())
            {
                s.Store(new User { Name = "PendingChange" }, "users/waiting");
                s.SaveChanges();
            }

            // 4. Wait for Hub to go Idle again (it has changes, but no active replication connection)
            PullReplicationTestContext.WaitAndAssertDatabaseIsIdle(context.HubServer, context.HubDbName);

            // 5. Resurrect Sink
            using (var resurrectedSink = ResurrectServer(serverDisposeResult, context.Certificates))
            using (var newSinkStore = OpenStoreForResurrectedServer(resurrectedSink, context.SinkDbName, context.Certificates))
            {
                // 6. Verify Hub Wakes Up
                // Now that Sink is back, it detects pending changes on Hub (or connects), forcing Hub to wake up.
                PullReplicationTestContext.WaitAndAssertDatabaseIsWakeUp(context.HubServer, context.HubDbName);

                // 7. Verify Data Arrived
                Assert.True(WaitForDocument(newSinkStore, "users/waiting", timeout: 30_000),
                    "Replication did not resume or Hub did not serve the pending document after Sink resurrection.");
            }
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task TwoWay_SinkShouldStayAwake_HubShouldGoIdle_AndWakeUpOnChanges()
        {
            using var context = new PullReplicationTestContext(this);
            // Initialize with TwoWay mode (HubToSink | SinkToHub)
            await context.Initialize(PullReplicationMode.HubToSink | PullReplicationMode.SinkToHub);

            // 1. Verify Sink behavior (Acts as Initiator) -> Should NOT sleep
            // Even though it pushes changes (SinkToHub), it also pulls (HubToSink), so it must maintain the connection/state.
            var sinkWakeupEvent = new ManualResetEventSlim(initialState: false);
            context.SinkServer.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().AfterDatabaseRemovedFromIdle = sinkWakeupEvent;

            // Wait > MaxIdleTime (3s)
            var isSinkWokeUp = sinkWakeupEvent.Wait(TimeSpan.FromSeconds(5));
            Assert.False(isSinkWokeUp, "Sink database triggered 'AfterDatabaseRemovedFromIdle' in TwoWay mode. It should have stayed awake.");
            Assert.False(context.SinkServer.ServerStore.IdleDatabases.ContainsKey(context.SinkDbName), "Sink DB found in IdleDatabases in TwoWay mode.");

            // 2. Verify Hub behavior (Target) -> CAN sleep
            PullReplicationTestContext.WaitAndAssertDatabaseIsIdle(context.HubServer, context.HubDbName);

            // 3. Scenario A: Write to Hub (HubToSink flow) -> Hub should wake up
            using (var s = context.HubStore.OpenSession())
            {
                s.Store(new User { Name = "HubChange" }, "users/1");
                s.SaveChanges();
            }

            PullReplicationTestContext.WaitAndAssertDatabaseIsWakeUp(context.HubServer, context.HubDbName);
            Assert.True(WaitForDocument(context.SinkStore, "users/1", timeout: 15_000), "HubToSink replication failed in TwoWay mode.");

            // 4. Wait for Hub to go back to sleep (to verify independence of operations)
            PullReplicationTestContext.WaitAndAssertDatabaseIsIdle(context.HubServer, context.HubDbName);

            // 5. Scenario B: Write to Sink (SinkToHub flow) -> Hub should wake up to receive data
            using (var s = context.SinkStore.OpenSession())
            {
                s.Store(new User { Name = "SinkChange" }, "users/2");
                s.SaveChanges();
            }

            // Hub wakes up to process incoming replication from Sink
            PullReplicationTestContext.WaitAndAssertDatabaseIsWakeUp(context.HubServer, context.HubDbName);
            Assert.True(WaitForDocument(context.HubStore, "users/2", timeout: 15_000), "SinkToHub replication failed in TwoWay mode.");
        }

        #region Helpers

        private class PullReplicationTestContext : IDisposable
        {
            private readonly PullReplicationIdleTests _testBase;
            public RavenServer HubServer { get; private set; }
            public RavenServer SinkServer { get; private set; }
            public DocumentStore HubStore { get; private set; }
            public DocumentStore SinkStore { get; private set; }
            public TestCertificatesHolder Certificates { get; private set; }

            public string HubDbName => HubStore.Database;
            public string SinkDbName => SinkStore.Database;

            public PullReplicationTestContext(PullReplicationIdleTests testBase)
            {
                _testBase = testBase;
            }

            public async Task Initialize(PullReplicationMode pullReplicationMode, Dictionary<string, string> customSettings = null, [CallerMemberName] string caller = null)
            {
                var settings = customSettings ?? new Dictionary<string, string>();
                settings[RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "3";
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
                    CreateDatabase =  true,
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
                await SinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
                {
                    ConnectionStringName = connectionStringName,
                    HubName = pullName,
                    Mode = mode,
                    CertificateWithPrivateKey = Convert.ToBase64String(Certificates.ClientCertificate2.Value.Export(X509ContentType.Pfx))
                }));
            }

            public static void WaitAndAssertDatabaseIsIdle(RavenServer server, string dbName)
            {
                var value = WaitForValue(() => server.ServerStore.IdleDatabases.ContainsKey(dbName),
                    expectedVal: true,
                    timeout: (int)TimeSpan.FromSeconds(60).TotalMilliseconds,
                    interval: (int)TimeSpan.FromMilliseconds(330).TotalMilliseconds);

                Assert.True(value, $"Database '{dbName}' should be idle, but was not found in IdleDatabases.");
            }

            public static async Task AssertDatabaseIsNotIdle(RavenServer server, string dbName)
            {
                // Wait > MaxIdleTime (3s)
                await Task.Delay(5000);
                Assert.False(server.ServerStore.IdleDatabases.ContainsKey(dbName), $"Database '{dbName}' is found in IdleDatabases collection.");
            }

            public static void WaitAndAssertDatabaseIsWakeUp(RavenServer server, string dbName)
            {
                var value = WaitForValue(() => server.ServerStore.IdleDatabases.ContainsKey(dbName),
                    expectedVal: false,
                    timeout: (int)TimeSpan.FromSeconds(15).TotalMilliseconds,
                    interval: (int)TimeSpan.FromSeconds(1).TotalMilliseconds);

                Assert.False(value, $"Database '{dbName}' should be wake-up, but still was found in IdleDatabases");
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
            server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().SkipShouldContinueDisposeCheck = true;
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
