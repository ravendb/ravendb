using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace InterversionTests.Revisions
{
    // Cross-version revision-tombstone enforcement coverage (DESIGN.md §16; closes S2.1).
    public class MixedTombstoneCrossVersionTests : InterversionTestBase
    {
        public MixedTombstoneCrossVersionTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task EnforceConfiguration_RevisionTombstones_ReplicateAndEvictOnOldPeer()
        {
            var customSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false",
                [RavenConfiguration.GetKey(x => x.Licensing.EulaAccepted)] = "true",
            };

            var oldNode = await GetServerAsync(Versions.PrePRv62, customSettings: customSettings);

            using var newStore = GetDocumentStore(new Options
            {
                Path = NewDataPath(suffix: "new"),
                RunInMemory = false
            });

            var oldDb = GetDatabaseName() + "-old";
            using var oldStore = new DocumentStore
            {
                Urls = new[] { oldNode.Url },
                Database = oldDb
            };
            oldStore.Initialize();
            await oldStore.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(oldDb)
            {
                Settings = { [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false" }
            }));

            // High retention so all 5 puts land as revisions on both peers before eviction is triggered.
            await ConfigureRevisionsAsync(newStore, minToKeep: 100);
            await ConfigureRevisionsAsync(oldStore, minToKeep: 100);

            await SetupExternalReplicationAsync(newStore, oldStore);

            const string docId = "users/mixed-tomb";

            using (var session = newStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "v0" }, docId);
                await session.SaveChangesAsync();
            }
            for (int i = 1; i <= 4; i++)
            {
                using var session = newStore.OpenAsyncSession();
                var u = await session.LoadAsync<User>(docId);
                u.Name = "v" + i;
                await session.SaveChangesAsync();
            }

            await WaitForRevisionsAsync(oldStore, docId, expectedExactly: 5);

            await ConfigureRevisionsAsync(newStore, minToKeep: 1);
            var enforceOp = await newStore.Operations.SendAsync(new EnforceRevisionsConfigurationOperation());
            await enforceOp.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

            await WaitForRevisionsAsync(newStore, docId, expectedExactly: 1);
            await WaitForRevisionsAsync(oldStore, docId, expectedExactly: 1, timeoutMs: 30_000);
        }

        private static async Task ConfigureRevisionsAsync(IDocumentStore store, int minToKeep)
        {
            await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = minToKeep,
                    PurgeOnDelete = false
                }
            }));
        }

        private static async Task SetupExternalReplicationAsync(IDocumentStore src, IDocumentStore dst)
        {
            var csName = $"cs-to-{dst.Database}-{Guid.NewGuid():N}";
            await src.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Name = csName,
                Database = dst.Database,
                TopologyDiscoveryUrls = new[] { dst.Urls[0] }
            }));

            await src.Maintenance.SendAsync(new UpdateExternalReplicationOperation(new ExternalReplication(dst.Database, csName)
            {
                Name = $"ExternalReplicationTo{dst.Database}-{Guid.NewGuid():N}",
                Url = dst.Urls[0]
            }));
        }

        private static async Task WaitForRevisionsAsync(IDocumentStore store, string docId, int expectedExactly, int timeoutMs = 30_000)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            int last = -1;
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                using var session = store.OpenAsyncSession();
                try
                {
                    var metadata = await session.Advanced.Revisions.GetMetadataForAsync(docId, pageSize: 100);
                    last = metadata.Count;
                    if (last == expectedExactly)
                        return;
                }
                catch
                {
                    last = -1;
                }
                await Task.Delay(250);
            }
            Assert.Fail($"Expected exactly {expectedExactly} revisions for {docId} on {store.Urls[0]} within {timeoutMs}ms; final count = {last}.");
        }
    }
}
