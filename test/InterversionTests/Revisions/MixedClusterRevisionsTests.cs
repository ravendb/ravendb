using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace InterversionTests.Revisions
{
    public class MixedClusterRevisionsTests : MixedClusterTestBase
    {
        public MixedClusterRevisionsTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task MixedCluster_RevisionReplication_ConvergesInBothDirections()
        {
            var customSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false",
                [RavenConfiguration.GetKey(x => x.Licensing.EulaAccepted)] = "true",
            };

            var cluster = await CreateCluster(new[] { Versions.PrePRv62, Versions.PrePRv62 }, customSettings: customSettings);
            Assert.Equal(2, cluster.Count);

            await UpgradeServerAsync(toVersion: "current", cluster[0], customSettings);
            var newNode = cluster[0];
            var oldNode = cluster[1];

            var databaseName = GetDatabaseName();

            // Pinned to a specific node so the client doesn't re-route via topology discovery.
            using var newStore = PinnedStore(newNode.Url, databaseName);

            var dbRecord = new DatabaseRecord(databaseName)
            {
                Settings =
                {
                    [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false",
                }
            };
            await newStore.Maintenance.Server.SendAsync(new CreateDatabaseOperation(dbRecord, replicationFactor: 2));

            await newStore.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 100,
                    PurgeOnDelete = false
                }
            }));

            using var oldStore = PinnedStore(oldNode.Url, databaseName);

            const string fromNewDocId = "users/from-new";
            using (var session = newStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "v0" }, fromNewDocId);
                await session.SaveChangesAsync();
            }
            var fromNewV0Cv = await GetLatestCvAsync(newStore, fromNewDocId);

            using (var session = newStore.OpenAsyncSession())
            {
                var u = await session.LoadAsync<User>(fromNewDocId);
                u.Name = "v1";
                await session.SaveChangesAsync();
            }
            var fromNewV1Cv = await GetLatestCvAsync(newStore, fromNewDocId);

            await AssertExactRevisionsByCvAsync(
                oldStore, fromNewDocId,
                expectedCvsNewestFirst: new[] { fromNewV1Cv, fromNewV0Cv },
                label: "new->old direction");

            const string fromOldDocId = "users/from-old";
            using (var session = oldStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "v0" }, fromOldDocId);
                await session.SaveChangesAsync();
            }
            var fromOldV0Cv = await GetLatestCvAsync(oldStore, fromOldDocId);

            using (var session = oldStore.OpenAsyncSession())
            {
                var u = await session.LoadAsync<User>(fromOldDocId);
                u.Name = "v1";
                await session.SaveChangesAsync();
            }
            var fromOldV1Cv = await GetLatestCvAsync(oldStore, fromOldDocId);

            await AssertExactRevisionsByCvAsync(
                newStore, fromOldDocId,
                expectedCvsNewestFirst: new[] { fromOldV1Cv, fromOldV0Cv },
                label: "old->new direction");

            using (var session = newStore.OpenAsyncSession())
            {
                session.Delete(fromNewDocId);
                await session.SaveChangesAsync();
            }

            await AssertExactRevisionCountAsync(oldStore, fromNewDocId, expected: 3, label: "new->old doc-delete on old");
            await AssertExactRevisionCountAsync(newStore, fromNewDocId, expected: 3, label: "new->old doc-delete on new");
            await AssertLatestRevisionHasDeleteFlagAsync(oldStore, fromNewDocId);
            await AssertLatestRevisionHasDeleteFlagAsync(newStore, fromNewDocId);
        }

        // Pinned (no topology updates, no read-balancing).
        private static DocumentStore PinnedStore(string url, string database)
        {
            var store = new DocumentStore
            {
                Urls = new[] { url },
                Database = database,
                Conventions = new Raven.Client.Documents.Conventions.DocumentConventions
                {
                    DisableTopologyUpdates = true,
                    ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior.None
                }
            };
            store.Initialize();
            return store;
        }

        private static async Task<string> GetLatestCvAsync(IDocumentStore store, string docId)
        {
            using var session = store.OpenAsyncSession();
            var md = await session.Advanced.Revisions.GetMetadataForAsync(docId, pageSize: 1);
            return md.Count == 0 ? null : md[0].GetString("@change-vector");
        }

        private static async Task AssertExactRevisionsByCvAsync(
            IDocumentStore store, string docId, string[] expectedCvsNewestFirst, string label, int timeoutMs = 30_000)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                using var session = store.OpenAsyncSession();
                try
                {
                    var md = await session.Advanced.Revisions.GetMetadataForAsync(docId, pageSize: 100);
                    if (md.Count == expectedCvsNewestFirst.Length)
                    {
                        bool allMatch = true;
                        for (int i = 0; i < expectedCvsNewestFirst.Length; i++)
                        {
                            if (md[i].GetString("@change-vector") != expectedCvsNewestFirst[i])
                            {
                                allMatch = false;
                                break;
                            }
                        }
                        if (allMatch)
                            return;
                    }
                }
                catch { }
                await Task.Delay(250);
            }

            using var s = store.OpenAsyncSession();
            var actual = await s.Advanced.Revisions.GetMetadataForAsync(docId, pageSize: 100);
            var actualCvs = actual.Select(m => m.GetString("@change-vector")).ToList();
            Assert.Fail(
                $"[{label}] doc '{docId}' on {store.Urls[0]}: expected revisions {string.Join(", ", expectedCvsNewestFirst)}, got {string.Join(", ", actualCvs)}.");
        }

        private static async Task AssertExactRevisionCountAsync(IDocumentStore store, string docId, int expected, string label, int timeoutMs = 30_000)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            int last = -1;
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                using var session = store.OpenAsyncSession();
                try
                {
                    var md = await session.Advanced.Revisions.GetMetadataForAsync(docId, pageSize: 100);
                    last = md.Count;
                    if (last == expected)
                        return;
                }
                catch { last = -1; }
                await Task.Delay(250);
            }
            Assert.Fail($"[{label}] doc '{docId}' on {store.Urls[0]}: expected exactly {expected} revisions, got {last}.");
        }

        private static async Task AssertLatestRevisionHasDeleteFlagAsync(IDocumentStore store, string docId)
        {
            using var session = store.OpenAsyncSession();
            var md = await session.Advanced.Revisions.GetMetadataForAsync(docId, pageSize: 1);
            Assert.True(md.Count >= 1, $"doc '{docId}' on {store.Urls[0]}: no revisions found after delete.");
            var flags = md[0].GetString("@flags") ?? "";
            Assert.Contains("DeleteRevision", flags);
        }
    }
}
