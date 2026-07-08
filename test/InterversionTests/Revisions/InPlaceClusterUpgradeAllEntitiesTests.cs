using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Tables;
using Xunit;
using static InterversionTests.Revisions.RevisionsInterversionHelpers;
using static Raven.Server.Documents.Schemas.Attachments;
using static Raven.Server.Documents.Schemas.Revisions;

namespace InterversionTests.Revisions
{
    // In-place cluster upgrade covering all four entities at three binary-mix points; pinned stores -- per-node semantics are load-bearing.
    public class InPlaceClusterUpgradeAllEntitiesTests : MixedClusterTestBase
    {
        public InPlaceClusterUpgradeAllEntitiesTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Cluster | RavenTestCategory.Interversion, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task ClusterInPlaceUpgrade_AllFourEntities_AssertedAtEveryBinaryMix()
        {
            var customSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false",
                [RavenConfiguration.GetKey(x => x.Licensing.EulaAccepted)] = "true",
            };

            var cluster = await CreateCluster(new[] { Versions.PrePRv62, Versions.PrePRv62 }, customSettings: customSettings);
            Assert.Equal(2, cluster.Count);
            var node1 = cluster[0];
            var node2 = cluster[1];

            var dbName = GetDatabaseName();
            using var bootstrapStore = PinnedStore(node1.Url, dbName);

            await bootstrapStore.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(dbName)
            {
                Settings = { [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false" }
            }, replicationFactor: 2));

            await ConfigureRevisionsAsync(bootstrapStore, minToKeep: 2);

            var allDocs = new List<DocSnapshot>();

            // STAGE 1: both old
            allDocs.Add(await CascadeOnNodeAsync(node1, dbName, "users/n1-stage1"));
            allDocs.Add(await CascadeOnNodeAsync(node2, dbName, "users/n2-stage1"));

            await WaitForReplicationConvergenceAsync(node1, node2, dbName, allDocs);

            await AssertAllEntitiesReachableAsync(node1, dbName, allDocs, label: "STAGE 1 on node1 (old)");
            await AssertAllEntitiesReachableAsync(node2, dbName, allDocs, label: "STAGE 1 on node2 (old)");

            // STAGE 2: node1 upgraded
            await UpgradeServerAsync(toVersion: "current", node1, customSettings);

            allDocs.Add(await CascadeOnNodeAsync(node1, dbName, "users/n1-stage2"));
            allDocs.Add(await CascadeOnNodeAsync(node2, dbName, "users/n2-stage2"));

            await WaitForReplicationConvergenceAsync(node1, node2, dbName, allDocs);

            await AssertExactRevisionCountAsync(node1, dbName, allDocs, expected: 2, label: "STAGE 2 on node1 (current, mixed cluster)");
            await AssertExactRevisionCountAsync(node2, dbName, allDocs, expected: 2, label: "STAGE 2 on node2 (old, mixed cluster)");

            // STAGE 3: node2 upgraded
            await UpgradeServerAsync(toVersion: "current", node2, customSettings);

            await Task.Delay(2000);

            await AssertExactRevisionCountAsync(node1, dbName, allDocs, expected: 2, label: "STAGE 3 on node1 (current)");
            await AssertExactRevisionCountAsync(node2, dbName, allDocs, expected: 2, label: "STAGE 3 on node2 (current)");

            // Internal-Voron assertions only valid once both nodes are current bits.
            await AssertStrictEntityRowsAsync(node1, dbName, allDocs, label: "STAGE 3 strict on node1");
            await AssertStrictEntityRowsAsync(node2, dbName, allDocs, label: "STAGE 3 strict on node2");
        }

        private sealed class DocSnapshot
        {
            public string DocId;
            public string CreatedOnNodeUrl;
            public List<string> RevisionCVs = new();    // in cascade order, oldest first
            public string AttachmentName;
            public string AttachmentContentType;
            public string AttachmentParentRevisionCV;          // E3 parent (live attachment)
            public string AttachmentTombstoneParentRevisionCV; // E4 parent (tombstone)
        }
        // Cap=2 means every step past the first two also produces a revision-tombstone, so the cascade fills all four tables.
        private static async Task<DocSnapshot> CascadeOnNodeAsync(InterversionTestBase.ProcessNode node, string dbName, string docId)
        {
            using var store = PinnedStore(node.Url, dbName);

            var snapshot = new DocSnapshot
            {
                DocId = docId,
                CreatedOnNodeUrl = node.Url,
                AttachmentName = "att-1",
                AttachmentContentType = "application/octet-stream"
            };

            using (var s = store.OpenAsyncSession()) { await s.StoreAsync(new User { Name = "v0" }, docId); await s.SaveChangesAsync(); }
            snapshot.RevisionCVs.Add(await GetLatestRevisionCvAsync(store, docId));

            using (var s = store.OpenAsyncSession()) { var u = await s.LoadAsync<User>(docId); u.Name = "v1"; await s.SaveChangesAsync(); }
            snapshot.RevisionCVs.Add(await GetLatestRevisionCvAsync(store, docId));

            using (var s = store.OpenAsyncSession()) { var u = await s.LoadAsync<User>(docId); u.Name = "v2"; await s.SaveChangesAsync(); }
            snapshot.RevisionCVs.Add(await GetLatestRevisionCvAsync(store, docId));

            using (var s = store.OpenAsyncSession())
            {
                var u = await s.LoadAsync<User>(docId);
                s.Advanced.Attachments.Store(u, snapshot.AttachmentName,
                    new MemoryStream(new byte[] { 1, 2, 3, 4 }), snapshot.AttachmentContentType);
                await s.SaveChangesAsync();
            }
            snapshot.AttachmentParentRevisionCV = await GetLatestRevisionCvAsync(store, docId);
            snapshot.RevisionCVs.Add(snapshot.AttachmentParentRevisionCV);

            using (var s = store.OpenAsyncSession())
            {
                var u = await s.LoadAsync<User>(docId);
                s.Advanced.Attachments.Delete(u, snapshot.AttachmentName);
                await s.SaveChangesAsync();
            }
            snapshot.AttachmentTombstoneParentRevisionCV = await GetLatestRevisionCvAsync(store, docId);
            snapshot.RevisionCVs.Add(snapshot.AttachmentTombstoneParentRevisionCV);

            using (var s = store.OpenAsyncSession())
            {
                s.Delete(docId);
                await s.SaveChangesAsync();
            }
            snapshot.RevisionCVs.Add(await GetLatestRevisionCvAsync(store, docId));

            return snapshot;
        }

        // Pinned (no topology updates, no read-balancing) so requests hit only the given URL.
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
        private static async Task WaitForReplicationConvergenceAsync(
            InterversionTestBase.ProcessNode node1,
            InterversionTestBase.ProcessNode node2,
            string dbName,
            List<DocSnapshot> expectedDocs,
            int timeoutMs = 60_000)
        {
            using var s1 = PinnedStore(node1.Url, dbName);
            using var s2 = PinnedStore(node2.Url, dbName);

            var sw = System.Diagnostics.Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                bool allConverged = true;
                foreach (var doc in expectedDocs)
                {
                    int c1 = await RevisionCountAsync(s1, doc.DocId);
                    int c2 = await RevisionCountAsync(s2, doc.DocId);
                    if (c1 < 1 || c2 < 1 || c1 != c2)
                    {
                        allConverged = false;
                        break;
                    }
                }
                if (allConverged)
                    return;
                await Task.Delay(500);
            }
        }

        private static async Task<int> RevisionCountAsync(IDocumentStore store, string docId)
        {
            try
            {
                using var session = store.OpenAsyncSession();
                var md = await session.Advanced.Revisions.GetMetadataForAsync(docId, pageSize: 100);
                return md.Count;
            }
            catch { return -1; }
        }

        private static async Task AssertExactRevisionCountAsync(
            InterversionTestBase.ProcessNode node, string dbName, List<DocSnapshot> docs, int expected, string label)
        {
            using var store = PinnedStore(node.Url, dbName);

            var mismatches = new List<string>();
            foreach (var doc in docs)
            {
                using var session = store.OpenAsyncSession();
                var md = await session.Advanced.Revisions.GetMetadataForAsync(doc.DocId, pageSize: 100);
                if (md.Count != expected)
                {
                    mismatches.Add($"  - '{doc.DocId}' (created on {doc.CreatedOnNodeUrl}): expected exactly {expected} revisions, got {md.Count}");
                }
            }

            Assert.True(mismatches.Count == 0,
                $"[{label}] revision-count mismatch on {node.Url}:\n" + string.Join("\n", mismatches));
        }

        // Client-API check usable on either old or current bits.
        private static async Task AssertAllEntitiesReachableAsync(
            InterversionTestBase.ProcessNode node, string dbName, List<DocSnapshot> docs, string label)
        {
            using var store = PinnedStore(node.Url, dbName);

            foreach (var doc in docs)
            {
                using var session = store.OpenAsyncSession();
                var md = await session.Advanced.Revisions.GetMetadataForAsync(doc.DocId, pageSize: 100);
                Assert.True(md.Count >= 1,
                    $"[{label}] doc '{doc.DocId}': expected >=1 revision, got {md.Count}");

                var latestFlags = md[0].GetString("@flags") ?? "";
                Assert.Contains("DeleteRevision", latestFlags);
            }
        }

        // Current-bits only (requires DocumentDatabase access).
        private async Task AssertStrictEntityRowsAsync(
            InterversionTestBase.ProcessNode node, string dbName, List<DocSnapshot> docs, string label)
        {
            var server = Servers.Single(s => s.WebUrl == node.Url);
            var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(dbName);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                foreach (var doc in docs)
                {
                    // Every evicted-revision CV (the first N-2 entries) must resolve to a revision-tombstone row.
                    var tombTable = ctx.Transaction.InnerTransaction.OpenTable(
                        database.DocumentsStorage.TombstonesSchema, RevisionsTombstonesSlice);

                    using (DocumentIdWorker.Compatibility.GetLowerIdSliceAndStorageKey(ctx, doc.DocId, out Slice lowerIdSlice, out _))
                    {
                        int evictedCount = Math.Max(0, doc.RevisionCVs.Count - 2);
                        for (int i = 0; i < evictedCount; i++)
                        {
                            var evictedCv = doc.RevisionCVs[i];
                            if (string.IsNullOrEmpty(evictedCv))
                                continue;

                            var cv = ctx.GetChangeVector(evictedCv);
                            using (RevisionsStorage.BuildRevisionKeys(ctx, cv, doc.DocId, out var keys))
                            {
                                bool hit = database.DocumentsStorage.RevisionsStorage.TryReadRevisionTombstone(tombTable, in keys.Tombstone, out _);
                                Assert.True(hit, $"[{label}] doc '{doc.DocId}': revision-tombstone for evicted CV '{evictedCv}' not reachable.");
                            }
                        }
                    }

                    using (DocumentIdWorker.Compatibility.GetLowerIdSliceAndStorageKey(ctx, doc.DocId, out Slice docIdSlice, out _))
                    {
                        var attTombTable = ctx.Transaction.InnerTransaction.OpenTable(
                            database.DocumentsStorage.TombstonesSchema, AttachmentsTombstonesSlice);
                        bool found = HasAnyRevisionAttachmentTombstoneForDoc(ctx, attTombTable, docIdSlice);
                        Assert.True(found,
                            $"[{label}] doc '{doc.DocId}': no revision-attachment-tombstone row found for this doc.");
                    }
                }
            }
        }

        private static bool HasAnyRevisionAttachmentTombstoneForDoc(DocumentsOperationContext ctx, Table table, Slice lowerIdSlice)
        {
            unsafe
            {
                int prefixLen = lowerIdSlice.Size + 3;
                Span<byte> buf = stackalloc byte[256];
                if (prefixLen > buf.Length)
                    buf = new byte[prefixLen];

                lowerIdSlice.AsReadOnlySpan().CopyTo(buf);
                buf[lowerIdSlice.Size] = Sparrow.Server.Utils.SpecialChars.RecordSeparator;
                buf[lowerIdSlice.Size + 1] = (byte)'r';
                buf[lowerIdSlice.Size + 2] = Sparrow.Server.Utils.SpecialChars.RecordSeparator;

                using (Slice.From(ctx.Allocator, buf.Slice(0, prefixLen), out Slice prefixSlice))
                {
                    foreach (var _ in table.SeekByPrimaryKeyPrefix(prefixSlice, Slices.Empty, 0))
                        return true;
                }
            }
            return false;
        }
    }
}
