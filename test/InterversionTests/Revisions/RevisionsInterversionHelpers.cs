using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace InterversionTests.Revisions
{
    // Shared, store-only helpers for the revisions interversion suites. Static so they're reachable
    // across the different base classes the tests use (OldDataFixture, InterversionTestBase, MixedClusterTestBase).
    internal static class RevisionsInterversionHelpers
    {
        // ---- backup-status helpers ----------------------------------------------------------------
        // (KillSlavedServerProcess(Process) lives on InterversionTestBase -- call it with `node.Process`.)

        // Backup completion via GetPeriodicBackupStatusOperation: status.LastFullBackup transitions to a
        // non-null value once the backup is fully flushed to disk. Replaces file-presence polling which
        // is racy (subdir created before the write completed).
        public static async Task WaitForFullBackupAsync(IDocumentStore store, long taskId, int timeoutMs = 120_000)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                var status = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                if (status?.LastFullBackup != null)
                    return;
                await Task.Delay(500);
            }
            Assert.Fail($"Full backup didn't complete within {timeoutMs}ms.");
        }

        public static async Task<DateTime?> GetLastIncrementalBackupAsync(IDocumentStore store, long taskId)
        {
            var status = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
            return status?.LastIncrementalBackup;
        }

        public static async Task WaitForIncrementalBackupAsync(IDocumentStore store, long taskId, DateTime? after, int timeoutMs = 120_000)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                var status = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                var current = status?.LastIncrementalBackup;
                if (current != null && (after == null || current > after))
                    return;
                await Task.Delay(500);
            }
            Assert.Fail($"Incremental backup didn't complete within {timeoutMs}ms.");
        }

        // ---- revisions config / cv lookup -------------------------------------------------------

        public static Task ConfigureRevisionsAsync(IDocumentStore store, int minToKeep = 100)
        {
            return store.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = minToKeep,
                    PurgeOnDelete = false
                }
            }));
        }

        public static async Task<string> GetLatestRevisionCvAsync(IDocumentStore store, string docId)
        {
            using var session = store.OpenAsyncSession();
            var metadata = await session.Advanced.Revisions.GetMetadataForAsync(docId, pageSize: 1);
            return metadata.Count == 0 ? null : metadata[0].GetString("@change-vector");
        }

        // ---- replication wiring ------------------------------------------------------------------

        // External replication, with Guid-unique connection-string / replication names so a test that
        // wires multiple replications (or re-wires after a tear-down) doesn't collide on the server.
        public static async Task SetupExternalReplicationAsync(IDocumentStore src, IDocumentStore dst)
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

        // ---- revision-count / cv waits -----------------------------------------------------------

        public static async Task WaitForRevisionsAsync(IDocumentStore store, string docId, int expectedAtLeast, int timeoutMs = 30_000)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                using var session = store.OpenAsyncSession();
                try
                {
                    var metadata = await session.Advanced.Revisions.GetMetadataForAsync(docId, pageSize: 100);
                    if (metadata.Count >= expectedAtLeast)
                        return;
                }
                catch { }
                await Task.Delay(250);
            }
            using var s = store.OpenAsyncSession();
            int actual = 0;
            try
            {
                actual = (await s.Advanced.Revisions.GetMetadataForAsync(docId, pageSize: 100)).Count;
            }
            catch { }
            Assert.True(actual >= expectedAtLeast,
                $"Expected at least {expectedAtLeast} revisions for {docId} on {store.Urls[0]}, got {actual}.");
        }

        public static async Task WaitForExactRevisionCountAsync(IDocumentStore store, string docId, int expected, string label, int timeoutMs = 30_000)
        {
            var sw = Stopwatch.StartNew();
            int last = -1;
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                using var session = store.OpenAsyncSession();
                try
                {
                    var metadata = await session.Advanced.Revisions.GetMetadataForAsync(docId, pageSize: 100);
                    last = metadata.Count;
                    if (last == expected) return;
                }
                catch { last = -1; }
                await Task.Delay(250);
            }
            Assert.Fail($"[{label}] doc '{docId}' on {store.Urls[0]}: expected exactly {expected} revisions, got {last}.");
        }

        public static async Task WaitForExactRevisionCvsAsync(IDocumentStore store, string docId, string[] expectedCvsNewestFirst, string label, int timeoutMs = 30_000)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                using var session = store.OpenAsyncSession();
                try
                {
                    var metadata = await session.Advanced.Revisions.GetMetadataForAsync(docId, pageSize: 100);
                    if (metadata.Count == expectedCvsNewestFirst.Length)
                    {
                        bool allMatch = true;
                        for (int i = 0; i < expectedCvsNewestFirst.Length; i++)
                        {
                            if (metadata[i].GetString("@change-vector") != expectedCvsNewestFirst[i]) { allMatch = false; break; }
                        }
                        if (allMatch) return;
                    }
                }
                catch { }
                await Task.Delay(250);
            }
            using var s = store.OpenAsyncSession();
            var actual = await s.Advanced.Revisions.GetMetadataForAsync(docId, pageSize: 100);
            var actualCvs = string.Join(", ", actual.Select(m => m.GetString("@change-vector")));
            Assert.Fail($"[{label}] doc '{docId}' on {store.Urls[0]}: expected CVs [{string.Join(", ", expectedCvsNewestFirst)}], got [{actualCvs}].");
        }

        // ---- doc-presence waits -------------------------------------------------------------------

        public static async Task WaitForDocumentDeletedAsync(IDocumentStore store, string docId, int timeoutMs = 30_000)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                using var session = store.OpenAsyncSession();
                var doc = await session.LoadAsync<User>(docId);
                if (doc == null)
                    return;
                await Task.Delay(250);
            }
            Assert.Fail($"Document '{docId}' was not deleted on {store.Urls[0]} within {timeoutMs}ms.");
        }

        public static async Task WaitForDocumentNameAsync(IDocumentStore store, string docId, string expectedName, int timeoutMs = 30_000)
        {
            var sw = Stopwatch.StartNew();
            User lastSeen = null;
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                using var session = store.OpenAsyncSession();
                lastSeen = await session.LoadAsync<User>(docId);
                if (lastSeen != null && lastSeen.Name == expectedName)
                    return;
                await Task.Delay(250);
            }
            Assert.Fail($"Document '{docId}' on {store.Urls[0]}: expected Name='{expectedName}', got Name='{lastSeen?.Name ?? "<null>"}' within {timeoutMs}ms.");
        }
    }
}
