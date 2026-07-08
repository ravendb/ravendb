using System;
using System.Collections.Generic;
using System.Linq;
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
using static InterversionTests.Revisions.RevisionsInterversionHelpers;

namespace InterversionTests.Revisions
{
    // Cross-version wire-format compat for hash-form composites; post-mitigation regression gate (DESIGN.md §11, §16).
    public class CrossVersionHashFormWireTests : InterversionTestBase
    {
        public CrossVersionHashFormWireTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task Scenario1_HashFormRevisionTombstone_WireToOldPeer_OldPeerCanRead()
        {
            await RunNewToOldDocLifecycleAndAssertOldSeesTombstone();
        }

        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Attachments | RavenTestCategory.Interversion, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task Scenario2_HashFormRevisionAttachment_WireToOldPeer_OldPeerCanRead()
        {
            var (newStore, oldStore) = await SetupBidirectionalRevisionsReplication();

            var docId = "users/att";
            var attachmentBytes = new byte[] { 1, 2, 3, 4, 5 };
            using (var session = newStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "v0" }, docId);
                session.Advanced.Attachments.Store(docId, "att-1", new System.IO.MemoryStream(attachmentBytes), "application/octet-stream");
                await session.SaveChangesAsync();
            }
            using (var session = newStore.OpenAsyncSession())
            {
                var u = await session.LoadAsync<User>(docId);
                u.Name = "v1";
                await session.SaveChangesAsync();
            }

            await WaitForRevisionsAsync(oldStore, docId, expectedAtLeast: 2);

            using (var session = oldStore.OpenAsyncSession())
            {
                var metadata = await session.Advanced.Revisions.GetMetadataForAsync(docId, pageSize: 10);
                Assert.NotEmpty(metadata);
                string revCv = metadata[0].GetString(Raven.Client.Constants.Documents.Metadata.ChangeVector);
                Assert.False(string.IsNullOrEmpty(revCv));

                using var attStream = await session.Advanced.Attachments.GetRevisionAsync(docId, "att-1", revCv);
                Assert.NotNull(attStream);
                using var ms = new System.IO.MemoryStream();
                await attStream.Stream.CopyToAsync(ms);
                Assert.Equal(attachmentBytes, ms.ToArray());
            }
        }

        // No rev-att-tombstone scenario here: EnforceConfiguration reaps revisions but not rev-attachment rows, so no rev-att-tombstones leak via public API.

        [RavenMultiplatformFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Interversion, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task Scenario3_RawFormRevisionTombstone_WireToNewPeer_NewPeerHashes()
        {
            var (newStore, oldStore) = await SetupBidirectionalRevisionsReplication();

            var docId = "users/raw";
            using (var session = oldStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "v0" }, docId);
                await session.SaveChangesAsync();
            }
            using (var session = oldStore.OpenAsyncSession())
            {
                var u = await session.LoadAsync<User>(docId);
                u.Name = "v1";
                await session.SaveChangesAsync();
            }
            using (var session = oldStore.OpenAsyncSession())
            {
                session.Delete(docId);
                await session.SaveChangesAsync();
            }

            await WaitForDocumentDeletedAsync(newStore, docId);
            await WaitForRevisionsAsync(newStore, docId, expectedAtLeast: 2);
        }

        private async Task RunNewToOldDocLifecycleAndAssertOldSeesTombstone()
        {
            var (newStore, oldStore) = await SetupBidirectionalRevisionsReplication();

            var docId = "users/tomb";
            using (var session = newStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "v0" }, docId);
                await session.SaveChangesAsync();
            }
            using (var session = newStore.OpenAsyncSession())
            {
                var u = await session.LoadAsync<User>(docId);
                u.Name = "v1";
                await session.SaveChangesAsync();
            }
            using (var session = newStore.OpenAsyncSession())
            {
                session.Delete(docId);
                await session.SaveChangesAsync();
            }

            // Doc-tombstone path alone isn't a wire check -- the revision-tombstone reachability assertion below is the real gate.
            await WaitForDocumentDeletedAsync(oldStore, docId);

            await WaitForRevisionsAsync(oldStore, docId, expectedAtLeast: 3);

            using (var session = oldStore.OpenAsyncSession())
            {
                var metadata = await session.Advanced.Revisions.GetMetadataForAsync(docId, pageSize: 100);
                var deleteRevisions = metadata.Count(m =>
                {
                    var f = m.GetString("@flags") ?? "";
                    return f.Contains("DeleteRevision");
                });
                Assert.True(deleteRevisions >= 1,
                    $"Old peer must see a DeleteRevision entry for {docId} -- got {deleteRevisions} in {metadata.Count} total revisions.");
            }
        }

        private async Task<(IDocumentStore newStore, IDocumentStore oldStore)> SetupBidirectionalRevisionsReplication()
        {
            var customSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false",
                [RavenConfiguration.GetKey(x => x.Licensing.EulaAccepted)] = "true",
            };

            var oldNode = await GetServerAsync(Versions.PrePRv62, customSettings: customSettings);

            var newStore = GetDocumentStore(new Options
            {
                Path = NewDataPath(suffix: "new"),
                RunInMemory = false
            });

            var oldDb = GetDatabaseName() + "-old";
            var oldStore = new DocumentStore
            {
                Urls = new[] { oldNode.Url },
                Database = oldDb
            };
            oldStore.Initialize();
            await oldStore.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(oldDb)
            {
                Settings = { [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false" }
            }));

            await ConfigureRevisionsAsync(newStore);
            await ConfigureRevisionsAsync(oldStore);

            await SetupExternalReplicationAsync(newStore, oldStore);
            await SetupExternalReplicationAsync(oldStore, newStore);

            return (newStore, oldStore);
        }
    }
}
