using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using SlowTests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13341 : ReplicationTestBase
    {
        public RavenDB_13341(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task DatabaseChangeVectorIsUpdatedCorrectly()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                var documentsStorage = documentDatabase.DocumentsStorage;

                using (documentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var lastAttachmentChangeVector = string.Empty;
                    var lastRevisionChangeVector = string.Empty;

                    var attachmentStorage = documentDatabase.DocumentsStorage.AttachmentsStorage;
                    foreach (var document in documentsStorage.GetDocumentsFrom(context, etag: 0))
                    {
                        VerifyAttachments();

                        VerifyRevisions();

                        void VerifyAttachments()
                        {
                            if (document.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
                                return;

                            if (metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                                return;

                            foreach (BlittableJsonReaderObject attachmentFromDocument in attachments)
                            {
                                attachmentFromDocument.TryGet(nameof(AttachmentName.Name), out string attachmentName);
                                var attachment = attachmentStorage.GetAttachment(context, document.Id, attachmentName, AttachmentType.Document, null);
                                Assert.True(document.Etag > attachment.Etag);

                                var conflictStatus = ChangeVectorUtils.GetConflictStatus(document.ChangeVector, attachment.ChangeVector);
                                Assert.Equal(ConflictStatus.Update, conflictStatus);

                                var attachmentConflictStatus = ChangeVectorUtils.GetConflictStatus(attachment.ChangeVector, lastAttachmentChangeVector);
                                if (attachmentConflictStatus == ConflictStatus.Update)
                                    lastAttachmentChangeVector = attachment.ChangeVector;
                            }
                        }

                        void VerifyRevisions()
                        {
                            var revisions = documentsStorage.RevisionsStorage.GetRevisions(context, document.Id, 0, int.MaxValue);
                            foreach (var revision in revisions.Revisions)
                            {
                                var conflictStatus = ChangeVectorUtils.GetConflictStatus(revision.ChangeVector, lastRevisionChangeVector);
                                if (conflictStatus == ConflictStatus.Update)
                                    lastRevisionChangeVector = revision.ChangeVector;
                            }
                        }
                    }

                    var lastChangeVector = DocumentsStorage.GetDatabaseChangeVector(context);

                    var lastCvAttachmentConflictStatus = ChangeVectorUtils.GetConflictStatus(lastAttachmentChangeVector, lastChangeVector);
                    Assert.Equal(ConflictStatus.AlreadyMerged, lastCvAttachmentConflictStatus);

                    var lastCvRevisionConflictStatus = ChangeVectorUtils.GetConflictStatus(lastRevisionChangeVector, lastChangeVector);
                    Assert.Equal(ConflictStatus.AlreadyMerged, lastCvRevisionConflictStatus);
                }
            }
        }

        [Fact]
        public async Task DatabaseChangeVectorIsUpdatedCorrectlyInACluster()
        {
            var databaseName = nameof(DatabaseChangeVectorIsUpdatedCorrectlyInACluster);
            var (_, leader) = await CreateRaftCluster(3, shouldRunInMemory: false);
            var db = await CreateDatabaseInCluster(databaseName, 1, leader.WebUrl);

            var mainServer = db.Servers[0];
            using (var store = GetDocumentStore(new Options
            {
                Server = mainServer,
                ModifyDatabaseName = s => databaseName,
                CreateDatabase = false
            }))
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                const int timeout = 60_000;
                await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(databaseName));
                var val = await WaitForValueAsync(async () => await GetMembersCount(), 2, timeout);
                Assert.Equal(2, val);

                var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                var addedNodeTag = databaseRecord.Topology.Members.Last();
                var addedToServer = Servers.FirstOrDefault(x => x.ServerStore.NodeTag == addedNodeTag);

                await VerifyDocumentsCount();

                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, hardDelete: true, fromNode: addedNodeTag));
                val = await WaitForValueAsync(async () => await GetMembersCount(), 1, timeout);
                Assert.Equal(1, val);
                val = await WaitForValueAsync(async () => await GetDeletedCount(), 0, timeout);
                Assert.Equal(0, val);

                await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(databaseName, addedNodeTag));
                val = await WaitForValueAsync(async () => await GetMembersCount(), 2, timeout);
                Assert.Equal(2, val);

                await VerifyDocumentsCount();

                async Task VerifyDocumentsCount()
                {
                    using (var newReplicatedStore = GetDocumentStore(new Options
                    {
                        Server = addedToServer,
                        ModifyDatabaseName = s => databaseName,
                        CreateDatabase = false,
                        DeleteDatabaseOnDispose = false,
                        ModifyDocumentStore = s => s.Conventions.DisableTopologyUpdates = true
                    }))
                    {
                        var documentsCount = await WaitForValueAsync(async () => (await newReplicatedStore.Maintenance.SendAsync(new GetStatisticsOperation())).CountOfDocuments, 1059);
                        Assert.Equal(1059, documentsCount);
                    }
                }

                async Task<int> GetMembersCount()
                {
                    var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                    if (res == null)
                        return -1;

                    return res.Topology.Members.Count;
                }

                async Task<int> GetDeletedCount()
                {
                    var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                    if (res == null)
                        return -1;

                    return res.DeletionInProgress?.Count ?? 0;
                }
            }
        }

        [Fact]
        public async Task CanReplicateMissingAttachment()
        {
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {
                const string documentId = "users/1-A";
                const string attachmentName = "foo.png";
                const string contentType = "image/png";

                using (var session = source.OpenAsyncSession())
                using (var stream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    await session.StoreAsync(new User { Name = "Foo" }, documentId);
                    session.Advanced.Attachments.Store(documentId, attachmentName, stream, contentType);
                    await session.SaveChangesAsync();
                }

                var documentDatabase = (await GetDocumentDatabaseInstanceFor(source));
                var documentsStorage = documentDatabase.DocumentsStorage;

                using (documentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var attachmentStorage = documentsStorage.AttachmentsStorage;
                    var attachment = attachmentStorage.GetAttachment(context, documentId, attachmentName, AttachmentType.Document, null);

                    using (var stream = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        attachmentStorage.PutAttachment(context, documentId, attachmentName, contentType, attachment.Base64Hash.ToString(), null, stream, updateDocument: false);
                    }

                    tx.Commit();
                }

                await SetupReplicationAsync(source, destination);
                Assert.NotNull(WaitForDocumentWithAttachmentToReplicate<User>(destination, documentId, attachmentName, 15 * 1000));
            }
        }
    }
}
