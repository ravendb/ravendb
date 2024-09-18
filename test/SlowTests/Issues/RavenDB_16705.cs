﻿using System.IO;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16705 : ReplicationTestBase
    {
        public RavenDB_16705(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Attachments | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task BatchingWithMissingAttachmentsShouldNotCauseReplicationLoop(Options options)
        {
            using (var source = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Replication.MaxItemsCount)] = "1";
                    options.ModifyDatabaseRecord?.Invoke(record);
                }
            }))
            using (var destination = GetDocumentStore(options))
            {
                const string documentId1 = "users/1-A";
                const string documentId2 = "users/2-A$users/1-A";
                const string attachmentName1 = "foo1.png";
                const string attachmentName2 = "foo2.png";
                const string contentType = "image/png";

                using (var session = source.OpenAsyncSession())
                using (var stream1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                using (var stream2 = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    await session.StoreAsync(new User { Name = "Foo" }, documentId1);
                    session.Advanced.Attachments.Store(documentId1, attachmentName1, stream1, contentType);
                    session.Advanced.Attachments.Store(documentId1, attachmentName2, stream2, contentType);
                    await session.SaveChangesAsync();
                }

                using (var session = source.OpenAsyncSession())
                using (var stream1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                using (var stream2 = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    await session.StoreAsync(new User { Name = "Foo" }, documentId2);
                    session.Advanced.Attachments.Store(documentId2, attachmentName1, stream1, contentType);
                    session.Advanced.Attachments.Store(documentId2, attachmentName2, stream2, contentType);
                    await session.SaveChangesAsync();
                }

                var documentDatabase = await GetDocumentDatabaseInstanceForAsync(source, options.DatabaseMode, documentId1);
                var documentsStorage = documentDatabase.DocumentsStorage;

                using (documentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var attachmentStorage = documentsStorage.AttachmentsStorage;

                    ModifyAttachment(attachmentStorage, context, documentId1, attachmentName1, contentType);
                    ModifyAttachment(attachmentStorage, context, documentId1, attachmentName2, contentType);

                    tx.Commit();
                }

                using (documentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var attachmentStorage = documentsStorage.AttachmentsStorage;

                    ModifyAttachment(attachmentStorage, context, documentId2, attachmentName1, contentType);
                    ModifyAttachment(attachmentStorage, context, documentId2, attachmentName2, contentType);

                    tx.Commit();
                }

                await SetupReplicationAsync(source, destination);

                Assert.NotNull(WaitForDocumentWithAttachmentToReplicate<User>(destination, documentId1, attachmentName1, 15 * 1000));
                Assert.NotNull(WaitForDocumentWithAttachmentToReplicate<User>(destination, documentId1, attachmentName2, 15 * 1000));
                Assert.NotNull(WaitForDocumentWithAttachmentToReplicate<User>(destination, documentId2, attachmentName1, 15 * 1000));
                Assert.NotNull(WaitForDocumentWithAttachmentToReplicate<User>(destination, documentId2, attachmentName2, 15 * 1000));

                await EnsureNoReplicationLoopAsync(destination, options.DatabaseMode);
            }

            static void ModifyAttachment(AttachmentsStorage attachmentStorage, DocumentsOperationContext context, string documentId, string attachmentName, string contentType)
            {
                var attachment = attachmentStorage.GetAttachment(context, documentId, attachmentName, AttachmentType.Document, null);

                using (var stream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    attachmentStorage.PutAttachment(context, documentId, attachmentName, contentType, attachment.Base64Hash.ToString(), flags: AttachmentFlags.None, stream.Length, retireAtDt: null, null, stream, updateDocument: false);
                }
            }
        }
    }
}
