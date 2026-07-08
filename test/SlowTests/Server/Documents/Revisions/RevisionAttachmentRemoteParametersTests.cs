using System;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Binary;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Tables;
using Xunit;
using static Raven.Server.Documents.Schemas.Attachments;

namespace SlowTests.Server.Documents.Revisions
{
    // Phase 5 of RavenDB-22358 dropped the WriteRemoteParameters call from the revision-attachment write paths.
    // PutRevisionAttachment (live document-put) and PutRevisionAttachmentDirect (replication-receive +
    // smuggler-import) now hardcode RemoteAttachmentFlags.None / RemoteAt = -1 / empty Identifier, silently
    // converting remote revision attachments into local-looking rows. This test pins the live path.
    public class RevisionAttachmentRemoteParametersTests : RavenTestBase
    {
        public RevisionAttachmentRemoteParametersTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Attachments)]
        public async Task RevisionAttachments_LivePath_PreservesRemoteParameters()
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisionsAsync(store);
                DocumentDatabase database = await Databases.GetDocumentDatabaseInstanceFor(store);

                const string docId = "users/1";
                const string attachmentName = "remote-att";
                const string contentType = "application/octet-stream";
                const string base64Hash = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"; // 44 char base64 sentinel
                const string identifier = "blob-12345";
                var remoteAt = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc);

                // Store the parent doc -- this creates the first revision (no attachments yet).
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Joe" }, docId);
                    await session.SaveChangesAsync();
                }

                // Put a remote attachment on the doc. With revisions enabled, the doc-modify caused by the
                // attachment write triggers a fresh revision -- which fans the attachment into a
                // revision-attachment row via RevisionAttachments -> PutRevisionAttachment.
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (var tx = ctx.OpenWriteTransaction())
                {
                    var remoteParams = new RemoteAttachmentParameters(identifier, remoteAt) { Flags = RemoteAttachmentFlags.Remote };
                    database.DocumentsStorage.AttachmentsStorage.PutAttachment(
                        ctx, docId, attachmentName, contentType, base64Hash, size: 1L,
                        remoteParams: remoteParams, expectedChangeVector: null, stream: null, streamAlreadyInRemoteStorage : true,
                        updateDocument: true, extractCollectionName: true, source: AttachmentsStorage.AttachmentSource.None);
                    tx.Commit();
                }

                // Iterate the attachments table. Document-attachment rows already get remote params right
                // (PutDirect uses WriteRemoteParameters). The bug is on revision-attachment rows: their PK
                // contains the marker `0x1E 'r' 0x1E` after the lowered docId.
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
                using (readCtx.OpenReadTransaction())
                {
                    Table table = readCtx.Transaction.InnerTransaction.OpenTable(
                        database.DocumentsStorage.AttachmentsStorage.AttachmentsSchema,
                        AttachmentsMetadataSlice);

                    int revRowsChecked = 0;
                    unsafe
                    {
                        foreach (var sr in table.SeekByPrimaryKeyPrefix(Slices.BeforeAllKeys, Slices.Empty, 0))
                        {
                            var tvr = sr.Value.Reader;
                            byte* keyPtr = tvr.Read((int)AttachmentsTable.LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType, out int keySize);
                            if (IsRevisionAttachmentKey(keyPtr, keySize) == false)
                                continue;

                            int rawFlags = Bits.SwapBytes(*(int*)tvr.Read((int)AttachmentsTable.Flags, out _));
                            long remoteAtTicks = *(long*)tvr.Read((int)AttachmentsTable.RemoteAt, out _);
                            byte* idPtr = tvr.Read((int)AttachmentsTable.Identifier, out int idSize);
                            string identifierOnRow = idSize == 0 ? string.Empty : Encoding.UTF8.GetString(idPtr, idSize);

                            // Pre-fix the writer hardcodes Flags=None, RemoteAt=-1, Identifier="" -- losing the
                            // remote pointer entirely. Post-fix these match the doc-attachment row.
                            Assert.Equal((int)RemoteAttachmentFlags.Remote, rawFlags);
                            Assert.Equal(remoteAt.Ticks, remoteAtTicks);
                            Assert.Equal(identifier, identifierOnRow);
                            revRowsChecked++;
                        }
                    }
                    Assert.True(revRowsChecked > 0, "Expected at least one revision-attachment row.");
                }
            }
        }

        // C4: PutRevisionAttachmentDirect is the sink that replication-receive and smuggler-import both
        // route through. It must also accept and persist RemoteAttachmentParameters; Phase 5 dropped the
        // parameter from its signature, hardcoding None/-1/empty.
        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Attachments)]
        public async Task PutRevisionAttachmentDirect_PreservesRemoteParameters()
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisionsAsync(store);
                DocumentDatabase database = await Databases.GetDocumentDatabaseInstanceFor(store);

                const string docId = "users/2";
                const string attachmentName = "remote-direct-att";
                const string contentType = "application/octet-stream";
                const string base64Hash = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"; // 44 char base64 sentinel
                const string identifier = "blob-direct-67890";
                var remoteAt = new DateTime(2024, 6, 15, 9, 30, 0, DateTimeKind.Utc);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Jane" }, docId);
                    await session.SaveChangesAsync();
                }

                string docChangeVector;
                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(docId);
                    docChangeVector = session.Advanced.GetChangeVectorFor(user);
                }

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (var tx = ctx.OpenWriteTransaction())
                {
                    using (RevisionsStorage.BuildRevisionKey(ctx, docChangeVector, out RevisionKey revKey))
                    using (DocumentIdWorker.GetLoweredIdSliceFromId(ctx, docId, out Slice lowerDocId))
                    using (DocumentIdWorker.Compatibility.GetLowerIdSliceAndStorageKey(ctx, attachmentName, out Slice lowerName, out Slice nameSlice))
                    using (DocumentIdWorker.Compatibility.GetLowerIdSliceAndStorageKey(ctx, contentType, out Slice lowerContentType, out Slice contentTypeSlice))
                    using (Slice.From(ctx.Allocator, base64Hash, out Slice hashSlice))
                    {
                        var remoteParams = new RemoteAttachmentParameters(identifier, remoteAt) { Flags = RemoteAttachmentFlags.Remote };
                        unsafe
                        {
                            using (AttachmentsStorage.BuildRevisionAttachmentKey(ctx, in revKey,
                                       lowerDocId.Content.Ptr, lowerDocId.Size,
                                       lowerName.Content.Ptr, lowerName.Size,
                                       hashSlice,
                                       lowerContentType.Content.Ptr, lowerContentType.Size,
                                       out RevisionAttachmentKey keyPair))
                            {
                                database.DocumentsStorage.AttachmentsStorage.PutRevisionAttachmentDirect(
                                    ctx, in keyPair, nameSlice, contentTypeSlice, hashSlice, size: 1L,
                                    remoteParams, docChangeVector);
                            }
                        }
                    }
                    tx.Commit();
                }

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
                using (readCtx.OpenReadTransaction())
                {
                    Table table = readCtx.Transaction.InnerTransaction.OpenTable(
                        database.DocumentsStorage.AttachmentsStorage.AttachmentsSchema,
                        AttachmentsMetadataSlice);

                    int found = 0;
                    unsafe
                    {
                        foreach (var sr in table.SeekByPrimaryKeyPrefix(Slices.BeforeAllKeys, Slices.Empty, 0))
                        {
                            var tvr = sr.Value.Reader;
                            byte* keyPtr = tvr.Read((int)AttachmentsTable.LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType, out int keySize);
                            if (IsRevisionAttachmentKey(keyPtr, keySize) == false)
                                continue;

                            int rawFlags = Bits.SwapBytes(*(int*)tvr.Read((int)AttachmentsTable.Flags, out _));
                            long remoteAtTicks = *(long*)tvr.Read((int)AttachmentsTable.RemoteAt, out _);
                            byte* idPtr = tvr.Read((int)AttachmentsTable.Identifier, out int idSize);
                            string identifierOnRow = idSize == 0 ? string.Empty : Encoding.UTF8.GetString(idPtr, idSize);

                            Assert.Equal((int)RemoteAttachmentFlags.Remote, rawFlags);
                            Assert.Equal(remoteAt.Ticks, remoteAtTicks);
                            Assert.Equal(identifier, identifierOnRow);
                            found++;
                        }
                    }
                    Assert.True(found > 0, "Expected revision-attachment row from PutRevisionAttachmentDirect");
                }
            }
        }

        // Revision-attachment PKs are [lowerDocId][0x1E]['r'][0x1E][revCv-or-hash][...].
        // Document-attachment PKs use ['d'] in the same slot. Discriminate on the marker.
        private static unsafe bool IsRevisionAttachmentKey(byte* keyPtr, int keySize)
        {
            for (int i = 0; i < keySize - 2; i++)
            {
                if (keyPtr[i] == 0x1E && keyPtr[i + 1] == (byte)'r' && keyPtr[i + 2] == 0x1E)
                    return true;
            }
            return false;
        }
    }
}
