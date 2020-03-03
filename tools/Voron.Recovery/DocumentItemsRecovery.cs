using System.IO;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Extensions;
using Sparrow.Logging;
using Voron.Data;


namespace Voron.Recovery
{
    public static class DocumentItemsRecovery
    {
        public static void WriteCounterItem(CounterGroupDetail counterGroupDetail, DocumentDatabase database, DatabaseDestination databaseDestination, string orphanCountersDocIdPrefix,
            DocumentsOperationContext context, SmugglerResult results, string recoveryLogCollection, Slice attachmentSlice,
            string logDocId, Logger logger, bool skipDocExistence = false)
        {
            if (skipDocExistence == false)
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    using (var originalDoc = database.DocumentsStorage.Get(context, counterGroupDetail.DocumentId))
                    {
                        if (originalDoc == null)
                        {
                            var orphanDocId = $"{orphanCountersDocIdPrefix}/{counterGroupDetail.DocumentId}";
                            using (var orphanDoc = database.DocumentsStorage.Get(context, orphanDocId))
                            {
                                if (orphanDoc == null)
                                {
                                    using (var doc = context.ReadObject(
                                        new DynamicJsonValue
                                        {
                                            [nameof(RecoveryConstants.OriginalDocId)] = counterGroupDetail.DocumentId.ToString(),
                                            [Raven.Client.Constants.Documents.Metadata.Key] = new DynamicJsonValue
                                            {
                                                [Raven.Client.Constants.Documents.Metadata.Collection] = recoveryLogCollection
                                            }
                                        }, orphanDocId, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                                    {
                                        database.DocumentsStorage.Put(context, orphanDocId, null, doc);
                                    }
                                }

                                counterGroupDetail.DocumentId.Dispose();
                                counterGroupDetail.DocumentId = context.GetLazyString(orphanDocId);

                                tx.Commit();
                            }
                        }
                    }
                }
            }

            using var actions = databaseDestination.Counters(results);
            actions.WriteCounter(counterGroupDetail);
        }

        public static void WriteAttachment(string hash, string name, string contentType, Stream attachmentStream, long totalSize,
            DocumentDatabase database, DocumentsOperationContext context, string orphanCountersDocIdPrefix, string recoveryLogCollection, Slice attachmentsSlice,
            string logDocId, Logger logger)
        {
            // store this attachment either under relevant orphan doc, or under already seen doc
            using (var tx = context.OpenWriteTransaction())
            {
                var orphanAttachmentDocId = RecoveredDatabaseCreator.GetOrphanAttachmentDocId(orphanCountersDocIdPrefix, hash);
                var orphanAttachmentDoc = database.DocumentsStorage.Get(context, orphanAttachmentDocId);
                if (orphanAttachmentDoc != null)
                {
                    // check which documents already seen for this attachment and attach it to them
                    orphanAttachmentDoc.Data.Modifications = new DynamicJsonValue(orphanAttachmentDoc.Data);
                    foreach (var docId in orphanAttachmentDoc.Data.GetPropertyNames())
                    {
                        if (docId.Equals(Raven.Client.Constants.Documents.Metadata.Key) || docId.Equals(Raven.Client.Constants.Documents.Metadata.Collection))
                            continue;
                        if (orphanAttachmentDoc.Data.TryGetMember(docId, out var attachmentDataObj) == false)
                            continue;
                        if (!(attachmentDataObj is BlittableJsonReaderObject attachmentData))
                            continue;

                        var seenDoc = database.DocumentsStorage.Get(context, docId);
                        if (seenDoc != null)
                        {
                            if (attachmentData.TryGet(nameof(RecoveryConstants.Name), out string originalName) == false)
                                originalName = name;
                            if (attachmentData.TryGet(nameof(RecoveryConstants.ContentType), out string originalContentType) == false)
                                originalContentType = contentType;
                            orphanAttachmentDoc.Data.Modifications.Remove(docId);
                            var attachmentDetails = database.DocumentsStorage.AttachmentsStorage.PutAttachment(context, docId, originalName, originalContentType, hash, null,
                                attachmentStream);
                            if (attachmentDetails.Size != totalSize)
                            {
                                RecoveredDatabaseCreator.Log(
                                    $"Attachment {originalName} of doc {docId} stream size is {attachmentDetails.Size} which is not as reported in datafile: {totalSize}",
                                    logDocId, logger, context, database);
                            }
                        }
                    }

                    using (var newDocument = context.ReadObject(orphanAttachmentDoc.Data, orphanAttachmentDoc.Id,
                        BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                    {

                        if (newDocument.GetPropertyNames().Length == 1) // 1 for @metadata
                        {
                            database.DocumentsStorage.Delete(context, orphanAttachmentDoc.Id, null);
                        }
                        else
                        {
                            var metadata = orphanAttachmentDoc.Data.GetMetadata();
                            if (metadata != null)
                                metadata.Modifications = new DynamicJsonValue(metadata) {[Raven.Client.Constants.Documents.Metadata.Collection] = recoveryLogCollection};
                            database.DocumentsStorage.Put(context, orphanAttachmentDoc.Id, null, newDocument);
                        }
                    }
                }
                else
                {
                    VoronStream existingStream = null;
                    using (var tree = context.Transaction.InnerTransaction.CreateTree(attachmentsSlice))
                    {
                        existingStream = tree.ReadStream(hash);
                    }
                    // although no previous doc asked for this attachment, it still might appear from previous recovery sessions on the same recovered database. If so - ignore this WriteAttachment call
                    if (existingStream == null)
                    {
                        using (var newDocument = context.ReadObject(
                            new DynamicJsonValue
                            {
                                [Raven.Client.Constants.Documents.Metadata.Key] = new DynamicJsonValue {[Raven.Client.Constants.Documents.Metadata.Collection] = recoveryLogCollection}
                            },
                            orphanAttachmentDocId, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                        {
                            database.DocumentsStorage.Put(context, orphanAttachmentDocId, null, newDocument);
                            database.DocumentsStorage.AttachmentsStorage.PutAttachment(context, orphanAttachmentDocId, name, contentType, hash, null, attachmentStream);
                        }
                    }
                }
                tx.Commit();
            }
        }
    }
}
