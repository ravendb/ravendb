using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.Json;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Extensions;
using Sparrow.Logging;


namespace Voron.Recovery
{
    public static class DocumentRecovery
    {
        public static void WriteDocument(Document document, DocumentDatabase database, DatabaseDestination databaseDestination, string orphanCountersDocIdPrefix,
            DocumentsOperationContext context, SmugglerResult results, string recoveryLogCollection, Slice attachmentSlice,
            string logDocId, Logger logger)
        {
            var actions = databaseDestination.Documents();
            WriteDocumentInternal(document, actions, database, databaseDestination, orphanCountersDocIdPrefix, context, results, recoveryLogCollection, attachmentSlice, logDocId, logger);
        }

        public static void WriteRevision(
            Document document, DocumentDatabase database, DatabaseDestination databaseDestination, string orphanCountersDocIdPrefix,
            DocumentsOperationContext context, SmugglerResult results, string recoveryLogCollection, Slice attachmentSlice,
            string logDocId, Logger logger)
        {
            if (database.DocumentsStorage.RevisionsStorage.Configuration == null)
            {
                var revisionsConfiguration = new RevisionsConfiguration {Default = new RevisionsCollectionConfiguration {Disabled = false}};
                var configurationJson = EntityToBlittable.ConvertCommandToBlittable(revisionsConfiguration, context);
                (long index, _) = database.ServerStore.ModifyDatabaseRevisions(context, database.Name, configurationJson, Guid.NewGuid().ToString()).Result;
                AsyncHelpers.RunSync(() => database.RachisLogIndexNotifications.WaitForIndexNotification(index, database.ServerStore.Engine.OperationTimeout));
            }

            var actions = databaseDestination.RevisionDocuments();
            WriteDocumentInternal(document, actions, database, databaseDestination, orphanCountersDocIdPrefix, context, results, recoveryLogCollection, attachmentSlice, logDocId, logger);
        }

        public static void WriteConflict(DocumentConflict conflict, DatabaseDestination databaseDestination, SmugglerResult progress)
        {
            using var actions = databaseDestination.Conflicts();
            actions.WriteConflict(conflict, progress.Conflicts);
        }


        private static void WriteDocumentInternal(
            Document document, IDocumentActions actions, DocumentDatabase database, DatabaseDestination databaseDestination, string orphanCountersDocIdPrefix,
            DocumentsOperationContext context, SmugglerResult progress, string recoveryLogCollection, Slice attachmentsSlice,
            string logDocId, Logger logger)
        {
            BlittableJsonReaderObject metadata = document.Data.GetMetadata();

            bool hadCountersFlag = false;
            if (document.Flags.HasFlag(DocumentFlags.Revision) == false && // revisions contain _snapshot_ of counter, only the current doc will have counter
                document.Flags.HasFlag(DocumentFlags.HasCounters))
            {
                // remove all counter names, and later on search if we saw orphan counters - and add them to this doc
                // after that if counter is discovered it will be written to this existing doc
                metadata.Modifications = new DynamicJsonValue(metadata);
                metadata.Modifications.Remove(Raven.Client.Constants.Documents.Metadata.Counters);
                document.Data.Modifications = new DynamicJsonValue(document.Data) {[Raven.Client.Constants.Documents.Metadata.Key] = metadata};
                document.Flags = document.Flags.Strip(DocumentFlags.HasCounters); // later on counters will add back this flag
                hadCountersFlag = true;
            }


            IMetadataDictionary[] attachments = null;
            if (document.Flags.HasFlag(DocumentFlags.HasAttachments))
            {
                var metadataDictionary = new MetadataAsDictionary(metadata);
                attachments = metadataDictionary.GetObjects(Raven.Client.Constants.Documents.Metadata.Attachments);
                metadata.Modifications = new DynamicJsonValue(metadata);
                metadata.Modifications.Remove(Raven.Client.Constants.Documents.Metadata.Attachments);
                document.Data.Modifications = new DynamicJsonValue(document.Data) {[Raven.Client.Constants.Documents.Metadata.Key] = metadata};

                // Part of the recovery process is stripping DocumentFlags.HasAttachments, writing the doc and then adding the attachments.
                // This _might_ add additional revisions to the recovered database (it will start adding after discovering the first revision..)
                document.Flags = document.Flags.Strip(DocumentFlags.HasAttachments);
            }

            using (document.Data)
                document.Data = context.ReadObject(document.Data, document.Id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);

            var item = new DocumentItem {Document = document};
            actions.WriteDocument(item, progress.Documents);
            actions.Dispose();

            if (hadCountersFlag)
            {
                List<CounterGroupDetail> counterGroupDetailCloneList = null;
                using (var tx = context.OpenWriteTransaction())
                {
                    // Try get orphan counter document.
                    // if hasn't - do nothing, when we face counter it will be stored in this created document
                    var orphanCounterDocId = $"{orphanCountersDocIdPrefix}/{document.Id}";
                    var orphanCounterDoc = database.DocumentsStorage.Get(context, orphanCounterDocId);
                    if (orphanCounterDoc != null)
                    {

                        foreach (var counterGroupDetail in database.DocumentsStorage.CountersStorage.GetCounterValuesForDocument(context, orphanCounterDocId))
                        {
                            var counterGroupDetailClone = new CounterGroupDetail
                            {
                                ChangeVector = context.GetLazyString(counterGroupDetail.ChangeVector.ToString()),
                                DocumentId = context.GetLazyString(document.Id.ToString()), // replace back to the original Id
                                Etag = counterGroupDetail.Etag,
                                Values = counterGroupDetail.Values.Clone(context)
                            };
                            if (counterGroupDetailCloneList == null)
                                counterGroupDetailCloneList = new List<CounterGroupDetail>();
                            counterGroupDetailCloneList.Add(counterGroupDetailClone);
                        }

                        database.DocumentsStorage.Delete(context, orphanCounterDocId, null);
                    }

                    tx.Commit();
                }

                if (counterGroupDetailCloneList != null)
                    foreach (CounterGroupDetail groupDetailClone in counterGroupDetailCloneList)
                        DocumentItemsRecovery.WriteCounterItem(groupDetailClone, database, databaseDestination, orphanCountersDocIdPrefix, context, progress, recoveryLogCollection, attachmentsSlice, logDocId, logger, true);
            }

            if (attachments != null)
            {
                foreach (var attachment in attachments)
                {
                    var hash = attachment.GetString(nameof(AttachmentName.Hash));
                    var name = attachment.GetString(nameof(AttachmentName.Name));
                    var contentType = attachment.GetString(nameof(AttachmentName.ContentType));

                    if (string.IsNullOrEmpty(hash) || string.IsNullOrEmpty(name))
                    {
                        RecoveredDatabaseCreator.Log(
                            $"Document {document.Id} has attachment flag set with empty hash / name",
                            logDocId, logger, context, database);
                        continue;
                    }

                    using (var tx = context.OpenWriteTransaction())
                    {
                        // if attachment already exists, attach it to this doc and remove from orphan
                        // otherwise create a doc consists the hash as doc Id for later use

                        using (var tree = context.Transaction.InnerTransaction.CreateTree(attachmentsSlice))
                        {
                            var orphanAttachmentDocId = RecoveredDatabaseCreator.GetOrphanAttachmentDocId(orphanCountersDocIdPrefix, hash);
                            var existingStream = tree.ReadStream(hash);
                            if (existingStream != null)
                            {
                                // This document points to an attachment which is already in db and already pointed by another document
                                // we just need to point again this document to the attachment
                                try
                                {
                                    database.DocumentsStorage.AttachmentsStorage.PutAttachment(context, document.Id, name, contentType, hash, null,
                                        existingStream);

                                    var noLongerOrphanAttachmentDoc = database.DocumentsStorage.Get(context, orphanAttachmentDocId);
                                    if (noLongerOrphanAttachmentDoc != null)
                                        database.DocumentsStorage.Delete(context, noLongerOrphanAttachmentDoc.Id, null);
                                }
                                finally
                                {
                                    existingStream.Dispose();
                                }
                            }
                            else
                            {
                                var orphanAttachmentDoc = database.DocumentsStorage.Get(context, orphanAttachmentDocId);
                                if (orphanAttachmentDoc == null)
                                {
                                    using (var doc = context.ReadObject(
                                        new DynamicJsonValue
                                        {
                                            [document.Id] = new DynamicJsonValue {[nameof(RecoveryConstants.Name)] = name, [nameof(RecoveryConstants.ContentType)] = contentType},
                                            [Raven.Client.Constants.Documents.Metadata.Key] = new DynamicJsonValue
                                            {
                                                [Raven.Client.Constants.Documents.Metadata.Collection] = recoveryLogCollection
                                            }
                                        }, orphanAttachmentDocId, BlittableJsonDocumentBuilder.UsageMode.ToDisk)
                                    )
                                    {
                                        database.DocumentsStorage.Put(context, orphanAttachmentDocId, null, doc);
                                    }
                                }
                                else
                                {
                                    orphanAttachmentDoc.Data.Modifications = new DynamicJsonValue(orphanAttachmentDoc.Data)
                                    {
                                        [document.Id] = new DynamicJsonValue {[nameof(RecoveryConstants.Name)] = name, [nameof(RecoveryConstants.ContentType)] = contentType}
                                    };
                                    var newDocument = context.ReadObject(orphanAttachmentDoc.Data, orphanAttachmentDoc.Id,
                                        BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                                    database.DocumentsStorage.Put(context, orphanAttachmentDoc.Id, null, newDocument);
                                }
                            }

                            tx.Commit();
                        }
                    }
                }
            }
        }
    }
}
