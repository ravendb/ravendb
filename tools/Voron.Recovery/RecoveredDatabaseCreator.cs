using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Raven.Client;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Json;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Threading;
using Voron.Data;

namespace Voron.Recovery
{
    public class RecoveredDatabaseCreator : IDisposable
    {
        public static RecoveredDatabaseCreator RecoveredDbTools(DocumentDatabase documentDatabase, string recoverySession, Logger logger) => new RecoveredDatabaseCreator(documentDatabase, recoverySession, logger);

        private readonly DatabaseDestination _databaseDestination;
        private readonly SmugglerResult _results;
        private readonly DocumentsOperationContext _context;
        private readonly DocumentDatabase _database;
        private readonly IDisposable _contextDisposal;
        private readonly Logger _logger;
        private readonly string _logDocId;
        private readonly string _orphanAttachmentsDocIdPrefix;
        private readonly string _orphanCountersDocIdPrefix;
        private readonly Slice _attachmentsSlice;
        private readonly ByteStringContext _byteStringContext;
        private readonly string _recoveryLogCollection;
        private string GetOrphanAttachmentDocId(string hash) => $"{_orphanAttachmentsDocIdPrefix}/{hash}";

        private RecoveredDatabaseCreator(DocumentDatabase documentDatabase, string recoverySession, Logger logger)
        {
            _logger = logger;
            _recoveryLogCollection = $"RecoveryLog-{recoverySession}";
            _orphanAttachmentsDocIdPrefix = $"OrphanAttachments/{recoverySession}";
            _orphanCountersDocIdPrefix = $"OrphanCounters/{recoverySession}";
            _logDocId = $"Log/{recoverySession}";
            _database = documentDatabase;
            _databaseDestination = new DatabaseDestination(documentDatabase);
            _results = new SmugglerResult();
            _databaseDestination.Initialize(new DatabaseSmugglerOptionsServerSide(), _results, ServerVersion.Build);
            _contextDisposal = _database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
            _byteStringContext = new ByteStringContext(SharedMultipleUseFlag.None);
            Slice.From(_byteStringContext, "Attachments", ByteStringType.Immutable, out _attachmentsSlice);

            using (var tx = _context.OpenWriteTransaction())
            {
                using (var doc = _context.ReadObject(
                    new DynamicJsonValue
                    {
                        ["RecoverySession"] = recoverySession,
                        ["RecoveryStarted"] = DateTime.Now,
                        [Constants.Documents.Metadata.Key] = new DynamicJsonValue {[Constants.Documents.Metadata.Collection] = _recoveryLogCollection}
                    }, _logDocId, BlittableJsonDocumentBuilder.UsageMode.ToDisk)
                )
                {
                    _database.DocumentsStorage.Put(_context, _logDocId, null, doc);
                }
                tx.Commit();
            }
        }

        public void WriteDocument(Document document)
        {
            var actions = _databaseDestination.Documents();
            WriteDocumentInternal(document, actions);
        }

        public void WriteRevision(Document document)
        {
            if (_database.DocumentsStorage.RevisionsStorage.Configuration == null)
            {
                var revisionsConfiguration = new RevisionsConfiguration {Default = new RevisionsCollectionConfiguration {Disabled = false}};
                var configurationJson = EntityToBlittable.ConvertCommandToBlittable(revisionsConfiguration, _context);
                (long index, _) = _database.ServerStore.ModifyDatabaseRevisions(_context, _database.Name, configurationJson, Guid.NewGuid().ToString()).Result;
                AsyncHelpers.RunSync(() => _database.RachisLogIndexNotifications.WaitForIndexNotification(index, _database.ServerStore.Engine.OperationTimeout));
            }
            var actions = _databaseDestination.RevisionDocuments();
            WriteDocumentInternal(document, actions);
        }

        public void WriteCounterItem(CounterGroupDetail counterGroupDetail, bool skipDocExistance = false)
        {
            if (skipDocExistance == false)
            {
                using (var tx = _context.OpenWriteTransaction())
                {
                    Document originalDoc = _database.DocumentsStorage.Get(_context, counterGroupDetail.DocumentId);
                    if (originalDoc == null)
                    {
                        var orphanDocId = $"{_orphanCountersDocIdPrefix}/{counterGroupDetail.DocumentId}";
                        var orphanDoc = _database.DocumentsStorage.Get(_context, orphanDocId);
                        if (orphanDoc == null)
                        {
                            using (var doc = _context.ReadObject(
                                new DynamicJsonValue
                                {
                                    ["OriginalDocId"] = counterGroupDetail.DocumentId.ToString(),
                                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue {[Constants.Documents.Metadata.Collection] = _recoveryLogCollection}
                                }, orphanDocId, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                            {
                                var _ = _database.DocumentsStorage.Put(_context, orphanDocId, null, doc);
                            }
                        }

                        counterGroupDetail.DocumentId.Dispose();
                        counterGroupDetail.DocumentId = _context.GetLazyString(orphanDocId);

                        tx.Commit();
                    }
                }
            }

            using var actions = _databaseDestination.Counters(_results);
            actions.WriteCounter(counterGroupDetail);
        }

        public void WriteAttachment(string hash, string name, string contentType, Stream attachmentStream)
        {
            // store this attachment either under relevant orphan doc, or under already seen doc
            using (var tx = _context.OpenWriteTransaction())
            {
                var orphanAttachmentDocId = GetOrphanAttachmentDocId(hash);
                var orphanAttachmentDoc = _database.DocumentsStorage.Get(_context, orphanAttachmentDocId);
                if (orphanAttachmentDoc != null)
                {
                    // check which documents already seen for this attachment and attach it to them
                    orphanAttachmentDoc.Data.Modifications = new DynamicJsonValue(orphanAttachmentDoc.Data);
                    foreach (var docId in orphanAttachmentDoc.Data.GetPropertyNames())
                    {
                        if (docId.Equals(Constants.Documents.Metadata.Key) || docId.Equals(Constants.Documents.Metadata.Collection))
                            continue;
                        if (orphanAttachmentDoc.Data.TryGetMember(docId, out var attachmentDataObj) == false)
                            continue;
                        if (!(attachmentDataObj is BlittableJsonReaderObject attachmentData))
                            continue;

                        var seenDoc = _database.DocumentsStorage.Get(_context, docId);
                        if (seenDoc != null)
                        {
                            if (attachmentData.TryGet("Name", out string originalName) == false)
                                originalName = name;
                            if (attachmentData.TryGet("ContentType", out string originalContentType) == false)
                                originalContentType = contentType;
                            orphanAttachmentDoc.Data.Modifications.Remove(docId);
                            _database.DocumentsStorage.AttachmentsStorage.PutAttachment(_context, docId, originalName, originalContentType, hash, null,
                                attachmentStream);
                        }
                    }

                    using (var newDocument = _context.ReadObject(orphanAttachmentDoc.Data, orphanAttachmentDoc.Id,
                        BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                    {

                        if (newDocument.GetPropertyNames().Length == 1) // 1 for @metadata and @collection
                        {
                            _database.DocumentsStorage.Delete(_context, orphanAttachmentDoc.Id, null);
                        }
                        else
                        {
                            var metadata = orphanAttachmentDoc.Data.GetMetadata();
                            if (metadata != null)
                                metadata.Modifications = new DynamicJsonValue(metadata) {[Constants.Documents.Metadata.Collection] = _recoveryLogCollection};
                            _database.DocumentsStorage.Put(_context, orphanAttachmentDoc.Id, null, newDocument);
                        }
                    }
                }
                else
                {
                    VoronStream existingStream = null;
                    using (var tree = _context.Transaction.InnerTransaction.CreateTree(_attachmentsSlice))
                    {
                        existingStream = tree.ReadStream(hash);
                    }
                    // although no previous doc asked for this attachment, it still might appear from previous recovery sessions. If so - ignore this WriteAttachment call
                    if (existingStream == null)
                    {
                        using (var newDocument = _context.ReadObject(
                            new DynamicJsonValue
                            {
                                [Constants.Documents.Metadata.Key] = new DynamicJsonValue {[Constants.Documents.Metadata.Collection] = _recoveryLogCollection}
                            },
                            orphanAttachmentDocId, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                        {
                            _database.DocumentsStorage.Put(_context, orphanAttachmentDocId, null, newDocument);
                            _database.DocumentsStorage.AttachmentsStorage.PutAttachment(_context, orphanAttachmentDocId, name, contentType, hash, null, attachmentStream);
                        }
                    }
                }
                tx.Commit();
            }
        }

        public void WriteConflict(DocumentConflict conflict)
        {
            using var actions = _databaseDestination.Conflicts();
            actions.WriteConflict(conflict, _results.Conflicts);
        }

        private void WriteDocumentInternal(Document document, IDocumentActions actions)
        {
            BlittableJsonReaderObject metadata = null;

            bool hadCountersFlag = false;
            if (document.Flags.HasFlag(DocumentFlags.Revision) == false && // revisions contain _snapshot_ of counter, only the current doc will have counter
                document.Flags.HasFlag(DocumentFlags.HasCounters))
            {
                metadata = document.Data.GetMetadata();
                if (metadata == null)
                {
                    Log($"Document {document.Id} has counters flag set but was unable to read its metadata and remove the counter names from it");
                    // Logging and storing without counters
                }
                else
                {
                    // remove all counter names, and later on search if we saw orphan counters - and add them to this doc
                    // after that if counter is discovered it will be written to this existing doc
                    metadata.Modifications = new DynamicJsonValue(metadata);
                    metadata.Modifications.Remove(Constants.Documents.Metadata.Counters);
                    document.Data.Modifications = new DynamicJsonValue(document.Data) {[Constants.Documents.Metadata.Key] = metadata};
                }

                document.Flags = document.Flags.Strip(DocumentFlags.HasCounters); // later on counters will add back this flag
                hadCountersFlag = true;
            }


            IMetadataDictionary[] attachments = null;
            if (document.Flags.HasFlag(DocumentFlags.HasAttachments))
            {
                metadata ??= document.Data.GetMetadata();
                if (metadata == null)
                {
                    Log($"Document {document.Id} has attachment flag set but was unable to read its metadata and retrieve the attachments hashes");
                    // Logging and storing without attachments
                }
                else
                {
                    var metadataDictionary = new MetadataAsDictionary(metadata);
                    attachments = metadataDictionary.GetObjects(Constants.Documents.Metadata.Attachments);

                    metadata.Modifications = new DynamicJsonValue(metadata);
                    metadata.Modifications.Remove(Constants.Documents.Metadata.Attachments);
                    document.Data.Modifications = new DynamicJsonValue(document.Data) {[Constants.Documents.Metadata.Key] = metadata};
                }

                // Part of the recovery process is stripping DocumentFlags.HasAttachments, writing the doc and then adding the attachments.
                // This _might_ add additional revisions to the recovered database (it will start adding after discovering the first revision..)
                document.Flags = document.Flags.Strip(DocumentFlags.HasAttachments);
            }

            if (metadata != null)
            {
                using (document.Data)
                {
                    document.Data = _context.ReadObject(document.Data, document.Id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                }
            }

            var item = new DocumentItem {Document = document};
            actions.WriteDocument(item, _results.Documents);
            actions.Dispose();

            if (hadCountersFlag)
            {
                List<CounterGroupDetail> counterGroupDetailCloneList = null;
                using (var tx = _context.OpenWriteTransaction())
                {
                    // Try get orphan counter document.
                    // if hasn't - do nothing, when we face counter it will be stored in this created document
                    var orphanCounterDocId = $"{_orphanCountersDocIdPrefix}/{document.Id}";
                    var orphanCounterDoc = _database.DocumentsStorage.Get(_context, orphanCounterDocId);
                    if (orphanCounterDoc != null)
                    {

                        foreach (var counterGroupDetail in _database.DocumentsStorage.CountersStorage.GetCounterValuesForDocument(_context, orphanCounterDocId))
                        {
                            var counterGroupDetailClone = new CounterGroupDetail
                            {
                                ChangeVector = _context.GetLazyString(counterGroupDetail.ChangeVector.ToString()),
                                DocumentId = _context.GetLazyString(document.Id.ToString()), // replace back to the original Id
                                Etag = counterGroupDetail.Etag,
                                Values = counterGroupDetail.Values.Clone(_context)
                            };
                            if (counterGroupDetailCloneList == null)
                                counterGroupDetailCloneList = new List<CounterGroupDetail>();
                            counterGroupDetailCloneList.Add(counterGroupDetailClone);
                        }

                        _database.DocumentsStorage.Delete(_context, orphanCounterDocId, null);
                    }

                    tx.Commit();
                }

                if (counterGroupDetailCloneList != null)
                    foreach (CounterGroupDetail groupDetailClone in counterGroupDetailCloneList)
                        WriteCounterItem(groupDetailClone, true);
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
                        Log($"Document {document.Id} has attachment flag set with empty hash / name");
                        continue;
                    }

                    using (var tx = _context.OpenWriteTransaction())
                    {
                        // if attachment already exists, attach it to this doc and remove from orphan
                        // otherwise create a doc consists the hash as doc Id for later use

                        using (var tree = _context.Transaction.InnerTransaction.CreateTree(_attachmentsSlice))
                        {
                            var orphanAttachmentDocId = GetOrphanAttachmentDocId(hash);
                            var existingStream = tree.ReadStream(hash);
                            if (existingStream != null)
                            {
                                // This document points to an attachment which is already in db and already pointed by another document
                                // we just need to point again this document to the attachment
                                try
                                {
                                    _database.DocumentsStorage.AttachmentsStorage.PutAttachment(_context, document.Id, name, contentType, hash, null,
                                        existingStream);

                                    var noLongerOrphanAttachmentDoc = _database.DocumentsStorage.Get(_context, orphanAttachmentDocId);
                                    if (noLongerOrphanAttachmentDoc != null)
                                        _database.DocumentsStorage.Delete(_context, noLongerOrphanAttachmentDoc.Id, null);
                                }
                                finally
                                {
                                    existingStream.Dispose();
                                }
                            }
                            else
                            {
                                var orphanAttachmentDoc = _database.DocumentsStorage.Get(_context, orphanAttachmentDocId);
                                if (orphanAttachmentDoc == null)
                                {
                                    using (var doc = _context.ReadObject(
                                        new DynamicJsonValue
                                        {
                                            [document.Id] = new DynamicJsonValue {["Name"] = name, ["ContentType"] = contentType},
                                            [Constants.Documents.Metadata.Key] = new DynamicJsonValue {[Constants.Documents.Metadata.Collection] = _recoveryLogCollection}
                                        }, orphanAttachmentDocId, BlittableJsonDocumentBuilder.UsageMode.ToDisk)
                                    )
                                    {
                                        _database.DocumentsStorage.Put(_context, orphanAttachmentDocId, null, doc);
                                    }
                                }
                                else
                                {
                                    orphanAttachmentDoc.Data.Modifications = new DynamicJsonValue(orphanAttachmentDoc.Data)
                                    {
                                        [document.Id] = new DynamicJsonValue {["Name"] = name, ["ContentType"] = contentType}
                                    };
                                    var newDocument = _context.ReadObject(orphanAttachmentDoc.Data, orphanAttachmentDoc.Id,
                                        BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                                    _database.DocumentsStorage.Put(_context, orphanAttachmentDoc.Id, null, newDocument);
                                }
                            }

                            tx.Commit();
                        }
                    }
                }
            }
        }

        public void Log(string msg, Exception ex = null)
        {
            if (_logger.IsOperationsEnabled)
                _logger.Operations(msg, ex);

            DocumentsTransaction tx = null;
            if (_context.HasTransaction == false)
                tx = _context.OpenWriteTransaction();
            try
            {
                var logDoc = _database.DocumentsStorage.Get(_context, _logDocId);
                if (ex != null)
                    msg += $". Exception: {ex}";
                logDoc.Data.Modifications = new DynamicJsonValue
                {
                    [DateTime.Now.ToString(CultureInfo.InvariantCulture)] = msg
                };
                var newDocument = _context.ReadObject(logDoc.Data, _logDocId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                _database.DocumentsStorage.Put(_context, _logDocId, null, newDocument);
                tx?.Commit();
            }
            finally
            {
                tx?.Dispose();
            }
        }

        public void Dispose()
        {
            using (var tx = _context.OpenWriteTransaction())
            {
                var logDoc = _database.DocumentsStorage.Get(_context, _logDocId);
                logDoc.Data.Modifications = new DynamicJsonValue(logDoc.Data) {["RecoveryFinished"] = DateTime.Now};
                var newDocument = _context.ReadObject(logDoc.Data, _logDocId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                _database.DocumentsStorage.Put(_context, _logDocId, null, newDocument);
                tx.Commit();
            }
            _contextDisposal.Dispose();
            _byteStringContext.Dispose();
        }
    }
}
