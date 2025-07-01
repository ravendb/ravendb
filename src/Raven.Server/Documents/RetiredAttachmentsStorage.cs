using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents.Handlers.Processors.Attachments;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.DirectDownload;
using Raven.Server.Documents.Replication;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Server.Logging;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using static Raven.Server.Documents.Schemas.Attachments;

namespace Raven.Server.Documents;

public class RetiredAttachmentsStorage : AbstractBackgroundWorkStorage
{
    private DocumentInfoHelper _documentInfoHelper;
    private readonly RavenLogger _logger;
    private const string AttachmentsByRetire = "AttachmentsByRetire";

    public RetiredAttachmentsConfiguration Configuration;

    public RetiredAttachmentsStorage(Transaction tx, DocumentDatabase database) : base(tx, database, AttachmentsByRetire, nameof(AttachmentName.RetireAt))
    {
        _logger = database.Loggers.GetLogger<RetiredAttachmentsStorage>();
    }

    public IDisposable Initialize(DocumentsOperationContext context)
    {
        _documentInfoHelper = new DocumentInfoHelper(context);

        return new DisposableAction(() =>
        {
            _documentInfoHelper.Dispose();
        });
    }

    protected override void ProcessDocument(DocumentsOperationContext context, Slice lowerId, string id, DateTime currentTime)
    {
        var collection = id; // for retire attachments, the id is the collection name
        if (string.IsNullOrEmpty(collection))
            throw new InvalidOperationException($"Couldn't retire the attachment. Document collection is null. Lower id is '{lowerId}'.");
        
        ProcessDocumentForPutRetire(context, lowerId, collection, currentTime);
    }

    private void ProcessDocumentForPutRetire(DocumentsOperationContext context, Slice lowerId, string collection, DateTime currentTime)
    {
        using (var lowerDocId = _documentInfoHelper.GetDocumentId(lowerId))
        {
            if (lowerDocId == null)
            {
                throw new InvalidOperationException($"Couldn't retire the attachment. Document Lower id is '{lowerId}', Document collection is '{collection}'.");
            }

            using (var doc = Database.DocumentsStorage.Get(context, lowerDocId, DocumentFields.Data | DocumentFields.Id, throwOnConflict: true))
            {
                if (doc == null || doc.TryGetMetadata(out var metadata) == false)
                    return;

                if (metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                    return;
                var nameByKey = Database.DocumentsStorage.AttachmentsStorage.GetAttachmentNameByKey(context, lowerId);
                if (nameByKey == null)
                    return;

                for (var i = 0; i < attachments.Length; i++)
                {
                    var attachmentInMetadata = (BlittableJsonReaderObject)attachments[i];
                    if (attachmentInMetadata.TryGet(nameof(AttachmentName.Name), out string name) == false)
                        continue;

                    if (name == nameByKey)
                    {
                        if (HasPassed(attachmentInMetadata, currentTime, MetadataPropertyName) == false)
                            return;

                        Database.DocumentsStorage.AttachmentsStorage.RetireAttachment(context, new AttachmentDetailsServer()
                        {
                            Name = name,
                            DocumentId = doc.Id
                        }, lowerId);

                        break;

                    }
                }
            }
        }
    }

    protected override DocumentExpirationInfo GetDocumentAndIdOrCollection(BackgroundWorkParameters options, Slice clonedId, Slice ticksSlice)
    {
        return DocumentAndIdOrCollectionForPutRetire(options, clonedId, ticksSlice);
    }

    private DocumentExpirationInfo DocumentAndIdOrCollectionInternal(BackgroundWorkParameters options, Slice clonedId, Slice ticksSlice, out Document document, out string id, out string collectionStr)
    {
        document = null;
        collectionStr = null;

        using (var idLsv = _documentInfoHelper.GetDocumentId(clonedId))
        {
            id = idLsv;
            if (id == null)
            {
                return new DocumentExpirationInfo(ticksSlice, clonedId, id: null);
            }
            // document is disposed in caller method
            document = Database.DocumentsStorage.Get(options.Context, id, DocumentFields.Id | DocumentFields.Data | DocumentFields.ChangeVector);
            // doc was deleted
            if (document == null)
            {
                return null;
            }

            if (document.TryGetCollection(out collectionStr))
            {
                if (options.DatabaseRecord.RetiredAttachments == null)
                {
                    // no configuration, we don't care about this collection
                    return new DocumentExpirationInfo(ticksSlice, clonedId, id: null);
                }
            }
        }

        return null;
    }

    private DocumentExpirationInfo DocumentAndIdOrCollectionForPutRetire(BackgroundWorkParameters options, Slice clonedId, Slice ticksSlice)
    {
        var info = DocumentAndIdOrCollectionInternal(options, clonedId, ticksSlice, out var document, out var id, out var collectionStr);
        if (info != null)
            return info;

        if (document == null)
            return new DocumentExpirationInfo(ticksSlice, clonedId, id: null);

        return new DocumentExpirationInfo(ticksSlice, clonedId, id: collectionStr);
    }

    [StorageIndexEntryKeyGenerator]
    internal static unsafe ByteStringContext.Scope GenerateFlagAndHashForAttachments(Transaction tx, ref TableValueReader tvr, out Slice slice)
    {
        var hashPtr = tvr.Read((int)AttachmentsTable.Hash, out var hashSize);

        var flags = *(int*)tvr.Read((int)AttachmentsTable.Flags, out var size);
        Debug.Assert(size == sizeof(int));
        var scope = tx.Allocator.Allocate(sizeof(int) + 1 + hashSize, out var buffer); // flag + record separator + hash

        var span = new Span<byte>(buffer.Ptr, buffer.Length);
        MemoryMarshal.AsBytes(new Span<int>(ref flags)).CopyTo(span);
        buffer.Ptr[sizeof(int)] = SpecialChars.RecordSeparator;
        new ReadOnlySpan<byte>(hashPtr, hashSize).CopyTo(span[(sizeof(int) + 1)..]);

        slice = new Slice(buffer);
        return scope;
    }

    public unsafe void RemoveRetirePutValue(DocumentsOperationContext context, Slice lowerId, long ticks)
    {
        var ticksBigEndian = Bits.SwapBytes(ticks);

        var msg = $"Removing retired attachment put with key: '{lowerId}' from '{_treeName}' tree.";
        if (_logger.IsInfoEnabled)
            _logger.Info(msg);

        var tree = context.Transaction.InnerTransaction.ReadTree(_treeName);
        using (Slice.External(context.Allocator, (byte*)&ticksBigEndian, sizeof(long), out Slice ticksSlice))
            tree.MultiDelete(ticksSlice, lowerId);
    }

    protected override void HandleDocumentConflict(BackgroundWorkParameters options, Slice ticksAsSlice, Slice clonedId, Queue<DocumentExpirationInfo> expiredDocs, ref int totalCount)
    {
        if (ShouldHandleWorkOnCurrentNode(options.DatabaseRecord.Topology, options.NodeTag) == false)
            return;

        using (var docId = _documentInfoHelper.GetDocumentId(clonedId))
        {
            (bool allExpired, string id) = GetConflictedRetiredAttachment(options.Context, options.CurrentTime, docId, clonedId);

            if (allExpired)
            {
                expiredDocs.Enqueue(new DocumentExpirationInfo(ticksAsSlice, clonedId, id));
                totalCount++;
            }
        }
    }

    private (bool AllExpired, string Id) GetConflictedRetiredAttachment(DocumentsOperationContext context, DateTime currentTime, string docId, Slice attachmentKey)
    {
        string collection = null;
        var allExpired = true;
        var conflicts = Database.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, docId);

        if (conflicts.Count <= 0)
            return (true, null);

        foreach (var conflict in conflicts)
        {
            using (conflict)
            {
                collection = conflict.Collection;
                if (conflict.Doc.TryGetMetadata(out var metadata) == false)
                    continue;

                if (metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                    continue;

                var nameByKey = Database.DocumentsStorage.AttachmentsStorage.GetAttachmentNameByKey(context, attachmentKey);
                if (nameByKey == null)
                    continue;
                var found = false;
                var hasPassed = false;
                for (var i = 0; i < attachments.Length; i++)
                {
                    var attachmentInMetadata = (BlittableJsonReaderObject)attachments[i];
                    if (attachmentInMetadata.TryGet(nameof(AttachmentName.Name), out string name) == false)
                        continue;

                    if (name == nameByKey)
                    {
                        found = true;
                        hasPassed = HasPassed(attachmentInMetadata, currentTime, MetadataPropertyName);
                        break;
                    }
                }

                if (found == false)
                    continue;

                if (hasPassed)
                    continue;

                allExpired = false;
                break;
            }
        }

        return (allExpired, collection);
    }

    public DirectFileDownloader GetDownloader(OperationCancelToken tcs)
    {
        if (Configuration == null)
            throw new InvalidOperationException($"Cannot get retired attachment because {nameof(RetiredAttachmentsConfiguration)} is not configured on {Database.Name}.");
        if (Configuration.Disabled)
            throw new InvalidOperationException($"Cannot get retired attachment because {nameof(RetiredAttachmentsConfiguration)} is disabled.");

        var settings = UploaderSettings.GenerateDirectUploaderSetting(Database, nameof(AttachmentHandlerProcessorForGetAttachment), Configuration.S3Settings, Configuration.AzureSettings, glacierSettings: null, googleCloudSettings: null, ftpSettings: null, concurrentThreads: 8);
        return new DirectFileDownloader(settings, retentionPolicyParameters: null, _logger, FileUploaderBase.GenerateUploadResult(), progress => { }, tcs);
    }

    public async Task<Stream> StreamForDownloadDestinationInternal(DirectFileDownloader downloader, string objKeyName)
    {
        var folderName = string.Empty;

        return await downloader.StreamForDownloadDestination(Database, folderName, objKeyName);
    }

    public enum AttachmentRetireType : byte
    {
        PutRetire = 1,
        DeleteRetire = 2,
        ExistingRetire = 3
    }

    public RetireAttachmentsSender UpdateRetiredAttachmentsFromDatabaseRecord(DatabaseRecord dbRecord, RetireAttachmentsSender retireAttachmentsSender)
    {
        try
        {
            if (dbRecord.RetiredAttachments == null)
            {
                Configuration = null;
                retireAttachmentsSender?.Dispose();
                return null;
            }

            if (retireAttachmentsSender != null)
            {
                // no changes
                if (Equals(retireAttachmentsSender.Configuration, dbRecord.RetiredAttachments))
                    return retireAttachmentsSender;
            }

            retireAttachmentsSender?.Dispose();
            Configuration = dbRecord.RetiredAttachments;

            if (dbRecord.RetiredAttachments.Disabled)
                return null;

            var cleaner = new RetireAttachmentsSender(Database, dbRecord.RetiredAttachments);
            cleaner.Start();
            return cleaner;
        }
        catch (Exception e)
        {
            const string msg = $"Cannot enable {nameof(RetireAttachmentsSender)} as the configuration record is not valid.";
            Database.NotificationCenter.Add(AlertRaised.Create(
            Database.Name,
                $"Attachment retirement error in '{Database.Name}'", msg,
                AlertType.RetireAttachmentsConfigurationNotValid, NotificationSeverity.Error, Database.Name));

            if (_logger.IsInfoEnabled)
                _logger.Info(msg, e);

            return null;
        }
    }
}
