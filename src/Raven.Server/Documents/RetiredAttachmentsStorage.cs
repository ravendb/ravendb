using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
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
using Sparrow;
using Sparrow.Binary;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Server.Logging;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents;

public class RetiredAttachmentsStorage : AbstractBackgroundWorkStorage
{
    private DocumentInfoHelper _documentInfoHelper;
    private readonly RavenLogger _logger;
    private const string AttachmentsByRetire = "AttachmentsByRetire";

    public RetiredAttachmentsConfiguration Configuration;

    public RetiredAttachmentsStorage(Transaction tx, DocumentDatabase database) : base(tx, database, AttachmentsByRetire, nameof(AttachmentName.RetireParameters.At))
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

    // example key: s3-identifier|categories/1-a|d|image.jpg|S5Opbm22FH1LW5SAC3wRb3HA64QM7odd26djlt5cAkM=|image/jpeg
    private static ByteStringContext<ByteStringMemoryCache>.ExternalScope ExtractIdentifierStringAndDocumentIdSliceFromRetiredAttachmentKey(DocumentsOperationContext context, Slice key, out string identifier, out Slice documentIdSlice)
    {
        identifier = ExtractIdentifierStringFromRetiredAttachmentKey(context, key, out var sizeOfIdentifier);
        var s = sizeOfIdentifier + 1;

        var sizeOfId = AttachmentsStorage.AttachmentKey.FindNextSeparator(key, s) - s;
        var d2 = Slice.External(context.Allocator, key.Content, s, sizeOfId, out documentIdSlice);
        return d2;
    }

    private static LazyStringValue ExtractIdentifierStringFromRetiredAttachmentKey(DocumentsOperationContext context, Slice key, out int sizeOfIdentifier)
    {
        sizeOfIdentifier = AttachmentsStorage.AttachmentKey.FindNextSeparator(key, 0);
        using var d1 = Slice.External(context.Allocator, key.Content, 0, sizeOfIdentifier, out var identifierSlice);
        return GetStringFromIdSlice(context, identifierSlice);
    }

    private static ByteStringContext<ByteStringMemoryCache>.ExternalScope ExtractIdentifierStringAndAttachmentKeySlice(DocumentsOperationContext context, Slice key, out LazyStringValue identifier, out Slice attachmentKeySlice)
    {
        identifier = ExtractIdentifierStringFromRetiredAttachmentKey(context, key, out _);
        var d2 = ExtractAttachmentKeySliceFromRetiredAttachmentKey(context, key, out attachmentKeySlice);

        return d2;
    }

    private static ByteStringContext<ByteStringMemoryCache>.ExternalScope ExtractAttachmentKeySliceFromRetiredAttachmentKey(DocumentsOperationContext context, Slice key, out Slice attachmentKeySlice)
    {
        var sizeOfIdentifier = AttachmentsStorage.AttachmentKey.FindNextSeparator(key, 0);
        var sizeOfAttachmentKey = key.Content.Length - sizeOfIdentifier - 1; // -1 for record separator

        var d2 = Slice.External(context.Allocator, key.Content, sizeOfIdentifier + 1, sizeOfAttachmentKey, out attachmentKeySlice);
        return d2;
    }

    public unsafe ByteStringContext<ByteStringMemoryCache>.ExternalScope ExtractHashSliceFromAttachmentId(DocumentsOperationContext context, Slice key, out Slice hashSlice)
    {
        var p = key.Content.Ptr;
        var size = key.Size;
        var sizeOfIdentifier = AttachmentsStorage.AttachmentKey.GetSizeOfDocId(new ReadOnlySpan<byte>(p, size));
        p = p + sizeOfIdentifier + 1; // skip identifier and record separator
        size = size - sizeOfIdentifier - 1; // skip identifier and record separator
        byte* pp = ExtractHashFromAttachmentIdInternal(p, size);
        var d1 = Slice.External(context.Allocator, pp, AttachmentsStorage.AttachmentHashSize, out hashSlice);

        return d1;
    }

    internal static unsafe byte* ExtractHashFromAttachmentIdInternal(byte* p, int size)
    {
        AttachmentsStorage.ExtractDocIdAndAttachmentNameFromTombstone(p, size, out _, out _, out _, out int attachmentHashIndex);
        var pp = p + attachmentHashIndex;
        return pp;
    }

    protected override unsafe void ProcessDocument(DocumentsOperationContext context, Slice lowerId, string id, DateTime currentTime)
    {
        var sizeOfIdentifier = AttachmentsStorage.AttachmentKey.GetSizeOfDocId(new ReadOnlySpan<byte>(lowerId.Content.Ptr, lowerId.Size));
        using (var lowerDocId = _documentInfoHelper.GetDocumentId(lowerId, sizeOfIdentifier + 1))
        {
            if (lowerDocId == null)
            {
                throw new InvalidOperationException($"Couldn't retire the attachment. Retired attachment key is '{lowerId}'.");
            }

            using (var doc = Database.DocumentsStorage.Get(context, lowerDocId, DocumentFields.Data | DocumentFields.Id, throwOnConflict: true))
            {
                if (doc == null || doc.TryGetMetadata(out var metadata) == false)
                    return;

                if (metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                    return;

                using var attachmentKeyDisposable = ExtractAttachmentKeySliceFromRetiredAttachmentKey(context, lowerId, out Slice attachmentKeySlice);
                var nameByKey = Database.DocumentsStorage.AttachmentsStorage.GetAttachmentNameByKey(context, attachmentKeySlice);
                if (nameByKey == null)
                    return;

                for (var i = 0; i < attachments.Length; i++)
                {
                    var attachmentInMetadata = (BlittableJsonReaderObject)attachments[i];
                    if (attachmentInMetadata.TryGet(nameof(AttachmentName.Name), out string name) == false)
                        continue;
                    if (attachmentInMetadata.TryGet(nameof(AttachmentName.RetireParameters), out BlittableJsonReaderObject retireParamsObject) == false)
                        continue;
                    if (retireParamsObject.TryGet(nameof(RetireAttachmentParameters.Identifier), out LazyStringValue identifierFromMetadata) == false)
                        continue;
                    if (identifierFromMetadata != id)
                        continue;

                    if (name == nameByKey)
                    {
                        if (HasPassed(retireParamsObject, currentTime, MetadataPropertyName) == false)
                            return;

                        Database.DocumentsStorage.AttachmentsStorage.RetireAttachment(context, new AttachmentDetailsServer()
                        {
                            Name = name,
                            DocumentId = doc.Id
                        }, attachmentKeySlice);

                        break;
                    }
                }
            }
        }
    }

    public void Put(DocumentsOperationContext context, Slice lowerId, string processDateString, string identifier)
    {
        using (CreateRetiredAttachmentsKeyWithIdentifier(context, lowerId, identifier, out Slice key))
            base.Put(context, key, processDateString);
    }

    private unsafe ByteStringContext.InternalScope CreateRetiredAttachmentsKeyWithIdentifier(DocumentsOperationContext context, Slice lowerId, string identifier, out Slice outSlice)
    {
        // something like: my-s3-cool-storage|categories/1-a|d|image.jpg|S5Opbm22FH1LW5SAC3wRb3HA64QM7odd26djlt5cAkM=|image/jpeg
        using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, identifier, out _, out Slice identifierSlice))
        {
            var size = identifierSlice.Content.Length + 1 + lowerId.Content.Length; // identifier + record separator + lowerId 
            var scope = context.Allocator.Allocate(size, out ByteString keyMem);
            var pos = 0;
            Memory.Copy(keyMem.Ptr + pos, identifierSlice.Content.Ptr, identifierSlice.Content.Length);
            pos = identifierSlice.Content.Length;
            keyMem.Ptr[pos++] = SpecialChars.RecordSeparator;
            Memory.Copy(keyMem.Ptr + pos, lowerId.Content.Ptr, lowerId.Content.Length);

            outSlice = new Slice(SliceOptions.Key, keyMem);
            return scope;
        }
    }

    protected override DocumentExpirationInfo GetDocumentAndId(BackgroundWorkParameters options, Slice clonedId, Slice ticksSlice)
    {
        using (ExtractIdentifierStringAndDocumentIdSliceFromRetiredAttachmentKey(options.Context, clonedId, out string identifier, out Slice documentIdSlice))
        {
            if (Database.DocumentsStorage.GetTableValueReaderForDocument(options.Context, documentIdSlice, throwOnConflict: false, out _) == false)
            {
                // doc was deleted
                return new DocumentExpirationInfo(ticksSlice, clonedId, id: null, DocumentExpirationInfoStatus.Delete);
            }

            if (ShouldSkipItem( identifier))
            {
                return new DocumentExpirationInfo(ticksSlice, clonedId, id: identifier, DocumentExpirationInfoStatus.Skip);
            }

            return new DocumentExpirationInfo(ticksSlice, clonedId, id: identifier, DocumentExpirationInfoStatus.Process);
        }
    }

    private bool ShouldSkipItem(string identifier)
    {
        if (Configuration?.Destinations == null)
        {
            return true;
        }

        if (Configuration.Destinations.Count == 0)
        {
            return true;
        }

        if (Configuration.Destinations.TryGetValue(identifier, out var destination) == false || destination.HasUploader() == false)
        {
            // no destinations, we don't care about this attachment
            return true;
        }

        return false;
    }

    [StorageIndexEntryKeyGenerator]
    internal static unsafe ByteStringContext.Scope GenerateFlagAndHashForAttachments(Transaction tx, ref TableValueReader tvr, out Slice slice)
    {
        var hashPtr = tvr.Read((int)Schemas.Attachments.AttachmentsTable.Hash, out var hashSize);

        var flags = *(int*)tvr.Read((int)Schemas.Attachments.AttachmentsTable.Flags, out var size);
        Debug.Assert(size == sizeof(int));
        var scope = tx.Allocator.Allocate(sizeof(int) + 1 + hashSize, out var buffer); // flag + record separator + hash

        var span = new Span<byte>(buffer.Ptr, buffer.Length);
        MemoryMarshal.AsBytes(new Span<int>(ref flags)).CopyTo(span);
        buffer.Ptr[sizeof(int)] = SpecialChars.RecordSeparator;
        new ReadOnlySpan<byte>(hashPtr, hashSize).CopyTo(span[(sizeof(int) + 1)..]);

        slice = new Slice(buffer);
        return scope;
    }

    public unsafe void RemoveRetirePutValue(DocumentsOperationContext context, Slice lowerId, string identifier, long ticks)
    {
        var ticksBigEndian = Bits.SwapBytes(ticks);

        var msg = $"Removing retired attachment put with key: '{lowerId}' from '{_treeName}' tree.";
        if (_logger.IsDebugEnabled)
            _logger.Debug(msg);

        var tree = context.Transaction.InnerTransaction.ReadTree(_treeName);

        using (CreateRetiredAttachmentsKeyWithIdentifier(context, lowerId, identifier, out Slice key))
        using (Slice.External(context.Allocator, (byte*)&ticksBigEndian, sizeof(long), out Slice ticksSlice))
            tree.MultiDelete(ticksSlice, key);
    }

    protected override void HandleDocumentConflict(BackgroundWorkParameters options, Slice ticksAsSlice, Slice clonedId, Queue<DocumentExpirationInfo> expiredDocs, ref int totalCount)
    {
        (bool allShouldRetire, string id) = GetConflictedRetiredAttachment(options.Context, options.CurrentTime, clonedId);

        if (allShouldRetire)
        {
            expiredDocs.Enqueue(new DocumentExpirationInfo(ticksAsSlice, clonedId, id, DocumentExpirationInfoStatus.Process));
            totalCount++;
        }
    }

    private const string AlertTitle = "Remote Attachments";
    private const string WarnMessage = "A retired attachment was skipped.";
    private long _counter = 0;

    protected override void HandleSkippedItem(DocumentExpirationInfo item)
    {
        if (_logger.IsDebugEnabled)
        {
            _logger.Debug($"Skipping retired attachment '{item.LowerId}' with identifier '{item.Id}'");
        }

        if (_counter++ % 1024 == 0)
        {
            var alert = AlertRaised.Create(Database.Name, AlertTitle, WarnMessage, AlertType.Attachments_RemoteAttachmentWithoutIdentifier, NotificationSeverity.Warning, key: nameof(AlertType.Attachments_RemoteAttachmentWithoutIdentifier));
            Database.NotificationCenter.Add(alert);
        }
    }

    private unsafe (bool AllHasPassed, string Id) GetConflictedRetiredAttachment(DocumentsOperationContext context, DateTime currentTime, Slice clonedId)
    {
        var sizeOfIdentifier = AttachmentsStorage.AttachmentKey.GetSizeOfDocId(new ReadOnlySpan<byte>(clonedId.Content.Ptr, clonedId.Size));
        using (var docId = _documentInfoHelper.GetDocumentId(clonedId, sizeOfIdentifier + 1))
        {
            var conflicts = Database.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, docId);

            if (conflicts.Count <= 0)
                return (true, null);

            var allHashPassed = true;
            LazyStringValue identifier = null;

            foreach (var conflict in conflicts)
            {
                using (conflict)
                {
                    if (conflict.Doc.TryGetMetadata(out var metadata) == false)
                        continue;

                    if (metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                        continue;

                    using var d = ExtractIdentifierStringAndAttachmentKeySlice(context, clonedId, out identifier, out var attachmentKeySlice);
                    var nameByKey = Database.DocumentsStorage.AttachmentsStorage.GetAttachmentNameByKey(context, attachmentKeySlice);
                    if (nameByKey == null)
                        continue;
                    var found = false;
                    var hasPassed = false;
                    for (var i = 0; i < attachments.Length; i++)
                    {
                        var attachmentInMetadata = (BlittableJsonReaderObject)attachments[i];
                        if (attachmentInMetadata.TryGet(nameof(AttachmentName.Name), out string name) == false)
                            continue;
                        if (attachmentInMetadata.TryGet(nameof(AttachmentName.RetireParameters), out BlittableJsonReaderObject retireParamsObject) == false)
                            continue;
                        if (retireParamsObject.TryGet(nameof(RetireAttachmentParameters.Identifier), out LazyStringValue identifierFromMetadata) == false)
                            continue;
                        if (identifierFromMetadata != identifier)
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

                    allHashPassed = false;
                    break;
                }
            }

            return (allHashPassed, identifier);
        }
    }

    public DirectFileDownloader GetDownloader(Attachment attachment, OperationCancelToken tcs)
    {
        if (Configuration == null)
            throw new InvalidOperationException($"Cannot get retired attachment '{attachment.Key}' because {nameof(RetiredAttachmentsConfiguration)} is not configured on {Database.Name}.");
        if (Configuration.Destinations.TryGetValue(attachment.RetireParameters.Identifier, out var destination) == false)
            throw new InvalidOperationException($"Cannot get retired attachment '{attachment.Key}' because destination for '{attachment.RetireParameters.Identifier}' doesn't exist.");
        if (destination.Disabled)
            throw new InvalidOperationException($"Cannot get retired attachment '{attachment.Key}' because destination for '{attachment.RetireParameters.Identifier}' is disabled.");

        var settings = UploaderSettings.GenerateDirectUploaderSettingsForAttachments(Database, nameof(AttachmentHandlerProcessorForGetAttachment), destination.S3Settings, destination.AzureSettings);
        return new DirectFileDownloader(settings, tcs);
    }

    public Task<Stream> StreamForDownloadDestinationInternal(DirectFileDownloader downloader, string objKeyName)
    {
        var folderName = string.Empty;

        return downloader.StreamForDownloadDestination(Database, folderName, objKeyName);
    }

    private static unsafe LazyStringValue GetStringFromIdSlice(DocumentsOperationContext context, Slice identifierSlice)
    {
        var lzs = context.GetLazyStringValue(identifierSlice.Content.Ptr, out bool success);
        if (success == false)
        {
            throw new InvalidOperationException($"Failed to get string from id: {identifierSlice}");
        }

        return lzs;
    }

    public long TryUpdateRetiredAttachment(DocumentsOperationContext context, DateTime? newRetireAtDt, long currentDt, string newIdentifier, string currentIdentifier, Slice keySlice)
    {
        // Handle case where there's no current retirement date
        if (currentDt == -1L)
        {
            TryPutRetiredAttachment(context, keySlice, newRetireAtDt, newIdentifier, out currentDt);
            return currentDt;
        }

        // Handle case where retirement is being removed
        if (newRetireAtDt.HasValue == false)
        {
            RemoveRetirePutValue(context, keySlice, currentIdentifier, currentDt);
            return -1L;
        }

        // Both values are present - check what changed
        var retireAtChanged = newRetireAtDt.Value.Ticks != currentDt;
        var identifierChanged = HasIdentifierChanged(currentIdentifier, newIdentifier);

        if (retireAtChanged == false && identifierChanged == false)
        {
            // No changes needed
            return currentDt;
        }

        // Something changed - remove the old entry and add the new one
        RemoveRetirePutValue(context, keySlice, currentIdentifier, currentDt);
        TryPutRetiredAttachment(context, keySlice, newRetireAtDt, newIdentifier, out currentDt);

        return currentDt;
    }

    private static bool HasIdentifierChanged(string currentIdentifier, string newIdentifier)
    {
        return (string.IsNullOrEmpty(currentIdentifier) && string.IsNullOrEmpty(newIdentifier)) == false && currentIdentifier != newIdentifier;
    }

    private void TryPutRetiredAttachment(DocumentsOperationContext context, Slice keySlice, DateTime? retireAtDt, string identifier, out long retireAt)
    {
        if (retireAtDt.HasValue == false)
        {
            retireAt = -1L;
            return;
        }

        retireAt = retireAtDt.Value.Ticks;
        Put(context, keySlice, retireAtDt.Value.GetDefaultRavenFormat(), identifier);
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

            if (dbRecord.RetiredAttachments.Destinations.All(x => x.Value.Disabled))
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

            if (_logger.IsWarnEnabled)
                _logger.Warn(msg, e);

            return null;
        }
    }
}
