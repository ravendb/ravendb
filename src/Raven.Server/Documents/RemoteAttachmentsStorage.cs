using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents.BackgroundWork;
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

public class RemoteAttachmentsStorage : AbstractBackgroundWorkStorage<AttachmentRemoteInfo>
{
    private DocumentInfoHelper _documentInfoHelper;
    private readonly RavenLogger _logger;
    private const string AttachmentsByRemote = "AttachmentsByRemote";

    public RemoteAttachmentsConfiguration Configuration;

    public RemoteAttachmentsStorage(Transaction tx, DocumentDatabase database) : base(tx, database, AttachmentsByRemote, nameof(AttachmentName.RemoteParameters.At))
    {
        _logger = database.Loggers.GetLogger<RemoteAttachmentsStorage>();
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
    private static ByteStringContext<ByteStringMemoryCache>.ExternalScope ExtractIdentifierStringAndDocumentIdSliceFromRemoteAttachmentKey(DocumentsOperationContext context, Slice key, out string identifier, out Slice documentIdSlice)
    {
        identifier = ExtractIdentifierStringFromRemoteAttachmentKey(context, key, out var sizeOfIdentifier);
        var s = sizeOfIdentifier + 1;

        var sizeOfId = AttachmentsStorage.AttachmentKey.FindNextSeparator(key, s) - s;
        var d2 = Slice.External(context.Allocator, key.Content, s, sizeOfId, out documentIdSlice);
        return d2;
    }

    private static LazyStringValue ExtractIdentifierStringFromRemoteAttachmentKey(DocumentsOperationContext context, Slice key, out int sizeOfIdentifier)
    {
        sizeOfIdentifier = AttachmentsStorage.AttachmentKey.FindNextSeparator(key, 0);
        using var d1 = Slice.External(context.Allocator, key.Content, 0, sizeOfIdentifier, out var identifierSlice);
        return GetStringFromIdSlice(context, identifierSlice);
    }

    private static ByteStringContext<ByteStringMemoryCache>.ExternalScope ExtractIdentifierStringAndAttachmentKeySlice(DocumentsOperationContext context, Slice key, out LazyStringValue identifier, out Slice attachmentKeySlice)
    {
        identifier = ExtractIdentifierStringFromRemoteAttachmentKey(context, key, out _);
        var d2 = ExtractAttachmentKeySliceFromRemoteAttachmentKey(context, key, out attachmentKeySlice);

        return d2;
    }

    private static ByteStringContext<ByteStringMemoryCache>.ExternalScope ExtractAttachmentKeySliceFromRemoteAttachmentKey(DocumentsOperationContext context, Slice key, out Slice attachmentKeySlice)
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

    protected override unsafe void ProcessDocument(DocumentsOperationContext context, Slice attachmentKey, string identifier, DateTime currentTime)
    {
        var sizeOfIdentifier = AttachmentsStorage.AttachmentKey.GetSizeOfDocId(new ReadOnlySpan<byte>(attachmentKey.Content.Ptr, attachmentKey.Size));
        using (var lowerDocId = _documentInfoHelper.GetDocumentId(attachmentKey, sizeOfIdentifier + 1))
        {
            if (lowerDocId == null)
            {
                throw new InvalidOperationException($"Couldn't process the remote attachment. Remote attachment key is '{attachmentKey}'.");
            }

            using (var doc = Database.DocumentsStorage.Get(context, lowerDocId, DocumentFields.Data | DocumentFields.Id, throwOnConflict: true))
            {
                if (doc == null || doc.TryGetMetadata(out var metadata) == false)
                    return;

                if (metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                    return;

                using var attachmentKeyDisposable = ExtractAttachmentKeySliceFromRemoteAttachmentKey(context, attachmentKey, out Slice attachmentKeySlice);
                var nameByKey = Database.DocumentsStorage.AttachmentsStorage.GetAttachmentNameByKey(context, attachmentKeySlice);
                if (nameByKey == null)
                    return;

                for (var i = 0; i < attachments.Length; i++)
                {
                    var attachmentInMetadata = (BlittableJsonReaderObject)attachments[i];
                    if (attachmentInMetadata.TryGet(nameof(AttachmentName.Name), out string name) == false)
                        continue;
                    if (attachmentInMetadata.TryGet(nameof(AttachmentName.RemoteParameters), out BlittableJsonReaderObject remoteParamsObject) == false)
                        continue;
                    if (remoteParamsObject.TryGet(nameof(RemoteAttachmentParameters.Identifier), out LazyStringValue identifierFromMetadata) == false)
                        continue;
                    if (identifierFromMetadata != identifier)
                        continue;

                    if (name == nameByKey)
                    {
                        if (HasPassed(remoteParamsObject, currentTime, MetadataPropertyName) == false)
                            return;

                        Database.DocumentsStorage.AttachmentsStorage.MarkAsRemoteAttachment(context, new AttachmentDetailsServer()
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

    public void Put(DocumentsOperationContext context, Slice attachmentKey, string processDateString, string identifier)
    {
        using (CreateRemoteAttachmentsKeyWithIdentifier(context, attachmentKey, identifier, out Slice key))
            base.Put(context, key, processDateString);
    }

    private unsafe ByteStringContext.InternalScope CreateRemoteAttachmentsKeyWithIdentifier(DocumentsOperationContext context, Slice attachmentKey, string identifier, out Slice outSlice)
    {
        // something like: my-s3-cool-storage|categories/1-a|d|image.jpg|S5Opbm22FH1LW5SAC3wRb3HA64QM7odd26djlt5cAkM=|image/jpeg
        using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, identifier, out _, out Slice identifierSlice))
        {
            var size = identifierSlice.Content.Length + 1 + attachmentKey.Content.Length; // identifier + record separator + attachmentKey 
            var scope = context.Allocator.Allocate(size, out ByteString keyMem);
            var pos = 0;
            Memory.Copy(keyMem.Ptr + pos, identifierSlice.Content.Ptr, identifierSlice.Content.Length);
            pos = identifierSlice.Content.Length;
            keyMem.Ptr[pos++] = SpecialChars.RecordSeparator;
            Memory.Copy(keyMem.Ptr + pos, attachmentKey.Content.Ptr, attachmentKey.Content.Length);

            outSlice = new Slice(SliceOptions.Key, keyMem);
            return scope;
        }
    }

    protected override AttachmentRemoteInfo GetBackgroundWorkInfo(BackgroundWorkParameters options, Slice attachmentKey, Slice ticksSlice)
    {
        using (ExtractIdentifierStringAndDocumentIdSliceFromRemoteAttachmentKey(options.Context, attachmentKey, out string identifier, out Slice documentIdSlice))
        {
            if (Database.DocumentsStorage.GetTableValueReaderForDocument(options.Context, documentIdSlice, throwOnConflict: false, out _) == false)
            {
                // doc was deleted
                return new AttachmentRemoteInfo(ticksSlice, attachmentKey, null, BackgroundWorkInfoStatus.Delete);
            }

            if (ShouldSkipItem(identifier))
            {
                return new AttachmentRemoteInfo(ticksSlice, attachmentKey, identifier, BackgroundWorkInfoStatus.Skip);
            }

            return new AttachmentRemoteInfo(ticksSlice, attachmentKey, identifier, BackgroundWorkInfoStatus.Process);
        }
    }

    [DoesNotReturn]
    protected override void ThrowWrongDateFormat(Slice treeKey, string expirationDate)
    {
        throw new InvalidOperationException(
            $"The due date format for attachment with key '{treeKey}' is not valid: '{expirationDate}'. Use the following format: {Database.Time.GetUtcNow():O}");
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

    public unsafe void RemoveRemotePutValue(DocumentsOperationContext context, Slice attachmentKey, string identifier, long ticks)
    {
        var ticksBigEndian = Bits.SwapBytes(ticks);

        var msg = $"Removing remote attachment put with key: '{attachmentKey}' from '{_treeName}' tree.";
        if (_logger.IsDebugEnabled)
            _logger.Debug(msg);

        var tree = context.Transaction.InnerTransaction.ReadTree(_treeName);

        using (CreateRemoteAttachmentsKeyWithIdentifier(context, attachmentKey, identifier, out Slice key))
        using (Slice.External(context.Allocator, (byte*)&ticksBigEndian, sizeof(long), out Slice ticksSlice))
            tree.MultiDelete(ticksSlice, key);
    }

    protected override void HandleDocumentConflict(BackgroundWorkParameters options, Slice ticksAsSlice, Slice clonedId, Queue<AttachmentRemoteInfo> expiredDocs, ref int totalCount)
    {
        (bool allShouldRemote, string id) = GetConflictedRemoteAttachment(options.Context, options.CurrentTime, clonedId);

        if (allShouldRemote)
        {
            expiredDocs.Enqueue(new AttachmentRemoteInfo(ticksAsSlice, clonedId, id, BackgroundWorkInfoStatus.Process));
            totalCount++;
        }
    }

    private const string AlertTitle = "Remote Attachments";
    private const string WarnMessage = "A remote attachment was skipped.";
    private long _counter = 0;

    protected override void HandleSkippedItem(AttachmentRemoteInfo item)
    {
        if (_logger.IsDebugEnabled)
        {
            _logger.Debug($"Skipping remote attachment '{item.Key}' with identifier '{item.DestinationIdentifier}'");
        }

        if (_counter++ % 1024 == 0)
        {
            var alert = AlertRaised.Create(Database.Name, AlertTitle, WarnMessage, AlertType.Attachments_RemoteAttachmentWithoutIdentifier, NotificationSeverity.Warning, key: nameof(AlertType.Attachments_RemoteAttachmentWithoutIdentifier));
            Database.NotificationCenter.Add(alert);
        }
    }

    private unsafe (bool AllHasPassed, string Id) GetConflictedRemoteAttachment(DocumentsOperationContext context, DateTime currentTime, Slice clonedId)
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
                        if (attachmentInMetadata.TryGet(nameof(AttachmentName.RemoteParameters), out BlittableJsonReaderObject remoteParamsObject) == false)
                            continue;
                        if (remoteParamsObject.TryGet(nameof(RemoteAttachmentParameters.Identifier), out LazyStringValue identifierFromMetadata) == false)
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
            throw new InvalidOperationException($"Cannot get remote attachment '{attachment.Key}' because {nameof(RemoteAttachmentsConfiguration)} is not configured on {Database.Name}.");

        if (Configuration.Disabled)
            throw new InvalidOperationException($"Cannot get remote attachment '{attachment.Key}' because {nameof(RemoteAttachmentsConfiguration)} is disabled.");

        if (Configuration.Destinations == null || Configuration.Destinations.TryGetValue(attachment.RemoteParameters.Identifier, out var destination) == false)
            throw new InvalidOperationException($"Cannot get remote attachment '{attachment.Key}' because destination for '{attachment.RemoteParameters.Identifier}' doesn't exist.");
        if (destination.Disabled)
            throw new InvalidOperationException($"Cannot get remote attachment '{attachment.Key}' because destination for '{attachment.RemoteParameters.Identifier}' is disabled.");

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

    public long TryUpdateRemoteAttachment(DocumentsOperationContext context, DateTime? newRemoteAtDt, long currentDt, string newIdentifier, string currentIdentifier, Slice keySlice)
    {
        // Handle case where there's no current upload date
        if (currentDt == -1L)
        {
            TryPutRemoteAttachment(context, keySlice, newRemoteAtDt, newIdentifier, out currentDt);
            return currentDt;
        }

        // Handle case where remote attachment is being removed
        if (newRemoteAtDt.HasValue == false)
        {
            RemoveRemotePutValue(context, keySlice, currentIdentifier, currentDt);
            return -1L;
        }

        // Both values are present - check what changed
        var remoteAtChanged = newRemoteAtDt.Value.Ticks != currentDt;
        var identifierChanged = HasIdentifierChanged(currentIdentifier, newIdentifier);

        if (remoteAtChanged == false && identifierChanged == false)
        {
            // No changes needed
            return currentDt;
        }

        // Something changed - remove the old entry and add the new one
        RemoveRemotePutValue(context, keySlice, currentIdentifier, currentDt);
        TryPutRemoteAttachment(context, keySlice, newRemoteAtDt, newIdentifier, out currentDt);

        return currentDt;
    }

    private static bool HasIdentifierChanged(string currentIdentifier, string newIdentifier)
    {
        return (string.IsNullOrEmpty(currentIdentifier) && string.IsNullOrEmpty(newIdentifier)) == false && currentIdentifier != newIdentifier;
    }

    private void TryPutRemoteAttachment(DocumentsOperationContext context, Slice keySlice, DateTime? remoteAtDt, string identifier, out long remoteAt)
    {
        if (remoteAtDt.HasValue == false)
        {
            remoteAt = -1L;
            return;
        }

        remoteAt = remoteAtDt.Value.Ticks;
        Put(context, keySlice, remoteAtDt.Value.GetDefaultRavenFormat(), identifier);
    }

    public RemoteAttachmentsSender UpdateRemoteAttachmentsFromDatabaseRecord(DatabaseRecord dbRecord, RemoteAttachmentsSender remoteAttachmentsSender)
    {
        try
        {
            if (dbRecord.RemoteAttachments == null)
            {
                Configuration = null;
                remoteAttachmentsSender?.Dispose();
                return null;
            }

            if (remoteAttachmentsSender != null)
            {
                // no changes
                if (Equals(remoteAttachmentsSender.Configuration, dbRecord.RemoteAttachments))
                    return remoteAttachmentsSender;
            }

            remoteAttachmentsSender?.Dispose();
            Configuration = dbRecord.RemoteAttachments;

            if (dbRecord.RemoteAttachments.HasDestination() == false)
                return null;

            var cleaner = new RemoteAttachmentsSender(Database, dbRecord.RemoteAttachments);
            cleaner.Start();
            return cleaner;
        }
        catch (Exception e)
        {
            const string msg = $"Cannot enable {nameof(remoteAttachmentsSender)} as the configuration record is not valid.";
            Database.NotificationCenter.Add(AlertRaised.Create(
            Database.Name,
                $"Remote attachment upload error in '{Database.Name}'", msg,
                AlertType.RemoteAttachmentsConfigurationNotValid, NotificationSeverity.Error, Database.Name));

            if (_logger.IsWarnEnabled)
                _logger.Warn(msg, e);

            return null;
        }
    }
}
