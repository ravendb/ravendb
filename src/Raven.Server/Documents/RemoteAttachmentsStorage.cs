using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Extensions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Attachments;
using Raven.Server.Documents.BackgroundWork;
using Raven.Server.Documents.Handlers.Processors.Attachments;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.DirectDownload;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
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

public class RemoteAttachmentsStorage : AbstractBackgroundWorkStorage<DocumentExpirationInfo>
{
    private readonly RavenLogger _logger;
    private const string AttachmentsByRemote = "AttachmentsByRemote";

    public RemoteAttachmentsConfiguration Configuration;

    public RemoteAttachmentsStorage(Transaction tx, DocumentDatabase database) : base(tx, database, AttachmentsByRemote, nameof(AttachmentName.RemoteParameters.At))
    {
        _logger = database.Loggers.GetLogger<RemoteAttachmentsStorage>();
    }

    private IEnumerator<BlittableJsonReaderObject> EnumerateAttachmentsFromMetadataAndCheckIfShouldSkip(BlittableJsonReaderArray attachments, DateTime currentTime, Func<string, bool> shouldSkip)
    {
        foreach (BlittableJsonReaderObject attachmentInMetadata in attachments)
        {
            if (attachmentInMetadata.TryGet(nameof(AttachmentName.RemoteParameters), out BlittableJsonReaderObject remoteParamsObject) == false)
                continue;
            if (remoteParamsObject.TryGet(nameof(RemoteAttachmentParameters.Flags), out object flagsObj) && Enum.TryParse(flagsObj.ToString(), out RemoteAttachmentFlags flags) && flags == RemoteAttachmentFlags.Remote)
            {
                // this attachment is already remote
                continue;
            }
            if (remoteParamsObject.TryGet(nameof(RemoteAttachmentParameters.Identifier), out LazyStringValue identifierFromMetadata) == false)
                continue;
            if (HasPassed(remoteParamsObject, currentTime, MetadataPropertyName) == false)
                continue;

            if (shouldSkip.Invoke(identifierFromMetadata))
                continue;

            yield return attachmentInMetadata;
        }
    }

    public void Put(DocumentsOperationContext context, Slice attachmentKey, string processDateString, string identifier)
    {
        using (AttachmentsStorage.AttachmentKey.ExtractLowerDocumentIdSliceFromAttachmentKey(context, attachmentKey, out var lowerDocumentId))
            base.Put(context, lowerDocumentId, processDateString);
    }

    protected override DocumentExpirationInfo GetBackgroundWorkInfo(BackgroundWorkParameters options, Slice docId, Slice ticksSlice)
    {
        using (var doc = Database.DocumentsStorage.Get(options.Context, docId, DocumentFields.Data | DocumentFields.Id))
        {
            if (doc == null
                || doc.TryGetMetadata(out var metadata) == false
                || metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
            {
                // doc was deleted
                return new DocumentExpirationInfo(ticksSlice, docId, null, BackgroundWorkInfoStatus.Delete);
            }

            foreach (var _ in EnumerateAttachmentsFromMetadataAndCheckIfShouldSkip(attachments, options.CurrentTime, ShouldSkipItem))
            {
                // this document has at least one attachment to upload
                return new DocumentExpirationInfo(ticksSlice, docId, doc.Id, BackgroundWorkInfoStatus.Process);
            }

            // nothing to upload
            return new DocumentExpirationInfo(ticksSlice, docId, null, BackgroundWorkInfoStatus.Delete);
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

    public unsafe void RemoveRemotePutValue(DocumentsOperationContext context, Slice attachmentKey, long ticks)
    {
        var ticksBigEndian = Bits.SwapBytes(ticks);

        var msg = $"Removing remote attachment put with key: '{attachmentKey}' from '{_treeName}' tree.";
        if (_logger.IsDebugEnabled)
            _logger.Debug(msg);

        var tree = context.Transaction.InnerTransaction.ReadTree(_treeName);

        using (AttachmentsStorage.AttachmentKey.ExtractLowerDocumentIdSliceFromAttachmentKey(context, attachmentKey, out var lowerDocumentId))
        using (Slice.External(context.Allocator, (byte*)&ticksBigEndian, sizeof(long), out Slice ticksSlice))
            tree.MultiDelete(ticksSlice, lowerDocumentId);
    }

    protected override void ProcessDocument(DocumentsOperationContext context, Slice lowerId, string identifier, DateTime currentTime)
    {
        throw new NotImplementedException();
    }

    protected override void HandleDocumentConflict(BackgroundWorkParameters options, Slice ticksAsSlice, Slice clonedId, Queue<DocumentExpirationInfo> expiredDocs, ref int totalCount)
    {
        (bool allShouldRemote, string id) = GetConflictedRemoteAttachment(options.Context, options.CurrentTime, clonedId);

        if (allShouldRemote)
        {
            expiredDocs.Enqueue(id == null ? new DocumentExpirationInfo(ticksAsSlice, clonedId, null, BackgroundWorkInfoStatus.Delete) : new DocumentExpirationInfo(ticksAsSlice, clonedId, id, BackgroundWorkInfoStatus.Process));
            totalCount++;
        }
    }

    private (bool AllHasPassed, string Id) GetConflictedRemoteAttachment(DocumentsOperationContext context, DateTime currentTime, Slice clonedId)
    {
        var conflicts = Database.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, clonedId);

        if (conflicts.Count <= 0)
            return (true, null);

        string id = null;
        var allHashPassed = true;

        foreach (var conflict in conflicts)
        {
            using (conflict)
            {
                id = conflict.Id;
                if (conflict.Doc.TryGetMetadata(out var metadata) == false)
                    continue;

                if (metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                    continue;

                foreach (var _ in EnumerateAttachmentsFromMetadataAndCheckIfShouldSkip(attachments, currentTime, s => false))
                {
                    allHashPassed = false;
                    break;
                }

                if (allHashPassed == false)
                    break;
            }
        }

        return (allHashPassed, id);
    }

    public DirectFileDownloader GetDownloader(Attachment attachment, OperationCancelToken token)
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
        return new DirectFileDownloader(settings, token);
    }

    public Task<Stream> StreamForDownloadDestinationInternal(DirectFileDownloader downloader, string objKeyName)
    {
        var folderName = string.Empty;

        return downloader.StreamForDownloadDestination(Database, folderName, objKeyName);
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
            RemoveRemotePutValue(context, keySlice, currentDt);
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
        RemoveRemotePutValue(context, keySlice, currentDt);
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool PutRemoteAttachmentFromPatch(DocumentsOperationContext context, BlittableJsonReaderObject document, string docId, string attachmentName, string identifier, DateTime at)
    {
        if (document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
            metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
            return false;

        var attachmentsStorage = Database.DocumentsStorage.AttachmentsStorage;
        foreach (BlittableJsonReaderObject attachment in attachments)
        {
            if (attachment.TryGet(nameof(AttachmentName.Name), out LazyStringValue name) == false ||
                attachment.TryGet(nameof(AttachmentName.ContentType), out LazyStringValue contentType) == false ||
                attachment.TryGet(nameof(AttachmentName.Hash), out LazyStringValue hash) == false ||
                attachment.TryGet(nameof(AttachmentName.Size), out long size) == false)
                throw new ArgumentException($"The attachment info is missing a mandatory value: {attachment}");

            if (name == attachmentName)
            {
                if (attachment.TryGet(nameof(AttachmentName.RemoteParameters), out BlittableJsonReaderObject readerObject) && readerObject != null)
                {
                    RemoteAttachmentParameters current = JsonDeserializationClient.RemoteAttachmentParameters(readerObject);
                    if (current.IsRemoteStorageAttachment())
                        throw new InvalidOperationException($"Cannot update remote attachment '{name}' in document '{docId}' because it is already marked as remote.");
                }

                attachmentsStorage.PutAttachment(context, docId, name, contentType, hash, size, new RemoteAttachmentParameters(identifier, at), stream: null,
                    updateDocument: false); // we will update the document in PatchDocumentCommand
                return true;
            }
        }

        return false;
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
                AlertReason.RemoteAttachmentsConfigurationNotValid, NotificationSeverity.Error, Database.Name));

            if (_logger.IsWarnEnabled)
                _logger.Warn(msg, e);

            return null;
        }
    }

    public int ProcessRemoteAttachments(DocumentsOperationContext context, DateTime currentTime, Dictionary<string, List<Slice>> seenDocs, Dictionary<string, Dictionary<string, AttachmentRemoteInfo>> attachmentsToUploadByIdentifier)
    {
        var processedCount = 0;
        var docsTree = context.Transaction.InnerTransaction.ReadTree(_treeName);
        foreach (var (docId, allTicks) in seenDocs)
        {
            using (DocumentIdWorker.GetLoweredIdSliceFromId(context, docId, out var lowerId))
            using (var doc = Database.DocumentsStorage.Get(context, lowerId, DocumentFields.Data | DocumentFields.Id, throwOnConflict: true))
            {
                // remove all entries for processed document
                foreach (var ticks in allTicks)
                {
                    docsTree.MultiDelete(ticks, lowerId);
                }

                if (doc == null || doc.TryGetMetadata(out var metadata) == false || metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false || attachments == null)
                {
                    // the document was deleted or has no attachments anymore
                    continue;
                }

                // lets go over attachments and mark those we proceed as remote
                foreach (BlittableJsonReaderObject attachmentInMetadata in attachments)
                {
                    if (attachmentInMetadata.TryGet(nameof(AttachmentName.RemoteParameters), out BlittableJsonReaderObject remoteParamsObject) == false)
                        continue;

                    if (remoteParamsObject.TryGet(nameof(RemoteAttachmentParameters.Flags), out string flagsObj) && Enum.TryParse(flagsObj, out RemoteAttachmentFlags flags) && flags == RemoteAttachmentFlags.Remote)
                    {
                        // this attachment is already remote
                        continue;
                    }

                    if (HasPassed(remoteParamsObject, currentTime, MetadataPropertyName) == false)
                        continue;

                    if (remoteParamsObject.TryGet(nameof(RemoteAttachmentParameters.Identifier), out LazyStringValue identifierFromMetadata) == false)
                        continue;

                    if (attachmentInMetadata.TryGet(nameof(AttachmentName.Name), out LazyStringValue name) == false ||
                        attachmentInMetadata.TryGet(nameof(AttachmentName.ContentType), out LazyStringValue contentType) == false ||
                        attachmentInMetadata.TryGet(nameof(AttachmentName.Hash), out LazyStringValue hash) == false ||
                        attachmentInMetadata.TryGet(nameof(AttachmentName.Size), out long sizeInBytes) == false)
                        continue;

                    if (attachmentsToUploadByIdentifier.TryGetValue(identifierFromMetadata, out Dictionary<string, AttachmentRemoteInfo> dic) && dic.TryGetValue(hash, out AttachmentRemoteInfo info))
                    {
                        switch (info.Status)
                        {
                            case BackgroundWorkInfoStatus.Retry:
                                // this upload errored lets add back to the tree
                                remoteParamsObject.TryGet(nameof(RemoteAttachmentParameters.At), out LazyStringValue dateFromMetadata);
                                base.Put(context, lowerId, dateFromMetadata);

                                break;
                            case BackgroundWorkInfoStatus.Skip:
                                // this upload errored max retries, we are skipping it
                                // we already alerted on this item, when we iterated on it in RemoteAttachmentsSender._batchExceptionsByIdentifier so nothing to do here
                                break;
                            case BackgroundWorkInfoStatus.Process:
                                // we uploaded this, we can mark the attachment as remote and delete its stream
                                Database.DocumentsStorage.AttachmentsStorage.MarkAsRemoteAttachment(context, lowerId, docId, name, contentType, hash, sizeInBytes);
                                processedCount++;
                                break;
                            default:
                                throw new ArgumentOutOfRangeException(nameof(info.Status));
                        } 
                    }
                }

                if (processedCount > 0)
                {
                    Database.DocumentsStorage.AttachmentsStorage.UpdateDocumentAfterAttachmentChange(context, docId);
                }
            }
        }

        return processedCount;
    }
}
