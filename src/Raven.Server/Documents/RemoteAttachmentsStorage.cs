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

    protected override void ProcessDocument(DocumentsOperationContext context, Slice treeKey, string docId, DateTime currentTime)
    {
        using (var doc = Database.DocumentsStorage.Get(context, treeKey, DocumentFields.Data | DocumentFields.Id, throwOnConflict: true))
        {
            if (doc == null || doc.TryGetMetadata(out var metadata) == false)
                return;

            if (metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                return;

            foreach (var attachmentInMetadata in EnumerateAttachmentsFromMetadataAndCheckIfShouldSkip(attachments, currentTime, ShouldSkipItem))
            {
                Database.DocumentsStorage.AttachmentsStorage.MarkAsRemoteAttachment(context, attachmentInMetadata, treeKey, docId);
            }

            Database.DocumentsStorage.AttachmentsStorage.UpdateDocumentAfterAttachmentChange(context, docId);
        }
    }

    public void Put(DocumentsOperationContext context, Slice attachmentKey, string processDateString, string identifier)
    {
        using (AttachmentsStorage.AttachmentKey.ExtractLowerDocumentIdSliceFromAttachmentKey(context, attachmentKey, out var lowerDocumentId))
            base.Put(context, lowerDocumentId, processDateString);
    }

    protected override AttachmentRemoteInfo GetBackgroundWorkInfo(BackgroundWorkParameters options, Slice docId, Slice ticksSlice)
    {
        using (var doc = Database.DocumentsStorage.Get(options.Context, docId, DocumentFields.Data | DocumentFields.Id))
        {
            if (doc == null
                || doc.TryGetMetadata(out var metadata) == false
                || metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
            {
                // doc was deleted
                return new AttachmentRemoteInfo(ticksSlice, docId, null, BackgroundWorkInfoStatus.Delete);
            }

            foreach (var _ in EnumerateAttachmentsFromMetadataAndCheckIfShouldSkip(attachments, options.CurrentTime, ShouldSkipItem))
            {
                // this document has at least one attachment to upload
                return new AttachmentRemoteInfo(ticksSlice, docId, doc.Id, BackgroundWorkInfoStatus.Process);
            }

            // nothing to upload
            return new AttachmentRemoteInfo(ticksSlice, docId, null, BackgroundWorkInfoStatus.Delete);
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

        using (AttachmentsStorage.AttachmentKey.ExtractLowerDocumentIdSliceFromAttachmentKey(context, attachmentKey, out var lowerDocumentId))
        using (Slice.External(context.Allocator, (byte*)&ticksBigEndian, sizeof(long), out Slice ticksSlice))
            tree.MultiDelete(ticksSlice, lowerDocumentId);
    }

    protected override void HandleDocumentConflict(BackgroundWorkParameters options, Slice ticksAsSlice, Slice clonedId, Queue<AttachmentRemoteInfo> expiredDocs, ref int totalCount)
    {
        (bool allShouldRemote, string id) = GetConflictedRemoteAttachment(options.Context, options.CurrentTime, clonedId);

        if (allShouldRemote)
        {
            expiredDocs.Enqueue(id == null ? new AttachmentRemoteInfo(ticksAsSlice, clonedId, null, BackgroundWorkInfoStatus.Delete) : new AttachmentRemoteInfo(ticksAsSlice, clonedId, id, BackgroundWorkInfoStatus.Process));
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
            var alert = AlertRaised.Create(Database.Name, AlertTitle, WarnMessage, AlertReason.Attachments_RemoteAttachmentWithoutIdentifier, NotificationSeverity.Warning, key: nameof(AlertReason.Attachments_RemoteAttachmentWithoutIdentifier));
            Database.NotificationCenter.Add(alert);
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
                AlertReason.RemoteAttachmentsConfigurationNotValid, NotificationSeverity.Error, Database.Name));

            if (_logger.IsWarnEnabled)
                _logger.Warn(msg, e);

            return null;
        }
    }
}
