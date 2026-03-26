using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Extensions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Attachments;
using Raven.Server.Documents.BackgroundWork;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
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

    public DirectFileDownloader GetDownloader(Attachment attachment, OperationCancelToken token) => GetDownloaderImpl(attachment.RemoteParameters.Identifier, attachment.Key, token);

    private DirectFileDownloader GetDownloaderImpl(string remoteId, object key, OperationCancelToken token)
    {
        if (Configuration == null)
            throw new InvalidOperationException($"Cannot get remote attachment '{key}' because {nameof(RemoteAttachmentsConfiguration)} is not configured on {Database.Name}.");

        if (Configuration.Disabled)
            throw new InvalidOperationException($"Cannot get remote attachment '{key}' because {nameof(RemoteAttachmentsConfiguration)} is disabled.");

        if (Configuration.Destinations == null || Configuration.Destinations.TryGetValue(remoteId, out var destination) == false)
            throw new InvalidOperationException($"Cannot get remote attachment '{key}' because destination for '{remoteId}' doesn't exist.");

        if (destination.Disabled)
            throw new InvalidOperationException($"Cannot get remote attachment '{key}' because destination for '{remoteId}' is disabled.");

        var settings = UploaderSettings.GenerateDirectUploaderSettingsForAttachments(Database, nameof(AttachmentHandlerProcessorForGetAttachment), destination.S3Settings, destination.AzureSettings);
        return new DirectFileDownloader(settings, token);
    }

    public Task<Stream> StreamForDownloadDestinationInternal(DirectFileDownloader downloader, string objKeyName)
    {
        var folderName = string.Empty;
        return downloader.StreamForDownloadDestination(Database, folderName, objKeyName);
    }

    public long TryUpdateRemoteAttachment(DocumentsOperationContext context, string documentId, string name, DateTime? newRemoteAtDt, long currentDt, string newIdentifier, string currentIdentifier, Slice keySlice)
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

        if (_logger.IsDebugEnabled)
        {
            var curDt = new DateTime(currentDt);
            if (remoteAtChanged == false && identifierChanged == false)
            {
                // The attachment was re added, lets update the background work tree even if its the same identifier and at
                _logger.Debug("Updated remote parameters for attachment '{0}' of document '{1}' with unchanged remote parameters (identifier '{2}', at: '{3}'). Updating background work tree entry.", name, documentId, currentIdentifier, curDt.GetDefaultRavenFormat());
            }
            else
            {
                _logger.Debug("Updated remote parameters for attachment '{0}' of document '{1}' with new remote parameters (identifier '{2}' -> '{3}', at: '{4}' -> '{5}'). Updating background work tree entry.", name, documentId, currentIdentifier, newIdentifier, curDt.GetDefaultRavenFormat(), newRemoteAtDt.Value.GetDefaultRavenFormat());
            }
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

    public async ValueTask<string> GetAttachmentDataAsBase64Async(string remoteStorageId, string hash, string type, CancellationToken token)
    {
        using OperationCancelToken operationToken = new(token);
        using var downloader = GetDownloaderImpl(remoteStorageId, null, operationToken);
        await using var stream = await StreamForDownloadDestinationInternal(downloader, hash);

        // Determine the type based on content type or default to application/octet-stream
        return GenAiScriptTransformer.GetAttachmentDataAsBase64(stream, type ?? "application/octet-stream");
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

    public int ProcessRemoteAttachments(DocumentsOperationContext context, DateTime currentTime, Dictionary<Slice, List<Slice>> seenDocs, Dictionary<string, Dictionary<string, AttachmentRemoteInfo>> attachmentsToUploadByIdentifier)
    {
        var processedCount = 0;
        var docsTree = context.Transaction.InnerTransaction.ReadTree(_treeName);
        foreach (var (lowerId, allTicks) in seenDocs)
        {
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
                                // this upload errored lets add back to the tree with recalculated ticks
                                var newTicks = CalculateRetryTicks(currentTime, info.RetryCount);

                                if (_logger.IsDebugEnabled)
                                {
                                    remoteParamsObject.TryGet(nameof(RemoteAttachmentParameters.At), out LazyStringValue dateFromMetadata);
                                    var dt = ProcessDateUniversalTime(Database, lowerId, dateFromMetadata);
                                    var nextRetryTime = new DateTime(newTicks);
                                    var deltaTicks = currentTime.Ticks - dt.Ticks;
                                    var deltaTimeSpan = new TimeSpan(deltaTicks);
                                    _logger.Debug($"Scheduling retry #{info.RetryCount + 1} for attachment '{name}' (hash: '{hash}', identifier: '{identifierFromMetadata}') in document '{doc.Id}'. " +
                                                  $"Time elapsed since original: {deltaTimeSpan}, Next attempt: {nextRetryTime.GetDefaultRavenFormat()}, original time: {dt.GetDefaultRavenFormat()}.");
                                }

                                PutTicksDirectly(context, lowerId, newTicks);
                                break;
                            case BackgroundWorkInfoStatus.Process:
                                // we uploaded this, we can mark the attachment as remote and delete its stream
                                Database.DocumentsStorage.AttachmentsStorage.MarkAsRemoteAttachment(context, lowerId, doc.Id, name, contentType, hash, sizeInBytes);
                                processedCount++;
                                break;
                            default:
                                throw new ArgumentOutOfRangeException(nameof(info.Status));
                        } 
                    }
                }

                if (processedCount > 0)
                {
                    Database.DocumentsStorage.AttachmentsStorage.UpdateDocumentAfterAttachmentChangeInternal(context, lowerId, doc.Id);
                }
            }
        }

        return processedCount;
    }

    private static readonly long TicksPer15Minutes = 15 * TimeSpan.TicksPerMinute;
    private static readonly long TicksPer1Hour = TimeSpan.TicksPerHour;
    private static readonly long TicksPer3Hours = 3 * TimeSpan.TicksPerHour;
    private static readonly long TicksPer12Hours = 12 * TimeSpan.TicksPerHour;
    private static readonly long TicksPer24Hours = 24 * TimeSpan.TicksPerHour;

    private static long CalculateRetryTicks(DateTime currentTime, long retryCount)
    {
        long delay;

        // Simple tiered delays based on retry count
        if (retryCount < 4)
        {
            // First 3 retries: every 15 minutes (likely transient issues)
            delay = TicksPer15Minutes;
        }
        else if (retryCount < 12)
        {
            // Retries 4-11: every 1 hour (persistent issue, slow down)
            delay = TicksPer1Hour;
        }
        else if (retryCount < 24)
        {
            // Retries 12-23: every 3 hours (long-term issue)
            delay = TicksPer3Hours;
        }
        else if (retryCount < 48)
        {
            // Retries 24-47: every 12 hours (very persistent)
            delay = TicksPer12Hours;
        }
        else
        {
            // Retry 48+: once per day (maximum backoff)
            delay = TicksPer24Hours;
        }

        return currentTime.Ticks + delay;
    }
}
