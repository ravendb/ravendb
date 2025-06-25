using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
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
        //var type = GetRetireType(lowerId);
        //using var scope = CleanRetiredAttachmentsKey(context, lowerId, out var keySlice);
        //switch (type)
        //{
        //    case AttachmentRetireType.PutRetire:
                var collection = id; // for retire attachments, the id is the collection name
                if (string.IsNullOrEmpty(collection))
                    throw new InvalidOperationException($"Couldn't retire the attachment. Document collection is null. Lower id is '{lowerId}'.");
                ProcessDocumentForPutRetire(context, lowerId, collection, currentTime);
        //        break;

        //    case AttachmentRetireType.DeleteRetire:
        //        ProcessDocumentForDeleteRetire(context, keySlice, id, currentTime);
        //        break;

        //    default:
        //        throw new ArgumentOutOfRangeException(nameof(type), type, null);
        //}
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
        //var type = GetRetireType(clonedId);

        //switch (type)
        //{
        //    case AttachmentRetireType.PutRetire:
                return DocumentAndIdOrCollectionForPutRetire(options, clonedId, ticksSlice);
        //    case AttachmentRetireType.DeleteRetire:
        //        return DocumentAndIdOrCollectionForDeleteRetire(options, clonedId, ticksSlice);
        //    case AttachmentRetireType.ExistingRetire:
        //        return DocumentAndIdOrCollectionForExistingRetire(options, clonedId, ticksSlice);
        //    default:
        //        throw new ArgumentOutOfRangeException(nameof(type), type, null);
        //}
    }

    private DocumentExpirationInfo DocumentAndIdOrCollectionInternal(BackgroundWorkParameters options, Slice clonedId, Slice ticksSlice, out Document document, out string id, out string collectionStr)
    {
        document = null;
        collectionStr = null;
        //string id;
        //using var scope = CleanRetiredAttachmentsKey(options.Context, clonedId, out var keySlice);
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

                //TODO: egor this needs new handing
                //if (options.DatabaseRecord.RetiredAttachments.RetirePeriods.ContainsKey(collectionStr) == false)
                //{
                //    // we don't care about this collection, it was removed from the configuration
                //    return new DocumentExpirationInfo(ticksSlice, clonedId, id: null);
                //}
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

    private DocumentExpirationInfo DocumentAndIdOrCollectionForExistingRetire(BackgroundWorkParameters options, Slice clonedId, Slice ticksSlice)
    {
        var info = DocumentAndIdOrCollectionInternal(options, clonedId, ticksSlice, out var document, out var id, out var collectionStr);
        if (info != null)
            return info;

        if (document == null)
            return new DocumentExpirationInfo(ticksSlice, clonedId, id: null);

        return new DocumentExpirationInfo(ticksSlice, clonedId, id: id);
    }

    private DocumentExpirationInfo DocumentAndIdOrCollectionForDeleteRetire(BackgroundWorkParameters options, Slice clonedId, Slice ticksSlice)
    {
        var info = DocumentAndIdOrCollectionInternal(options, clonedId, ticksSlice, out var document, out var id, out var collectionStr);
        if (info != null)
            return info;

        if (document == null)
        {
            collectionStr = GetCollectionStringFromRetiredAttachmentsKey(options.Context, clonedId);

            if (options.DatabaseRecord.RetiredAttachments == null)
            {
                // no configuration, we don't care about this collection
                return new DocumentExpirationInfo(ticksSlice, clonedId, id: null);
            }
            //TODO: egor handle new logic
            //if (options.DatabaseRecord.RetiredAttachments.RetirePeriods.ContainsKey(collectionStr) == false)
            //{
            //    // we don't care about this collection, it was removed from the configuration
            //    return new DocumentExpirationInfo(ticksSlice, clonedId, id: null);
            //}
            //if (options.DatabaseRecord.RetiredAttachments.PurgeOnDelete == false)
            //{
            //    // purge on delete is false, we don't care about this collection
            //    return new DocumentExpirationInfo(ticksSlice, clonedId, id: null);
            //}

            return new DocumentExpirationInfo(ticksSlice, clonedId, id: collectionStr);
        }

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

    //public override void Put(DocumentsOperationContext context, Slice lowerId, string processDateString)
    //{
    //    using (CreateRetiredAttachmentsKeyWithType(context, lowerId, AttachmentRetireType.PutRetire, out Slice key))
    //        base.Put(context, key, processDateString);
    //}

    public unsafe void PutDelete(DocumentsOperationContext context, Slice lowerId, long ticks, string collection)
    {
        var ticksBigEndian = Bits.SwapBytes(ticks);

        var msg = $"Adding retired attachment delete with key: '{lowerId}', collection: {collection}, ticks: {ticks} to '{_treeName}' tree.";
        if (_logger.IsInfoEnabled)
            _logger.Info(msg);

        Debug.Assert(string.IsNullOrEmpty(collection) == false, "string.IsNullOrEmpty(collection) == false");
        var tree = context.Transaction.InnerTransaction.ReadTree(_treeName);
        using (CreateRetiredAttachmentsKeyWithTypeAndCollection(context, lowerId, AttachmentRetireType.DeleteRetire, collection, out Slice key))
        using (Slice.External(context.Allocator, (byte*)&ticksBigEndian, sizeof(long), out Slice ticksSlice))
            tree.MultiAdd(ticksSlice, key);
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

    private unsafe ByteStringContext.InternalScope CreateRetiredAttachmentsKeyWithTypeAndCollection(DocumentsOperationContext context, Slice lowerId, AttachmentRetireType retireType, string collection, out Slice outSlice)
    {
        var size = 1 + 1 + Encoding.UTF8.GetMaxByteCount(collection.Length) + 1 + lowerId.Content.Length; // retireType + record separator + collection size + record separator + lowerId 
        var scope = context.Allocator.Allocate(size, out ByteString keyMem);
        var pos = 0;
        switch (retireType)
        {
            case AttachmentRetireType.PutRetire:
                keyMem.Ptr[pos++] = (byte)'p';
                keyMem.Ptr[pos++] = SpecialChars.RecordSeparator;
                break;
            case AttachmentRetireType.DeleteRetire:
                keyMem.Ptr[pos++] = (byte)'d';
                keyMem.Ptr[pos++] = SpecialChars.RecordSeparator;
                break;
            case AttachmentRetireType.ExistingRetire:
                keyMem.Ptr[pos++] = (byte)'e';
                keyMem.Ptr[pos++] = SpecialChars.RecordSeparator;
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(retireType), retireType, null);
        }

        fixed (char* pCollection = collection)
        {
            var buff = (byte*)(keyMem.Ptr + pos);

            var dbLen = Encoding.UTF8.GetBytes(pCollection, collection.Length, buff, size - pos);

            pos += dbLen;
        }

        keyMem.Ptr[pos++] = SpecialChars.RecordSeparator;
        keyMem.Truncate(pos + lowerId.Content.Length);

        Memory.Copy(keyMem.Ptr + pos, lowerId.Content.Ptr, lowerId.Content.Length);
        outSlice = new Slice(SliceOptions.Key, keyMem);
        return scope;
    }

    private unsafe ByteStringContext.InternalScope CreateRetiredAttachmentsKeyWithType(DocumentsOperationContext context, Slice lowerId, AttachmentRetireType retireType, out Slice outSlice)
    {
        var size = 1 + 1 + lowerId.Content.Length; // retireType + record separator + lowerId 
        var scope = context.Allocator.Allocate(size, out ByteString keyMem);
        var pos = 0;
        switch (retireType)
        {
            case AttachmentRetireType.PutRetire:
                keyMem.Ptr[pos++] = (byte)'p';
                break;

            case AttachmentRetireType.DeleteRetire:
                keyMem.Ptr[pos++] = (byte)'d';
                break;
            case AttachmentRetireType.ExistingRetire:
                keyMem.Ptr[pos++] = (byte)'e';
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(retireType), retireType, null);
        }

        keyMem.Ptr[pos++] = SpecialChars.RecordSeparator;
        Memory.Copy(keyMem.Ptr + pos, lowerId.Content.Ptr, lowerId.Content.Length);

        outSlice = new Slice(SliceOptions.Key, keyMem);
        return scope;
    }

    public ByteStringContext<ByteStringMemoryCache>.ExternalScope RemoveTypeAndCollectionFromRetiredAttachmentsKey(DocumentsOperationContext context, Slice lowerId, out Slice outSlice)
    {
        var pos = 2;
        var keyPos = lowerId.Content.IndexOf(SpecialChars.RecordSeparator, pos) + 1;
        var size = lowerId.Content.Length - keyPos; // retireType - record separator - collection - record separator - lowerId 

        return Slice.External(context.Allocator, lowerId.Content, keyPos, size, out outSlice);
    }

    private string GetCollectionStringFromRetiredAttachmentsKey(DocumentsOperationContext context, Slice lowerId)
    {
        var colPos = 2;
        var sepPos = lowerId.Content.IndexOf(SpecialChars.RecordSeparator, colPos);

        using var dispose1 = Slice.External(context.Allocator, lowerId.Content, colPos, sepPos - colPos, out var collectionSlice);
        return collectionSlice.ToString();
    }

    public ByteStringContext<ByteStringMemoryCache>.ExternalScope CleanRetiredAttachmentsKey(DocumentsOperationContext context, Slice lowerId, out Slice outSlice)
    {
        //var type = GetRetireType(lowerId);

        //switch (type)
        //{
        //    case AttachmentRetireType.PutRetire:
                return RemoveTypeFromRetiredAttachmentsKey3(context, lowerId, out outSlice);

        //    case AttachmentRetireType.DeleteRetire:
        //        return RemoveTypeAndCollectionFromRetiredAttachmentsKey(context, lowerId, out outSlice);

        //    case AttachmentRetireType.ExistingRetire:
        //        return RemoveTypeFromRetiredAttachmentsKey2(context, lowerId, out outSlice);

        //    default:
        //        throw new ArgumentOutOfRangeException(nameof(type), type, null);
        //}
    }

    //public ByteStringContext<ByteStringMemoryCache>.ExternalScope RemoveTypeFromRetiredAttachmentsKey2(DocumentsOperationContext context, Slice lowerId, out Slice outSlice)
    //{
    //    var pos = 2;
    //    var size = lowerId.Content.Length - pos; // retireType - record separator - lowerId 
    //    return Slice.External(context.Allocator, lowerId.Content, pos, size, out outSlice);
    //}

    public ByteStringContext<ByteStringMemoryCache>.ExternalScope RemoveTypeFromRetiredAttachmentsKey3(DocumentsOperationContext context, Slice lowerId, out Slice outSlice)
    {
        var pos = 2;
        var size = lowerId.Content.Length - pos; // retireType - record separator - lowerId 
        return Slice.External(context.Allocator, lowerId.Content, pos, size, out outSlice);
    }
    protected override void HandleDocumentConflict(BackgroundWorkParameters options, Slice ticksAsSlice, Slice clonedId, Queue<DocumentExpirationInfo> expiredDocs, ref int totalCount)
    {
        if (ShouldHandleWorkOnCurrentNode(options.DatabaseRecord.Topology, options.NodeTag) == false)
            return;

        //using var scope = CleanRetiredAttachmentsKey(options.Context, clonedId, out Slice attachmentKey);
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

    public Task<Stream> GetRetiredAttachmentFromCloud(DocumentsOperationContext context, DirectFileDownloader downloader, Attachment attachment, OperationCancelToken tcs)
    {
        string collection;
        using (var documentInfoHelper = new DocumentInfoHelper(context))
        using (var document = Database.DocumentsStorage.Get(context, documentInfoHelper.GetDocumentId(attachment.Key), DocumentFields.Data, throwOnConflict: false))
        {
            collection = Database.DocumentsStorage.ExtractCollectionName(context, document.Data).Name;
        }

        return StreamForDownloadDestinationInternal(downloader, attachment.Base64Hash.ToString());
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

    public Queue<DocumentExpirationInfo> GetExistingAttachmentsToAddRetireAdd(BackgroundWorkParameters options, ref int totalCount, out Stopwatch duration, CancellationToken cancellationToken)
    {
        duration = Stopwatch.StartNew();
        var toProcess = new Queue<DocumentExpirationInfo>();
        var take = Math.Min(options.AmountToTake, options.MaxItemsToProcess);

        var processDateUniversalTime = options.CurrentTime.ToUniversalTime();
        var ticksBigEndian = Bits.SwapBytes(processDateUniversalTime.Ticks);

        Slice ticksAsSlice = TicksAsSlice(options, ticksBigEndian);

        foreach (var disposableSlice in Database.DocumentsStorage.AttachmentsStorage.GetAttachmentKeysByFlagAndHashIndexPrefix(options.Context, AttachmentFlags.None, take))
        {
            using (disposableSlice)
            {
                if (ShouldHandleWorkOnCurrentNode(options.DatabaseRecord.Topology, options.NodeTag) == false)
                    break;

                if (cancellationToken.IsCancellationRequested)
                    return toProcess;
                if (toProcess.Count >= options.AmountToTake)
                    return toProcess;
                if (totalCount >= options.MaxItemsToProcess)
                    return toProcess;

                using var attachmentKeyWithTypeDisposable = CreateRetiredAttachmentsKeyWithType(options.Context, disposableSlice.Key, AttachmentRetireType.ExistingRetire, out Slice key);
                var clonedId = key.Clone(options.Context.Transaction.InnerTransaction.Allocator);

                if (TryEnqueueItemToProcess(options, ref totalCount, clonedId, ticksAsSlice, toProcess) == false)
                    break;
            }
        }

        return toProcess;
    }

    private static unsafe Slice TicksAsSlice(BackgroundWorkParameters options, long ticksBigEndian)
    {
        using var scope = Slice.External(options.Context.Allocator, (byte*)&ticksBigEndian, sizeof(long), out Slice temp);
        var ticksAsSlice = temp.Clone(options.Context.Transaction.InnerTransaction.Allocator);
        return ticksAsSlice;
    }

    public int ProcessAddRetiredAtToExistingAttachments(DocumentsOperationContext context, Queue<DocumentExpirationInfo> attachments)
    {
        return 0;

        //TODO: egor

        //var retire = DateTime.MinValue.ToUniversalTime();
        //foreach (var info in attachments)
        //{
        //    using (CleanRetiredAttachmentsKey(context, info.LowerId, out var keySlice))
        //    {
        //        var attachment = Database.DocumentsStorage.AttachmentsStorage.GetAttachmentByKey(context, keySlice);

        //        //TODO: egor I want to use the ticks I sent to command, and not the ticks from retire dt, need to handle that when I will refactor the AttachmentsStorage.PutAttachment() method 
        //        Database.DocumentsStorage.AttachmentsStorage.PutAttachment(context, info.Id, attachment.Name, attachment.ContentType, attachment.Base64Hash.ToString(),
        //        attachment.Flags, attachment.Size, retireAtDt: retire, forceRetireAt: true);
        //    }
        //}

        //return attachments.Count;
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
