using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Raven.Server.Documents.Handlers.Processors.Attachments.Retired;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.DirectDownload;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using static Raven.Server.Documents.Schemas.Attachments;

namespace Raven.Server.Documents;

public class RetiredAttachmentsStorage : AbstractBackgroundWorkStorage
{
    private readonly Logger _logger;

    public RetiredAttachmentsStorage(Transaction tx, DocumentDatabase database) : base(tx, database, AttachmentsByRetire, nameof(AttachmentName.RetireAt))
    {
        _logger = LoggingSource.Instance.GetLogger<RetiredAttachmentsStorage>(database.Name);
    }

    private DocumentInfoHelper _documentInfoHelper;


    public IDisposable Initialize(DocumentsOperationContext context)
    {
        _documentInfoHelper = new DocumentInfoHelper(context);

        return new DisposableAction(() =>
        {
            _documentInfoHelper.Dispose();
        });
    }

    private const string AttachmentsByRetire = "AttachmentsByRetire";
    protected override void ProcessDocument(DocumentsOperationContext context, Slice lowerId, string id, DateTime currentTime)
    {
        var type = GetRetireType(lowerId);
        using var scope = CleanRetiredAttachmentsKey(context, lowerId, out var keySlice);
        switch (type)
        {
            case AttachmentRetireType.PutRetire:
                var collection = id; // for retire attachments, the id is the collection name
                if (string.IsNullOrEmpty(collection))
                    throw new InvalidOperationException($"Couldn't retire the attachment. Document collection is null. Lower id is '{lowerId}'.");
                ProcessDocumentForPutRetire(context, keySlice, collection, currentTime);
                break;

            case AttachmentRetireType.DeleteRetire:
                ProcessDocumentForDeleteRetire(context, keySlice, id, currentTime);
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(type), type, null);
        }
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
                context.Transaction.ForgetAbout(doc);
            }
        }
    }

    private void ProcessDocumentForDeleteRetire(DocumentsOperationContext context, Slice outSlice, string id, DateTime currentTime)
    {
        //TODO: egor do we want to do here anything?
        // here we already deleted the attachment metadata from document, and put a del value to retiretree, now when we are here it means we deleted the attachment from cloud as well.
    }

    protected override DocumentExpirationInfo GetDocumentAndIdOrCollection(BackgroundWorkParameters options, Slice clonedId, Slice ticksSlice)
    {
        var type = GetRetireType(clonedId);

        switch (type)
        {
            case AttachmentRetireType.PutRetire:
                return DocumentAndIdOrCollectionForPutRetire(options, clonedId, ticksSlice);

            case AttachmentRetireType.DeleteRetire:
                return DocumentAndIdOrCollectionForDeleteRetire(options, clonedId, ticksSlice);

            default:
                throw new ArgumentOutOfRangeException(nameof(type), type, null);
        }
    }

    private DocumentExpirationInfo DocumentAndIdOrCollectionForPutRetire(BackgroundWorkParameters options, Slice clonedId, Slice ticksSlice)
    {
        using var scope = CleanRetiredAttachmentsKey(options.Context, clonedId, out var keySlice);
        using (var id = _documentInfoHelper.GetDocumentId(keySlice))
        {
            if (id == null)
            {
                return new DocumentExpirationInfo(ticksSlice, clonedId, id: null);
            }
            // document is disposed in caller method
            var document = Database.DocumentsStorage.Get(options.Context, id, DocumentFields.Id | DocumentFields.Data | DocumentFields.ChangeVector);
            // doc was deleted
            if (document == null)
            {
                return new DocumentExpirationInfo(ticksSlice, clonedId, id: null);
            }

            if (document.TryGetCollection(out string collectionStr))
            {
                if (options.DatabaseRecord.RetiredAttachments.RetirePeriods.ContainsKey(collectionStr) == false)
                {
                    // we don't care about this collection, it was removed from the configuration
                    return new DocumentExpirationInfo(ticksSlice, clonedId, id: null);
                }
            }

            return new DocumentExpirationInfo(ticksSlice, clonedId, id: collectionStr)
            {
                Document = document
            };
        }
    }

    private DocumentExpirationInfo DocumentAndIdOrCollectionForDeleteRetire(BackgroundWorkParameters options, Slice clonedId, Slice ticksSlice)
    {
        using var scope = GetAttachmentsKeyAndCollectionSliceFromRetiredAttachmentsKey(options.Context, clonedId, out var keySlice, out var collectionSlice);
        using (var id = _documentInfoHelper.GetDocumentId(keySlice))
        {
            if (id == null)
            {
                return new DocumentExpirationInfo(ticksSlice, clonedId, id: null);
            }
            // document is disposed in caller method
            var document = Database.DocumentsStorage.Get(options.Context, id, DocumentFields.Id | DocumentFields.Data | DocumentFields.ChangeVector);
            // doc was deleted
            if (document == null)
            {
                // TODO: Do I need to check PurgeOnDelete again? 
                return new DocumentExpirationInfo(ticksSlice, clonedId, id: collectionSlice.ToString());
            }

            if (document.TryGetCollection(out string collectionStr))
            {
                if (options.DatabaseRecord.RetiredAttachments.RetirePeriods.ContainsKey(collectionStr) == false)
                {
                    // we don't care about this collection, it was removed from the configuration
                    return new DocumentExpirationInfo(ticksSlice, clonedId, id: null);
                }
            }

            return new DocumentExpirationInfo(ticksSlice, clonedId, id: collectionStr)
            {
                Document = document
            };
        }
    }
    [StorageIndexEntryKeyGenerator]
    internal static unsafe ByteStringContext.Scope GenerateHashAndFlagForAttachments(Transaction tx, ref TableValueReader tvr, out Slice slice)
    {
        var hashPtr = tvr.Read((int)AttachmentsTable.Hash, out var hashSize);

        var flags = *(int*)tvr.Read((int)AttachmentsTable.Flags, out var size);
        Debug.Assert(size == sizeof(int));
        var scope = tx.Allocator.Allocate( hashSize + sizeof(int), out var buffer);

        var span = new Span<byte>(buffer.Ptr, buffer.Length);
        new ReadOnlySpan<byte>(hashPtr, hashSize).CopyTo(span);
        MemoryMarshal.AsBytes(new Span<int>(ref flags)).CopyTo(span[hashSize..]);

        slice = new Slice(buffer);
        return scope;
    }

    public override void Put(DocumentsOperationContext context, Slice lowerId, string processDateString)
    {
        using (CreateRetiredAttachmentsKeyWithType(context, lowerId, AttachmentRetireType.PutRetire, out Slice key))
            base.Put(context, key, processDateString);
    }

    public unsafe void PutDelete(DocumentsOperationContext context, Slice lowerId, long ticks, string collection)
    {
        var ticksBigEndian = Bits.SwapBytes(ticks);

        var msg = $"Adding retired attachment delete with key: '{lowerId}', collection: {collection}, ticks: {ticks} to '{_treeName}' tree.";
        if (_logger.IsOperationsEnabled)
            _logger.Operations(msg);

        Debug.Assert(string.IsNullOrEmpty(collection) == false, "string.IsNullOrEmpty(collection) == false");
        var tree = context.Transaction.InnerTransaction.ReadTree(_treeName);
        using(CreateRetiredAttachmentsKeyWithTypeAndCollection(context, lowerId,AttachmentRetireType.DeleteRetire, collection, out Slice key))
        using (Slice.External(context.Allocator, (byte*)&ticksBigEndian, sizeof(long), out Slice ticksSlice))
            tree.MultiAdd(ticksSlice, key);
    }

    public unsafe void RemoveRetirePutValue(DocumentsOperationContext context, Slice lowerId, long ticks)
    {
        var ticksBigEndian = Bits.SwapBytes(ticks);

        var msg = $"Removing retired attachment put with key: '{lowerId}' from '{_treeName}' tree.";
        if (_logger.IsOperationsEnabled)
            _logger.Operations(msg);

        var tree = context.Transaction.InnerTransaction.ReadTree(_treeName);
        using (CreateRetiredAttachmentsKeyWithType(context, lowerId, AttachmentRetireType.PutRetire, out Slice key))
        using (Slice.External(context.Allocator, (byte*)&ticksBigEndian, sizeof(long), out Slice ticksSlice))
            tree.MultiDelete(ticksSlice, key);
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

            default:
                throw new ArgumentOutOfRangeException(nameof(retireType), retireType, null);
        }

        var outputBuffer = keyMem.ToSpan();
        fixed (char* pCollection = collection)
        {
            //var span = new ReadOnlySpan<byte>(pCollection, collection.Length * sizeof(char));
            //span.CopyTo(outputBuffer.Slice(pos));

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

            default:
                throw new ArgumentOutOfRangeException(nameof(retireType), retireType, null);
        }

        keyMem.Ptr[pos++] = SpecialChars.RecordSeparator;
        Memory.Copy(keyMem.Ptr + pos, lowerId.Content.Ptr, lowerId.Content.Length);

        outSlice=new Slice(SliceOptions.Key, keyMem);
        return scope;
    }
    public unsafe ByteStringContext.InternalScope RemoveTypeAndCollectionFromRetiredAttachmentsKey(DocumentsOperationContext context, Slice lowerId, out Slice outSlice)
    {
        var pos = 2;
        var keyPos = lowerId.Content.IndexOf(SpecialChars.RecordSeparator, pos) + 1;
        var size = lowerId.Content.Length - keyPos; // retireType - record separator - collection - record separator - lowerId 
     //   var scope = context.Allocator.Allocate(size, out ByteString keyMem);

       // Memory.Copy(keyMem.Ptr, lowerId.Content.Ptr + keyPos, size);

      //  outSlice = new Slice(SliceOptions.Key, keyMem);

        Slice.External(context.Allocator, lowerId.Content, keyPos, size, out outSlice);
        return default;
    }

    public unsafe ByteStringContext.InternalScope GetAttachmentsKeyAndCollectionSliceFromRetiredAttachmentsKey(DocumentsOperationContext context, Slice lowerId, out Slice outSlice, out Slice collectionSlice)
    {
        var colPos = 2;

        var sepPos = lowerId.Content.IndexOf(SpecialChars.RecordSeparator, colPos);
        var keyPos = sepPos + 1;
        var size = lowerId.Content.Length - keyPos; // retireType - record separator - collection - record separator - lowerId 
     //   var scope = context.Allocator.Allocate(size, out ByteString keyMem);

     //   Memory.Copy(keyMem.Ptr, lowerId.Content.Ptr + colPos, size);

        Slice.External(context.Allocator, lowerId.Content, colPos, sepPos - colPos, out collectionSlice);
        Slice.External(context.Allocator, lowerId.Content, keyPos, size, out outSlice);
        return default;
    }

    public ByteStringContext.InternalScope CleanRetiredAttachmentsKey(DocumentsOperationContext context, Slice lowerId, out Slice outSlice)
    {
        var type = GetRetireType(lowerId);

        switch (type)
        {
            case AttachmentRetireType.PutRetire:
                return RemoveTypeFromRetiredAttachmentsKey2(context, lowerId, out  outSlice);

            case AttachmentRetireType.DeleteRetire:
                return RemoveTypeAndCollectionFromRetiredAttachmentsKey(context, lowerId, out outSlice);

            default:
                throw new ArgumentOutOfRangeException(nameof(type), type, null);
        }
    }

    public unsafe ByteStringContext.InternalScope RemoveTypeFromRetiredAttachmentsKey2(DocumentsOperationContext context, Slice lowerId, out Slice outSlice)
    {
        var pos = 2;
        var size = lowerId.Content.Length - pos; // retireType - record separator - lowerId 
      //  var scope = context.Allocator.Allocate(size, out ByteString keyMem);

      //  Memory.Copy(keyMem.Ptr, lowerId.Content.Ptr + pos, size);

        Slice.External(context.Allocator, lowerId.Content, pos, size, out outSlice);
   //     outSlice = new Slice(SliceOptions.Key, keyMem);
        return default;
    }

    protected override void HandleDocumentConflict(BackgroundWorkParameters options, Slice ticksAsSlice, Slice clonedId, Queue<DocumentExpirationInfo> expiredDocs, ref int totalCount)
    {
        // TODO: egor We somehow need to make sure we upload the attachment just once? or we dont care?
        if (ShouldHandleWorkOnCurrentNode(options.DatabaseRecord.Topology, options.NodeTag) == false)
            return;

        using var scope = CleanRetiredAttachmentsKey(options.Context, clonedId, out Slice attachmentKey);
        using (var docId = _documentInfoHelper.GetDocumentId(attachmentKey))
        {
            (bool allExpired, string id) = GetConflictedRetiredAttachment(options.Context, options.CurrentTime, docId, attachmentKey);

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

    public AttachmentRetireType GetRetireType(Slice clonedId)
    {
        // this method get substring until record separator
        using (var type = _documentInfoHelper.GetDocumentId(clonedId))
        {
            switch (type)
            {
                case "p":
                    return AttachmentRetireType.PutRetire;

                case "d":
                    return AttachmentRetireType.DeleteRetire;

                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, $"Got unknown '{nameof(AttachmentRetireType)}' from retired key: '{clonedId}'. Should not happen and likely a bug!");
            }
        }
    }

    public DirectBackupDownloader GetDownloader(DocumentsOperationContext context, OperationCancelToken tcs)
    {
        var config = Database.ServerStore.Cluster.ReadRetireAttachmentsConfiguration(Database.Name);
        if (config == null)
            throw new InvalidOperationException($"Cannot get retired attachment because {nameof(RetiredAttachmentsConfiguration)} is not configured on {Database.Name}.");
        if (config.Disabled)
            throw new InvalidOperationException($"Cannot get retired attachment because {nameof(RetiredAttachmentsConfiguration)} is disabled.");

        var settings = UploaderSettings.GenerateDirectUploaderSetting(Database, nameof(RetiredAttachmentHandlerProcessorForGet), config.S3Settings, config.AzureSettings, glacierSettings: null, googleCloudSettings: null, ftpSettings: null);
        return new DirectBackupDownloader(settings, retentionPolicyParameters: null, _logger, BackupUploaderBase.GenerateUploadResult(), progress => { }, tcs);
    }

    public Task<Stream> GetRetiredAttachmentFromCloud(DocumentsOperationContext context, DirectBackupDownloader downloader, Attachment attachment, OperationCancelToken tcs)
    {
        string collection;
        using (var documentInfoHelper = new DocumentInfoHelper(context))
        using (var document = Database.DocumentsStorage.Get(context, documentInfoHelper.GetDocumentId(attachment.Key), DocumentFields.Data, throwOnConflict: false))
        {
            collection = Database.DocumentsStorage.ExtractCollectionName(context, document.Data).Name;
        }

        return StreamForDownloadDestinationInternal(downloader, attachment, collection);
    }

    public async Task<Stream> StreamForDownloadDestinationInternal(DirectBackupDownloader downloader, Attachment attachment, string collection)
    {
        var keyStr = attachment.Key.ToString();
        var objKeyName = Convert.ToBase64String(Encoding.UTF8.GetBytes(keyStr));
        var folderName = $"{collection}";

        return await downloader.StreamForDownloadDestination(Database, folderName, objKeyName);
    }

    public enum AttachmentRetireType : byte
    {
        PutRetire = 1,
        DeleteRetire = 2
    }
}
