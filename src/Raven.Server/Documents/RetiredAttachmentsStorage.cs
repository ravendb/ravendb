using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.Policy;
using System.Threading.Tasks;
using CsvHelper.Configuration;
using Elastic.Clients.Elasticsearch;
using Google.Apis.Storage.v1.Data;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Extensions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents.ETL.Providers.OLAP;
using Raven.Server.Documents.Handlers.Processors.Attachments.Retired;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.DirectDownload;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Schemas;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
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
    //internal readonly TableSchema RetiredAttachmentsSchema;
    public RetiredAttachmentsStorage(Transaction tx, DocumentDatabase database) : base(tx, database,
        LoggingSource.Instance.GetLogger<RetiredAttachmentsStorage>(database.Name), AttachmentsByRetire, nameof(AttachmentName.RetireAt))
    {
        //RetiredAttachmentsSchema = database.DocumentsStorage.RetiredAttachmentsSchema;
    }

    private DocumentInfoHelper _documentInfoHelper;

    // TODO: egor test that removes document with retired attachment
    // TODO: egor test that removes attachment from document with retired attachment
    // TODO: egor test that updates retired attachment  with regular attachment in document

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
        using var scope = RemoveTypeFromRetiredAttachmentsKey(context, lowerId, out var keySlice);
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
                // TODO: egor if I just return here, need to delete the attachment from cloud storage?
                if (doc == null || doc.TryGetMetadata(out var metadata) == false)
                    return;

                if (metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                    return;
                var nameByKey = Database.DocumentsStorage.AttachmentsStorage.GetAttachmentNameByKey(context, lowerId);
                if (nameByKey == null)
                    return;
                //var results = new DynamicJsonArray();
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
                            //DocumentId = lowerDocId
                        }, lowerId);

                        break;
                        //results.Add(attachmentInMetadata);
                    }
                }

                //var results = new DynamicJsonArray();
                //var hasRetired = false;
                //for (var i = 0; i < attachments.Length; i++)
                //{
                //    var attachmentInMetadata = (BlittableJsonReaderObject)attachments[i];
                //    if (attachmentInMetadata.TryGet(nameof(AttachmentName.Name), out string name) == false)
                //        continue;

                //    if (name == nameByKey)
                //    {
                //        hasRetired = true;
                //        // mark as retired

                //        attachmentInMetadata.TryGet(nameof(AttachmentName.Flags), out AttachmentFlags flags);

                //        flags |= AttachmentFlags.Retired;
                //        attachmentInMetadata.Modifications = new DynamicJsonValue(attachmentInMetadata)
                //        {
                //            [nameof(AttachmentName.Flags)] = flags.ToString()
                //        };
                //        results.Add(attachmentInMetadata);
                //    }
                //    else
                //    {
                //        results.Add(attachmentInMetadata);
                //    }
                //}

                //if (hasRetired)
                //{
                //    metadata.Modifications = new DynamicJsonValue(metadata)
                //    {
                //        [Constants.Documents.Metadata.Attachments] = results
                //    };
                //    doc.Data.Modifications = new DynamicJsonValue(doc.Data)
                //    {
                //        [Constants.Documents.Metadata.Key] = metadata
                //    };
                //}

                //using (var old = doc.Data)
                //{
                //    var newDocument = context.ReadObject(old, docId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                //    Database.DocumentsStorage.Put(context, docId, null, newDocument, flags: DocumentFlags.HasAttachments, nonPersistentFlags: NonPersistentDocumentFlags.ByAttachmentUpdate);
                //}

                //foreach (BlittableJsonReaderObject res in results)
                //{
                //    if (res.TryGet(nameof(AttachmentName.Hash), out string hash) == false)
                //        continue;

                //    using (Slice.From(context.Allocator, hash, out Slice hashSlice))
                //    {
                //        context.Transaction.CheckIfShouldDeleteAttachmentStream(hashSlice, fromRetire: true);
                //    }
                //}

            }
        }
    }

    private void ProcessDocumentForDeleteRetire(DocumentsOperationContext context, Slice outSlice, string id, DateTime currentTime)
    {
        //TODO: egor do we want to do here anything?
        // maybe the alcohoritm should be different for delete retire? like 1. add delete to retireTree then go over it, then delete the actual attachment (if exists) ?
        //throw new NotImplementedException();
    }
    //private void PutRetiredAttachment(DocumentsOperationContext context, Slice lowerId)
    //{
    //    if (context.Transaction == null)
    //    {
    //        DocumentPutAction.ThrowRequiresTransaction();
    //        Debug.Assert(false);// never hit
    //    }

    //    var table = context.Transaction.InnerTransaction.OpenTable(RetiredAttachmentsSchema, RetiredAttachmentsSlice);

    //    using (table.Allocate(out TableValueBuilder tvb))
    //    {
    //        unsafe
    //        {

    //            //LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType = 0,
    //            //Name = 2,
    //            //ContentType = 3,
    //            //Hash = 4,
    //            //Size = 5
    //            tvb.Add(lowerId.Content.Ptr, lowerId.Size);
    //            table.Insert(tvb);
    //        }
    //    }
    //}

    protected override MyStruct GetDocumentAndIdOrCollection(BackgroundWorkParameters options, Slice clonedId)
    {
        var type = GetRetireType(clonedId);
        using var scope = RemoveTypeFromRetiredAttachmentsKey(options.Context, clonedId, out var keySlice);
        switch (type)
        {
            case AttachmentRetireType.PutRetire:
                return GetDocumentIdForPutRetire(options, keySlice);

            case AttachmentRetireType.DeleteRetire:
                return GetDocumentIdForDeleteRetire(options, keySlice);

            default:
                throw new ArgumentOutOfRangeException(nameof(type), type, null);
            
        }

    }

    private MyStruct GetDocumentIdForPutRetire(BackgroundWorkParameters options, Slice clonedId)
    {

        using (var id = _documentInfoHelper.GetDocumentId(clonedId))
        {
            if (id == null)
            {
                return null;
            }
            // document is disposed in caller method
            var document = Database.DocumentsStorage.Get(options.Context, id, DocumentFields.Id | DocumentFields.Data | DocumentFields.ChangeVector);
            // doc was deleted
            if (document == null)
            {
                return null;
            }

            if (document.TryGetCollection(out string collectionStr))
            {
                if (options.DatabaseRecord.RetireAttachments.RetirePeriods.ContainsKey(collectionStr) == false)
                {
                    // we don't care about this collection, it was removed from the configuration
                    return null;
                }
                //TODO: egor do I care regarding retireAt here?

                //if (options.DatabaseRecord.RetireAttachments.RetirePeriods.TryGetValue(collectionStr, out var timeSpan))
                //{
                //    var retire = DateTime.UtcNow + timeSpan;
                //    Database.DocumentsStorage.AttachmentsStorage.RetireAttachmentsStorage.Get(context, keySlice, retire.GetDefaultRavenFormat());
                //}

            }

            return new MyStruct() { Document = document, Id = collectionStr };
        }
    }

    private MyStruct GetDocumentIdForDeleteRetire(BackgroundWorkParameters options, Slice clonedId)
    {
        using (var id = _documentInfoHelper.GetDocumentId(clonedId))
        {
            if (id == null)
            {
                return null;
            }

            var document = Database.DocumentsStorage.Get(options.Context, id, DocumentFields.Id | DocumentFields.Data | DocumentFields.ChangeVector);
            // doc was deleted
            if (document == null)
            {
                return null;
            }

            if (document.TryGetCollection(out string collectionStr))
            {
                if (options.DatabaseRecord.RetireAttachments.RetirePeriods.ContainsKey(collectionStr) == false)
                {
                    // we don't care about this collection, it was removed from the configuration
                    return null;
                }
                //TODO: egor do I care regarding retireAt here?

                //if (options.DatabaseRecord.RetireAttachments.RetirePeriods.TryGetValue(collectionStr, out var timeSpan))
                //{
                //    var retire = DateTime.UtcNow + timeSpan;
                //    Database.DocumentsStorage.AttachmentsStorage.RetireAttachmentsStorage.Get(context, keySlice, retire.GetDefaultRavenFormat());
                //}

            }
            return new MyStruct() { Document = document, Id = collectionStr };
        }
    }

    [StorageIndexEntryKeyGenerator]
    internal static unsafe ByteStringContext.Scope GenerateHashAndFlagForAttachments(Transaction tx, ref TableValueReader tvr, out Slice slice)
    {
        var hashPtr = tvr.Read((int)AttachmentsTable.Hash, out var hashSize);

        var flags = *(int*)tvr.Read((int)AttachmentsTable.Flags, out var size);
        Debug.Assert(size == sizeof(int));
        //flags = Bits.SwapBytes(flags);
        var scope = tx.Allocator.Allocate( hashSize + sizeof(int), out var buffer);

        var span = new Span<byte>(buffer.Ptr, buffer.Length);
        new ReadOnlySpan<byte>(hashPtr, hashSize).CopyTo(span);
        MemoryMarshal.AsBytes(new Span<int>(ref flags)).CopyTo(span[hashSize..]);

        slice = new Slice(buffer);
        return scope;
    }

    //internal static unsafe void UpdateHashAndFlagForAttachments(Transaction tx, Slice key, ref TableValueReader oldValue, ref TableValueReader newValue)
    //{
    //    //var hashPtrOld = oldValue.Read((int)AttachmentsTable.Hash, out var hashSizeOld);

    //    //var flagsOld = *(int*)oldValue.Read((int)AttachmentsTable.Flags, out var sizeOld);
    //    //var hashPtrNew = newValue.Read((int)AttachmentsTable.Hash, out var hashSizeNew);

    //    //var flagsNew = *(int*)newValue.Read((int)AttachmentsTable.Flags, out var sizeNew);




    //    //Console.WriteLine();
    //}
    public override void Put(DocumentsOperationContext context, Slice lowerId, string processDateString)
    {
        using (CreateRetiredAttachmentsKeyWithType(context, lowerId, AttachmentRetireType.PutRetire, out Slice key))
            base.Put(context, key, processDateString);
    }

    public unsafe void PutDelete(DocumentsOperationContext context, Slice lowerId, long ticks)
    {
        var ticksBigEndian = Bits.SwapBytes(ticks);

        var msg = $"Adding retired attachment delete with key: '{lowerId}' to '{_treeName}' tree.";
        if (Logger.IsOperationsEnabled)
            Logger.Operations(msg);

        var tree = context.Transaction.InnerTransaction.ReadTree(_treeName);
        using(CreateRetiredAttachmentsKeyWithType(context, lowerId,AttachmentRetireType.DeleteRetire, out Slice key))
        using (Slice.External(context.Allocator, (byte*)&ticksBigEndian, sizeof(long), out Slice ticksSlice))
            tree.MultiAdd(ticksSlice, key);
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
    public unsafe ByteStringContext.InternalScope RemoveTypeFromRetiredAttachmentsKey(DocumentsOperationContext context, Slice lowerId, out Slice outSlice)
    {
        var pos = 2;
        var size = lowerId.Content.Length - pos; // retireType - record separator - lowerId 
        var scope = context.Allocator.Allocate(size, out ByteString keyMem);

        Memory.Copy(keyMem.Ptr, lowerId.Content.Ptr + pos, size);

        outSlice = new Slice(SliceOptions.Key, keyMem);
        return scope;
    }

    public enum AttachmentRetireType : byte
    {
        PutRetire = 1,
        DeleteRetire = 2
    }
    protected override void HandleDocumentConflict(BackgroundWorkParameters options, Slice ticksAsSlice, Slice clonedId, Queue<DocumentExpirationInfo> expiredDocs, ref int totalCount)
    {
        // We somehow need to make sure we upload the attachment just once.
  
        if (ShouldHandleWorkOnCurrentNode(options.DatabaseRecord.Topology, options.NodeTag) == false)
            return;

        using var scope = RemoveTypeFromRetiredAttachmentsKey(options.Context, clonedId, out Slice attachmentKey);
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

    public AttachmentRetireType GetRetireType(/*DocumentsOperationContext context, */Slice clonedId)
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
        // TODO: egor maybe instead of allocating it each time, I can make it a field in the class? but then I need to handle config changes :(
        var config = Database.ServerStore.Cluster.ReadRetireAttachmentsConfiguration(Database.Name);
        if (config == null)
            throw new InvalidOperationException($"Cannot get retired attachment because {nameof(RetireAttachmentsConfiguration)} is not configured on {Database.Name}.");
        if (config.Disabled)
            throw new InvalidOperationException($"Cannot get retired attachment because {nameof(RetireAttachmentsConfiguration)} is disabled.");

        var settings = UploaderSettings.GenerateDirectUploaderSetting(Database, nameof(RetiredAttachmentHandlerProcessorForGetRetiredAttachment), config.S3Settings, config.AzureSettings, config.GlacierSettings, config.GoogleCloudSettings, config.FtpSettings);
        return new DirectBackupDownloader(settings, retentionPolicyParameters: null, Logger, OlapEtl.GenerateUploadResult(), progress => { }, tcs);
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
        var objKeyName = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(keyStr));
        var folderName = $"{collection}";
        //  var folderName = $"{Database.Name}/{collection}";


        //TODO: egor in case the blob doesnt exists need to throw a custom exception ?
        return await downloader.StreamForDownloadDestination(Database, folderName, objKeyName);
    }
}
