using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Documents.Replication;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;
using Bits = Sparrow.Binary.Bits;

namespace Raven.Server.Documents.Versioning
{
    public unsafe class VersioningStorage
    {
        public static readonly Slice KeyAndEtagSlice;
        public static readonly Slice RevisionsEtags;
        private static Logger _logger;

        private static readonly TableSchema DocsSchema;

        private readonly DocumentDatabase _database;
        private readonly VersioningConfiguration _versioningConfiguration;

        private const string RevisionDocuments = "RevisionDocuments";
        private const string RevisionsCount = "RevisionsCount";

        private enum Columns
        {
            LoweredKey = 0,
            Separator = 1,
            ChangeVector = 2,
            Etag = 3,
            Key = 4,
            Document = 5
        }

        private const byte Seperator = 30;
        private readonly VersioningConfigurationCollection _emptyConfiguration = new VersioningConfigurationCollection();

        private VersioningStorage(DocumentDatabase database, VersioningConfiguration versioningConfiguration)
        {
            _database = database;
            _versioningConfiguration = versioningConfiguration;

            _logger = LoggingSource.Instance.GetLogger<VersioningStorage>(database.Name);

            using (var tx = database.DocumentsStorage.Environment.WriteTransaction())
            {
                DocsSchema.Create(tx, RevisionDocuments, 16);

                tx.CreateTree(RevisionsCount);

                tx.Commit();
            }
        }

        static VersioningStorage()
        {
            Slice.From(StorageEnvironment.LabelsContext, "KeyAndEtag", ByteStringType.Immutable, out KeyAndEtagSlice);
            Slice.From(StorageEnvironment.LabelsContext, "RevisionsEtags", ByteStringType.Immutable, out RevisionsEtags);
            // The documents schema is as follows
            // 6 fields (lowered key, recored separator, etag, change vector, lazy string key, document)
            // We are you using the record separator in order to avoid loading another documents that has the same key prefix, 
            //      e.g. fitz(record-separator)01234567 and fitz0(record-separator)01234567, without the record separator we would have to load also fitz0 and filter it.
            // format of lazy string key is detailed in GetLowerKeySliceAndStorageKey
            DocsSchema = new TableSchema();
            DocsSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)Columns.LoweredKey,
                Count = 3,
                Name = KeyAndEtagSlice
            });
            
            DocsSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)Columns.Etag,
                Name = RevisionsEtags
            });
        }

        public static VersioningStorage LoadConfigurations(DocumentDatabase database)
        {
            DocumentsOperationContext context;
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var configuration = database.DocumentsStorage.Get(context, Constants.Documents.Versioning.ConfigurationKey);
                if (configuration == null)
                    return null;

                try
                {
                    var versioningConfiguration = JsonDeserializationServer.VersioningConfiguration(configuration.Data);
                    return new VersioningStorage(database, versioningConfiguration);
                }
                catch (Exception e)
                {
                    //TODO: This should generate an alert, so admin will know that something is very bad
                    //TODO: Or this should throw and we should have a config flag to ignore the error
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Cannot enable versioning for documents as the versioning configuration document {Constants.Documents.Versioning.ConfigurationKey} is not valid: {configuration.Data}", e);
                    return null;
                }
            }
        }

        private VersioningConfigurationCollection GetVersioningConfiguration(CollectionName collectionName)
        {
            VersioningConfigurationCollection configuration;
            if (_versioningConfiguration.Collections != null && _versioningConfiguration.Collections.TryGetValue(collectionName.Name, out configuration))
            {
                return configuration;
            }

            if (_versioningConfiguration.Default != null)
            {
                return _versioningConfiguration.Default;
            }

            return _emptyConfiguration;
        }

        public bool PutFromDocument(DocumentsOperationContext context, CollectionName collectionName, string key, 
            BlittableJsonReaderObject document, ChangeVectorEntry[] changeVector = null)
        {
            var enableVersioning = false;
            BlittableJsonReaderObject metadata;
            if (document.TryGet(Constants.Documents.Metadata.Key, out metadata))
            {
                bool disableVersioning;
                if (metadata.TryGet(Constants.Documents.Versioning.DisableVersioning, out disableVersioning))
                {
                    DynamicJsonValue mutatedMetadata;
                    Debug.Assert(metadata.Modifications == null);

                    metadata.Modifications = mutatedMetadata = new DynamicJsonValue(metadata);
                    mutatedMetadata.Remove(Constants.Documents.Versioning.DisableVersioning);
                    if (disableVersioning)
                        return false;
                }

                if (metadata.TryGet(Constants.Documents.Versioning.EnableVersioning, out enableVersioning))
                {
                    DynamicJsonValue mutatedMetadata = metadata.Modifications;
                    if (mutatedMetadata == null)
                        metadata.Modifications = mutatedMetadata = new DynamicJsonValue(metadata);
                    mutatedMetadata.Remove(Constants.Documents.Versioning.EnableVersioning);
                }
            }
            
            var configuration = GetVersioningConfiguration(collectionName);
            if (enableVersioning == false && configuration.Active == false)
                return false;

            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, RevisionDocuments);
            Slice prefixSlice;
            using (DocumentKeyWorker.GetSliceFromKey(context, key, out prefixSlice))
            {
                var revisionsCount = IncrementCountOfRevisions(context, prefixSlice, 1);
                DeleteOldRevisions(context, table, prefixSlice, configuration.MaxRevisions, revisionsCount);

                PutInternal(context, key, document, table, changeVector);
            }

            return true;
        }

        
        public void PutDirect(DocumentsOperationContext context, string key,  BlittableJsonReaderObject document, ChangeVectorEntry[] changeVector)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, RevisionDocuments);
            PutInternal(context, key, document, table, changeVector);
        }

        private void PutInternal(DocumentsOperationContext context, string key, BlittableJsonReaderObject document
            , Table table, ChangeVectorEntry[] changeVector)
        {
            
            BlittableJsonReaderObject.AssertNoModifications(document, key, assertChildren: true);

            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            DocumentKeyWorker.GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);

            var data = context.ReadObject(document, key);

            //byte recordSeperator = 30;
            var newEtag = _database.DocumentsStorage.GenerateNextEtag();
            var newEtagBigEndian = Bits.SwapBytes(newEtag);

            if (changeVector == null)
            {
                changeVector = new[]
                {
                    new ChangeVectorEntry
                    {
                        DbId = _database.DbId,
                        Etag = newEtag
                    }
                };
            }

            if (CheckIfVersionExists(context, key, changeVector))
            {
                return;
            }

            fixed (ChangeVectorEntry* pChangeVector = changeVector)
            {
                var tbv = new TableValueBuilder
                {
                    {lowerKey, lowerSize},
                    Seperator,
                    {(byte*) pChangeVector, sizeof(ChangeVectorEntry)*changeVector.Length},
                    {(byte*)&newEtagBigEndian, sizeof(long)},
                    {keyPtr, keySize},
                    {data.BasePointer, data.Size}
                };
                table.Insert(tbv);
            }         
        }

        private void DeleteOldRevisions(DocumentsOperationContext context, Table table, Slice prefixSlice, long? maxRevisions, long revisionsCount)
        {
            if (maxRevisions.HasValue == false || maxRevisions.Value == int.MaxValue)
                return;

            var numberOfRevisionsToDelete = revisionsCount - maxRevisions.Value;
            if (numberOfRevisionsToDelete <= 0)
                return;

            var deletedRevisionsCount = DeleteRevisions(context, table, prefixSlice, numberOfRevisionsToDelete);
            Debug.Assert(numberOfRevisionsToDelete == deletedRevisionsCount);
            IncrementCountOfRevisions(context, prefixSlice, -deletedRevisionsCount);
        }

        private  long DeleteRevisions(DocumentsOperationContext context, Table table, Slice prefixSlice,
            long numberOfRevisionsToDelete)
        {
            long maxEtagDeleted = 0;

            var deletedRevisionsCount = table.DeleteForwardFrom(DocsSchema.Indexes[KeyAndEtagSlice], prefixSlice,
                numberOfRevisionsToDelete,
                deleted =>
                {
                    int size;
                    var etag = Bits.SwapBytes(*(long*) deleted.Reader.Read(2, out size));
                    maxEtagDeleted = Math.Max(maxEtagDeleted, etag);
                });
            _database.DocumentsStorage.EnsureLastEtagIsPersisted(context, maxEtagDeleted);
            return deletedRevisionsCount;
        }

        private long IncrementCountOfRevisions(DocumentsOperationContext context, Slice prefixedLoweredKey, long delta)
        {
            var numbers = context.Transaction.InnerTransaction.ReadTree(RevisionsCount);
            return numbers.Increment(prefixedLoweredKey, delta);
        }

        private void DeleteCountOfRevisions(DocumentsOperationContext context, Slice prefixedLoweredKey)
        {
            var numbers = context.Transaction.InnerTransaction.ReadTree(RevisionsCount);
            numbers.Delete(prefixedLoweredKey);
        }

        public void Delete(DocumentsOperationContext context, CollectionName collectionName, Slice loweredKey)
        {
            var configuration = GetVersioningConfiguration(collectionName);
            if (configuration.Active == false)
                return;

            if (configuration.PurgeOnDelete == false)
                return;

            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, RevisionDocuments);
            var prefixKeyMem = default(ByteString);
            try
            {
                prefixKeyMem = context.Allocator.Allocate(loweredKey.Size + 1);
                loweredKey.CopyTo(0, prefixKeyMem.Ptr, 0, loweredKey.Size);
                prefixKeyMem.Ptr[loweredKey.Size] = (byte)30; // the record separator                
                var prefixSlice = new Slice(SliceOptions.Key, prefixKeyMem);

                DeleteRevisions(context, table, prefixSlice, long.MaxValue);
                DeleteCountOfRevisions(context, prefixSlice);
            }
            finally
            {
                if (prefixKeyMem.HasValue)
                    context.Allocator.Release(ref prefixKeyMem);
            }
        }

        public IEnumerable<Document> GetRevisions(DocumentsOperationContext context, string key, int start, int take)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, RevisionDocuments);

            Slice prefixSlice;
            using (DocumentKeyWorker.GetSliceFromKey(context, key, out prefixSlice))
            {
                // ReSharper disable once LoopCanBeConvertedToQuery
                foreach (
                    var sr in table.SeekForwardFrom(DocsSchema.Indexes[KeyAndEtagSlice], prefixSlice, startsWith: true)
                )
                {
                    foreach (var tvr in sr.Results)
                    {
                        if (start > 0)
                        {
                            start--;
                            continue;
                        }
                        if (take-- <= 0)
                            yield break;

                        var document = TableValueToDocument(context, ref tvr.Reader);
                        yield return document;
                    }
                    if (take <= 0)
                        yield break;
                }
            }
        }

        public IEnumerable<Document> GetRevisionsAfter(DocumentsOperationContext context, long etag, int take)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, RevisionDocuments);

            foreach (var tvr in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[RevisionsEtags], etag))
            {
                var document = TableValueToDocument(context, ref tvr.Reader);
                yield return document;

                if (take-- <= 0)
                    yield break;
            }
        }

        public IEnumerable<ReplicationBatchDocumentItem> GetRevisionsAfter(DocumentsOperationContext context, long etag)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, RevisionDocuments);

            foreach (var tvr in table.SeekForwardFrom(DocsSchema.FixedSizeIndexes[RevisionsEtags], etag))
            {
                yield return TableValueToDocument(context, ref tvr.Reader);
            }
        }

        private bool CheckIfVersionExists(DocumentsOperationContext context,string key, ChangeVectorEntry[] changeVector)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, RevisionDocuments);
            Slice keySlice;
            DocumentKeyWorker.GetSliceFromKey(context, key, out keySlice);
            fixed (ChangeVectorEntry* changeVectorPtr = changeVector)
            {
                Slice changeVectorSlice;
                using (Slice.External(context.Allocator, (byte*)changeVectorPtr, sizeof(ChangeVectorEntry) * changeVector.Length,
                    out changeVectorSlice))
                {
                    *(keySlice.Content.Ptr + keySlice.Size) = Seperator;
                    changeVectorSlice.CopyTo(keySlice.Content.Ptr + keySlice.Size + 1);
                }
            }
            foreach (var tvr in table.SeekForwardFrom(DocsSchema.Indexes[KeyAndEtagSlice],keySlice, startsWith: true))
            {
                foreach (var tableValueHolder in tvr.Results)
                {
                    var entry = TableValueToDocument(context, ref tableValueHolder.Reader);
                    if (entry.ChangeVector.SequenceEqual(changeVector))
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        private static Document TableValueToDocument(JsonOperationContext context, ref TableValueReader tvr)
        {
            var result = new Document
            {
                StorageId = tvr.Id
            };
            int size;
            // See format of the lazy string key in the GetLowerKeySliceAndStorageKey method
            var ptr = tvr.Read((int)Columns.Key, out size);
            byte offset;
            size = BlittableJsonReaderBase.ReadVariableSizeInt(ptr, 0, out offset);
            result.Key = new LazyStringValue(null, ptr + offset, size, context);
            ptr = tvr.Read((int)Columns.Etag, out size);
            result.Etag = Bits.SwapBytes(*(long*)ptr);
            result.Data = new BlittableJsonReaderObject(tvr.Read((int)Columns.Document, out size), size, context);


            int changeVectorSize = sizeof(ChangeVectorEntry);
            ptr = tvr.Read((int) Columns.ChangeVector, out size);
            int changeVecotorCount = size/changeVectorSize;
            result.ChangeVector = new ChangeVectorEntry[changeVecotorCount];
            for (var i = 0; i < changeVecotorCount; i++)
            {
                result.ChangeVector[i] = ((ChangeVectorEntry*) ptr)[i];
            }

            result.Flags = DocumentFlags.FromVersionStorage;

            return result;
        }

        public long GetNumberOfRevisionDocuments(DocumentsOperationContext context)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, RevisionDocuments);
            return table.GetNumberEntriesFor(DocsSchema.FixedSizeIndexes[RevisionsEtags]);
        }
    }
}