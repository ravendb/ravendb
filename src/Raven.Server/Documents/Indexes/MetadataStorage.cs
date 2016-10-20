using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using Raven.Abstractions.Extensions;
using Raven.Client.Replication.Messages;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes
{
    public class MetadataStorage : IDisposable
    {
        private static readonly Slice MetadataIdKeyName;
        private static readonly Slice TombstoneKeyName;
        private static readonly Slice ChangeVectorIndexName;
        private static readonly Slice MetadataEtagIndexName;
        private static readonly Slice TombstoneEtagIndexName;

        private const string GlobalChangeVectorTreeName = "GlobalMetadata";
        private const string IndexMetadataTableName = "IndexMetadataTable";
        private const string TransformerMetadataTableName = "TransformerMetadataTable";
        private const string TombstonesTableName = "TombstonesTable";

        private readonly Logger _logger;
        private readonly DocumentDatabase _documentDatabase;
        private readonly TableSchema _metadataSchema = new TableSchema();
        private readonly TableSchema _tombstoneSchema = new TableSchema();
        private TransactionContextPool _contextPool;

        private bool _isInitialized;

        public StorageEnvironment Environment { get; private set; }

        static MetadataStorage()
        {
            Slice.From(StorageEnvironment.LabelsContext, "MetadataIdKeyName", 
                ByteStringType.Immutable, out MetadataIdKeyName);
            Slice.From(StorageEnvironment.LabelsContext, "TombstoneKeyName",
                ByteStringType.Immutable, out TombstoneKeyName);
            Slice.From(StorageEnvironment.LabelsContext, "ChangeVectorIndexName",
                ByteStringType.Immutable, out ChangeVectorIndexName);
            Slice.From(StorageEnvironment.LabelsContext, "EtagIndexName", ByteStringType.Immutable,
                out MetadataEtagIndexName);

            Slice.From(StorageEnvironment.LabelsContext, "StorageTypeAndEtagIndex", ByteStringType.Immutable,
                out TombstoneEtagIndexName);
        }

        public MetadataStorage(DocumentDatabase documentDatabase)
        {
            _documentDatabase = documentDatabase;
            _logger = LoggingSource.Instance.GetLogger<MetadataStorage>("IndexMetadataStore_Of_" + _documentDatabase.Name);
        }


        public void Initialize(StorageEnvironment environment)
        {
            if (_isInitialized)
                return;
            _isInitialized = true;
            Environment = environment;
            try
            {
                _contextPool = new TransactionContextPool(Environment);

                using (var tx = Environment.WriteTransaction())
                {
                    _metadataSchema.DefineKey(new TableSchema.SchemaIndexDef
                    {
                        IsGlobal = false,
                        StartIndex = (int)MetadataFields.Id,
                        Count = 1,
                        Name = MetadataIdKeyName
                    });

                    _metadataSchema.DefineIndex(new TableSchema.SchemaIndexDef
                    {
                        IsGlobal = true,
                        Count = 1,
                        StartIndex = (int)MetadataFields.ChangeVector,
                        Name = ChangeVectorIndexName,
                    });

                    _metadataSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
                    {
                        IsGlobal = true,
                        StartIndex = (int)MetadataFields.Etag,
                        Name = MetadataEtagIndexName,
                    });

                    _metadataSchema.Create(tx, IndexMetadataTableName);
                    _metadataSchema.Create(tx, TransformerMetadataTableName);

                    _tombstoneSchema.DefineKey(new TableSchema.SchemaIndexDef
                    {
                        IsGlobal = true,
                        StartIndex = (int)TombstoneFields.DeletedEtag,
                        Count = 1,
                        Name = TombstoneKeyName
                    });

                    _tombstoneSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
                    {
                        IsGlobal = true,
                        StartIndex = (int)TombstoneFields.Etag,
                        Name = TombstoneEtagIndexName
                    });

                    _tombstoneSchema.Create(tx, TombstonesTableName);
                    tx.CreateTree(GlobalChangeVectorTreeName);

                    tx.Commit();
                }
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations("Could not open index metadata store for " + _documentDatabase.Name, e);

                Dispose();
                throw;
            }
        }

        public unsafe IndexTransformerTombstone WriteNewTombstoneFor(int id, MetadataStorageType type)
        {
            JsonOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = Environment.WriteTransaction())
            {
                var table = GetMetadataTableByType(type, tx);
                Index index;
                IndexTransformerMetadata metadata;
                Slice key;
                using (Slice.External(context.Allocator, (byte*)&id, sizeof(int), out key))
                {
                    var result = table.ReadByKey(key);

                    //TODO : add logging here
                    if (result == null) //no metadata, nothing to write tombstone for...
                        return default(IndexTransformerTombstone);

                    metadata = TableValueToMetadata(result);
                    index = _documentDatabase.IndexStore.GetIndex(id);
                    if (index == null) //TODO : add logging here
                        return default(IndexTransformerTombstone);
                }

                byte* name;
                byte* lowerCaseName;
                int size;
                GetLowerKeySlice(context, index.Name, out name, out lowerCaseName, out size);

                fixed (ChangeVectorEntry* pChangeVector = metadata.ChangeVector)
                {
                    var lastEtag = ReadLastEtag(tx);

                    var globalChangeVectorTree = tx.ReadTree(GlobalChangeVectorTreeName);
                    var globalChangeVector = ReplicationUtils.ReadChangeVectorFrom(globalChangeVectorTree);

                    var newEtag = lastEtag + 1;

                    var tombstone = new IndexTransformerTombstone
                    {
                        Name = new LazyStringValue(null, name, size, context),
                        LoweredName = new LazyStringValue(null, lowerCaseName, size, context),
                        Type = type,
                        ChangeVector = ReplicationUtils.UpdateChangeVectorWithNewEtag(Environment.DbId, newEtag, globalChangeVector),
                        DeletedEtag = metadata.Etag,
                        Etag = newEtag
                    };

                    var etag = Bits.SwapBytes(tombstone.Etag);
                    var deletedEtag = Bits.SwapBytes(tombstone.DeletedEtag);                    

                    tx.OpenTable(_tombstoneSchema, TombstonesTableName)
                      .Set(new TableValueBuilder
                        {
                          {tombstone.Name.Buffer, tombstone.Name.Size},
                          {tombstone.LoweredName.Buffer, tombstone.LoweredName.Size},
                          (int)tombstone.Type,
                          etag,
                          {(byte*)pChangeVector, sizeof(ChangeVectorEntry)*metadata.ChangeVector.Length},
                          deletedEtag,
                        });

                    tx.Commit();
                    return tombstone;
                }                
            }
        }

        //note: get last etag from indexes, transformers and tombstones, since they share etags
        public long ReadLastEtag(Transaction tx)
        {
            
            var lastIndexEtag = ReadLastIndexEtag(tx);
            var lastTransformerEtag = ReadLastTransformerEtag(tx);
            var lastTombstoneEtag = ReadLastTombstoneEtag(tx);
            var lastEtag = Math.Max(lastIndexEtag, Math.Max(lastTombstoneEtag, lastTransformerEtag));
            return lastEtag;
        }

        public unsafe IndexTransformerMetadata WriteNewMetadataFor(int id, MetadataStorageType type)
        {
            JsonOperationContext context;
            var metadata = new IndexTransformerMetadata();
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = Environment.WriteTransaction())
            {
                var metadataTable = GetMetadataTableByType(type, tx);
                TableValueReader result;
                Slice key;
                using (Slice.From(context.Allocator, (byte*) &id, sizeof(int), out key))
                {                    
                    if(metadataTable.Schema.Key.Name.ToString().Contains("Last"))
                        Debugger.Break();
                    result = metadataTable.ReadByKey(key);
                }

                if (result != null)
                    return TableValueToMetadata(result);

                var lastEtag = ReadLastEtag(tx); 

                var globalChangeVectorTree = tx.ReadTree(GlobalChangeVectorTreeName);
                var globalChangeVector = ReplicationUtils.ReadChangeVectorFrom(globalChangeVectorTree);

                metadata.Etag = lastEtag + 1;
                metadata.ChangeVector = ReplicationUtils.UpdateChangeVectorWithNewEtag(
                    Environment.DbId, metadata.Etag, globalChangeVector);
                metadata.Id = id;

                WriteMetadataInternal(metadata,tx,type);

                var newGlobalChangeVector = ReplicationUtils.MergeVectors(globalChangeVector, metadata.ChangeVector);
                ReplicationUtils.WriteChangeVectorTo(context, newGlobalChangeVector, globalChangeVectorTree);

                tx.Commit();
            }

            return metadata;
        }


        public static unsafe void GetLowerKeySlice(
            JsonOperationContext context, 
            string str, 
            out byte* key,
            out byte* lowerKey,
            out int size)
        {
            var byteCount = Encoding.UTF8.GetMaxByteCount(str.Length);
            var buffer = context.GetNativeTempBuffer(byteCount * 2);

            fixed (char* pChars = str)
            {
                var lowerCaseChars = (char*)buffer;
                for (var i = 0; i < str.Length; i++)
                {
                    lowerCaseChars[i] = char.ToLowerInvariant(pChars[i]);
                }
                lowerKey = (byte*)lowerCaseChars;
                size = Encoding.UTF8.GetBytes(lowerCaseChars, str.Length, lowerKey, byteCount);

                var originalCaseChars = (char*) (buffer + size);
                for (var i = 0; i < str.Length; i++)
                {
                    originalCaseChars[i] = pChars[i];
                }
                key = (byte*) originalCaseChars;
                Encoding.UTF8.GetBytes(originalCaseChars, str.Length, key, byteCount);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Table GetMetadataTableByType(MetadataStorageType storageType, Transaction tx)
        {
            Table metadataTable;
            switch (storageType)
            {
                case MetadataStorageType.Index:
                    metadataTable = tx.OpenTable(_metadataSchema, IndexMetadataTableName);
                    break;
                case MetadataStorageType.Transformer:
                    metadataTable = tx.OpenTable(_metadataSchema, TransformerMetadataTableName);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(storageType), storageType, null);
            }
            return metadataTable;
        }

        public IndexTransformerMetadata ReadMetadata(int id,MetadataStorageType storageType)
        {            
            JsonOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = Environment.ReadTransaction())
                return ReadMetadataInternal(id, tx, context, storageType);
        }

        private unsafe IndexTransformerMetadata ReadMetadataInternal(int indexId, Transaction tx, JsonOperationContext context, MetadataStorageType storageType)
        {
            var metadataTable = GetMetadataTableByType(storageType, tx);

            Slice key;
            TableValueReader tvr;
            using (Slice.External(context.Allocator, (byte*)&indexId, sizeof(int), out key))
            {
                tvr = metadataTable.ReadByKey(key);
            }

            return TableValueToMetadata(tvr);
        }

        public void WriteMetadata(IndexTransformerMetadata metadata, MetadataStorageType storageType)
        {
            JsonOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = Environment.WriteTransaction())
            {
                WriteMetadataInternal(metadata, tx, storageType);
                IndexTransformerMetadata tempMetadata = metadata;
                var globalChangeVectorTree = tx.ReadTree(GlobalChangeVectorTreeName);
                var newGlobalChangeVector = ReplicationUtils.MergeVectors(
                    ReplicationUtils.ReadChangeVectorFrom(globalChangeVectorTree),
                    tempMetadata.ChangeVector);
                ReplicationUtils.WriteChangeVectorTo(context, newGlobalChangeVector, globalChangeVectorTree);

                tx.Commit();
            }
        }

        private unsafe void WriteMetadataInternal(IndexTransformerMetadata metadata, Transaction tx, MetadataStorageType storageType)
        {
            if (tx == null) throw new ArgumentNullException(nameof(tx));

            //precautions
            if (metadata.Id <= 0) throw new ArgumentException("metadata.IndexId should be bigger than 0");
            if (metadata.ChangeVector == null) throw new ArgumentException("metadata.ChangeVector == null, should not be so");

            var metadataTable = GetMetadataTableByType(storageType, tx);

            fixed (ChangeVectorEntry* pChangeVector = metadata.ChangeVector)
            {
                var etag = Bits.SwapBytes(metadata.Etag);
                metadataTable.Set(new TableValueBuilder
                {
                    {(byte*) &metadata.Id, sizeof(int)},
                    {(byte*) pChangeVector, sizeof(ChangeVectorEntry)*metadata.ChangeVector.Length},
                    {(byte*) &etag, sizeof(long)}
                });              
            }
        }

        public unsafe void DeleteMetadata(int id,MetadataStorageType storageType)
        {
            JsonOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = Environment.WriteTransaction())
            {
                var metadataTable = GetMetadataTableByType(storageType, tx);
                Slice key;
                using (Slice.External(context.Allocator, (byte*) &id, sizeof(int), out key))
                    metadataTable.DeleteByKey(key);

                tx.Commit();
            }
        }

        public IEnumerable<IndexTransformerTombstone> GetTombstonesAfter(long etag, MetadataStorageType storageType, int start = 0, int take = 1024)
        {
            JsonOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = Environment.ReadTransaction())
            {
                var table = tx.OpenTable(_tombstoneSchema, TombstonesTableName);
                foreach (var tvr in table.SeekForwardFrom(
                    _tombstoneSchema.FixedSizeIndexes[TombstoneEtagIndexName], etag))
                {
                    if (start > 0)
                    {
                        start--;
                        continue;
                    }
                    if (take-- <= 0)
                    {
                        yield break;
                    }

                    yield return TableValueToTombstone(tvr, context);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe ByteStringContext<ByteStringMemoryCache>.ExternalScope GetSliceFromEtag(long etag, JsonOperationContext context, out Slice etagSlice)
        {
            return Slice.External(context.Allocator,(byte*)&etag,sizeof(long),out etagSlice);
        }


        public IEnumerable<IndexTransformerMetadata> GetMetadataAfter(long etag, MetadataStorageType storageType, int start = 0, int take = 1024)
        {
            JsonOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = Environment.ReadTransaction())
            {
                foreach (var indexTransformerMetadata in GetMetadataAfter(etag, storageType, tx, start, take))
                    yield return indexTransformerMetadata;
            }
        }

        public IEnumerable<IndexTransformerMetadata> GetMetadataAfter(long etag, MetadataStorageType storageType, Transaction tx,int start = 0, int take = 1024)
        {
            var metadataTable = GetMetadataTableByType(storageType, tx);
            foreach (var tvr in metadataTable.SeekForwardFrom(
                _metadataSchema.
                    FixedSizeIndexes[MetadataEtagIndexName], etag))
            {
                if (start > 0)
                {
                    start--;
                    continue;
                }
                if (take-- <= 0)
                {
                    yield break;
                }

                yield return TableValueToMetadata(tvr);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long ReadLastTombstoneEtag(Transaction tx)
        {
            return ReadLastMetadataEtag(tx.OpenTable(_tombstoneSchema, TombstonesTableName), TombstoneEtagIndexName, (int)TombstoneFields.Etag);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long ReadLastIndexEtag(Transaction tx)
        {
            return ReadLastMetadataEtag(tx.OpenTable(_metadataSchema, IndexMetadataTableName), MetadataEtagIndexName, (int)MetadataFields.Etag);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long ReadLastTransformerEtag(Transaction tx)
        {
            return ReadLastMetadataEtag(tx.OpenTable(_metadataSchema, TransformerMetadataTableName), MetadataEtagIndexName, (int)MetadataFields.Etag);
        }       

        private unsafe long ReadLastMetadataEtag(Table metadataSchemaTable,Slice indexName, int fieldIndex)
        {
            if (metadataSchemaTable.NumberOfEntries == 0)
                return 0;
            
            var result = metadataSchemaTable.SeekBackwardFrom(metadataSchemaTable.Schema.FixedSizeIndexes[indexName], long.MaxValue);
            var tvr = result.FirstOrDefault();
            if(tvr == null) //precaution
                throw new InvalidDataException("Tried to fetch last index etag and found no entries, despite NumberOfEntries > 0. This is not supposed to happen and is likely a bug.");

            int size;
            return IPAddress.NetworkToHostOrder(*(long*) tvr.Read(fieldIndex, out size));
        }

        public void Dispose()
        {           
        }

        private unsafe IndexTransformerMetadata TableValueToMetadata(TableValueReader tvr)
        {
            var metadata = new IndexTransformerMetadata();

            int size;
            metadata.Id = *(int*) tvr.Read((int) MetadataFields.Id, out size);
            metadata.ChangeVector = StorageUtil.GetChangeVectorEntriesFromTableValueReader(tvr,(int) MetadataFields.ChangeVector);
            metadata.Etag = IPAddress.NetworkToHostOrder(*(long*)tvr.Read((int)MetadataFields.Etag, out size)); 

            return metadata;
        }

        private unsafe IndexTransformerTombstone TableValueToTombstone(TableValueReader tvr,JsonOperationContext context)
        {
            var tombstone = new IndexTransformerTombstone();
            int size;
            var ptr = tvr.Read((int)TombstoneFields.Name, out size);
            tombstone.Name = new LazyStringValue(null, ptr, size, context);

            ptr = tvr.Read((int)TombstoneFields.LoweredName, out size);
            tombstone.LoweredName = new LazyStringValue(null, ptr, size, context);
            tombstone.Etag = IPAddress.NetworkToHostOrder(*(long*)tvr.Read((int)TombstoneFields.Etag, out size));
            tombstone.DeletedEtag = IPAddress.NetworkToHostOrder(*(long*)tvr.Read((int)TombstoneFields.DeletedEtag, out size));
            tombstone.Type = StorageUtil.GetEnumFromTableValueReader<MetadataStorageType>(tvr,(int) TombstoneFields.StorageType);
            tombstone.ChangeVector = StorageUtil.GetChangeVectorEntriesFromTableValueReader(tvr, (int)TombstoneFields.ChangeVector);

            return tombstone;
        }

        private enum MetadataFields
        {
            Id = 0,
            ChangeVector = 1,
            Etag = 2
        }

        private enum TombstoneFields
        {
            Name = 0,
            LoweredName = 1,
            StorageType = 2,
            Etag = 3,
            ChangeVector = 4,
            DeletedEtag = 5
        }
    }

    public enum MetadataStorageType
    {
        Index,
        Transformer
    }
}
