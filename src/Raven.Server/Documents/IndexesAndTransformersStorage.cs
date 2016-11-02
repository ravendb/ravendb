using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using Raven.Client.Replication.Messages;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents
{
    public unsafe class IndexesAndTransformersStorage
    {
        protected readonly Logger Logger;

        private StorageEnvironment _environment;

        private TransactionContextPool _contextPool;

        private static readonly TableSchema IndexesTableSchema;
        private static readonly Slice EtagIndexName;
        private static readonly Slice MetadataIdAndEtagIndexName;

        static IndexesAndTransformersStorage()
        {
            Slice.From(StorageEnvironment.LabelsContext, "EtagIndexName", out EtagIndexName);
            Slice.From(StorageEnvironment.LabelsContext, "MetadataIdAndEtagIndexName", out MetadataIdAndEtagIndexName);

            // table
            // index name, index id (-1 if tombstone), etag, change vector

            // Table schema is:
            //  - index id - int
            //  - etag - long
            //  - index name - string, lowercase
            //  - change vector
            IndexesTableSchema = new TableSchema();

            IndexesTableSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)MetadataFields.Name,
                Count = 1
            });
            
            IndexesTableSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)MetadataFields.Etag,
                Name = EtagIndexName
            });


            //index that encompasses two fields --> id and etag
            //this is useful to be able to seek over tombstones --> by doing SeekForwardStartingWith("-1")
            IndexesTableSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)MetadataFields.Id,
                Count = 2,
                Name = MetadataIdAndEtagIndexName
            });
        }

        public IndexesAndTransformersStorage(string resourceName)
        {
            Logger = LoggingSource.Instance.GetLogger<IndexesAndTransformersStorage>(resourceName);
        }

        public void Initialize(StorageEnvironment environment, TransactionContextPool contextPool)
        {
            _environment = environment;
            _contextPool = contextPool;

            TransactionOperationContext context;
            using (contextPool.AllocateOperationContext(out context))
            using (var tx = _environment.WriteTransaction(context.PersistentContext))
            {
                IndexesTableSchema.Create(tx, SchemaNameConstants.IndexMetadataTable);
                tx.CreateTree(SchemaNameConstants.GlobalChangeVectorTree);

                tx.Commit();
            }
        }

        public long OnIndexCreated(Index index)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = _environment.WriteTransaction())
            {
                var table = tx.OpenTable(IndexesTableSchema, SchemaNameConstants.IndexMetadataTable);
                var existing = GetIndexMetadataByNameInternal(tx, context, index.Name);
                var lastEtag = ReadLastEtag(table);
                var newEtag = lastEtag + 1;

                var changeVectorForWrite = GetChangeVectorForWrite(existing?.ChangeVector,newEtag);

                Slice indexNameAsSlice;
                using (GetLowerCaseSliceFrom(context, index.Name, out indexNameAsSlice))
                    WriteMetadataInternal(table, index.IndexId, indexNameAsSlice, newEtag, changeVectorForWrite);

                MergeEntryVectorWithGlobal(tx, context, changeVectorForWrite);

                tx.Commit();
                return newEtag;                
            }
        }

        public long OnIndexDeleted(Index index)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = _environment.WriteTransaction())
            {
                var table = tx.OpenTable(IndexesTableSchema, SchemaNameConstants.IndexMetadataTable);
                var existing = GetIndexMetadataByNameInternal(tx, context, index.Name);
                var lastEtag = ReadLastEtag(table);
                var newEtag = lastEtag + 1;

                //note : id == -1 in metadata means it is a tombstone
                Slice indexNameAsSlice;
                using (GetLowerCaseSliceFrom(context, index.Name, out indexNameAsSlice))
                {
                    var changeVectorForWrite = GetChangeVectorForWrite(existing?.ChangeVector, newEtag);
                    WriteMetadataInternal(table, -1, indexNameAsSlice, newEtag, changeVectorForWrite);
                }

                tx.Commit();
                return newEtag;
            }
        }
        
        public IEnumerable<Metadata> GetTombstonesFrom(long etag,int start,int take)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = _environment.WriteTransaction())
            {                
                int taken = 0;
                int skipped = 0;
                var table = tx.OpenTable(IndexesTableSchema, SchemaNameConstants.IndexMetadataTable);

                Slice slice;
                using (GetStartingSliceForTombstonesSeek(context, etag, out slice))
                {
                    foreach (var seekResult in table.SeekForwardFrom(IndexesTableSchema.Indexes[MetadataIdAndEtagIndexName], slice))
                    {
                        foreach (var tvr in seekResult.Results)
                        {
                            if (skipped++ >= start)
                            {
                                if (taken++ >= take)
                                    break;

                                yield return TableValueToMetadata(tvr);
                            }
                        }
                    }
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ByteStringContext.ExternalScope GetStartingSliceForTombstonesSeek(
            TransactionOperationContext context,long etag, out Slice slice)
        {
            const int PrefixSize = sizeof(int) + sizeof(long);
            var buffer = context.GetNativeTempBuffer(PrefixSize);
            *(int*) buffer = -1; //if IndexId/TransformerId == -1 is a tombstone
            *(long*) (buffer + sizeof(int)) = etag;

            return Slice.External(context.Allocator, buffer, PrefixSize, out slice);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void MergeEntryVectorWithGlobal(Transaction tx, 
            TransactionOperationContext context, 
            ChangeVectorEntry[] changeVectorForWrite)
        {
            var globalChangeVectorTree = tx.ReadTree(SchemaNameConstants.GlobalChangeVectorTree);
            var globalChangeVector = ReplicationUtil.ReadChangeVectorFrom(globalChangeVectorTree);

            // merge metadata change vector into global change vector
            // --> if we have any entry in global vector smaller than in metadata vector,
            // update the entry in global vector to a larger one
            ReplicationUtil.WriteChangeVectorTo(context, 
                ReplicationUtil.MergeVectors(globalChangeVector, changeVectorForWrite), 
                    globalChangeVectorTree);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ChangeVectorEntry[] GetChangeVectorForWrite(ChangeVectorEntry[] existingChangeVector, long etag)
        {            
            if (existingChangeVector == null || existingChangeVector.Length == 0)
            {
                return new[]
                {
                    new ChangeVectorEntry
                    {
                        DbId = _environment.DbId,
                        Etag = etag
                    }
                };
            }

            var changeVectorWithNewEtag = ReplicationUtil.UpdateChangeVectorWithNewEtag(_environment.DbId, etag, existingChangeVector);            
            return changeVectorWithNewEtag;            
        }

        public Metadata GetIndexMetadataByName(string name)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = _environment.ReadTransaction())
                return GetIndexMetadataByNameInternal(tx, context, name);
        }

        private Metadata GetIndexMetadataByNameInternal(Transaction tx, TransactionOperationContext context, string name)
        {
            var table = tx.OpenTable(IndexesTableSchema, SchemaNameConstants.IndexMetadataTable);

            Slice nameAsSlice;
            TableValueReader tvr;
            using (GetLowerCaseSliceFrom(context, name, out nameAsSlice))
                tvr = table.ReadByKey(nameAsSlice);

            if (tvr == null)
                return null;

            return TableValueToMetadata(tvr);
        }

        private Metadata TableValueToMetadata(TableValueReader tvr)
        {
            var metadata = new Metadata();

            int size;
            metadata.Id = IPAddress.NetworkToHostOrder(*(int*)tvr.Read((int)MetadataFields.Id, out size));
            metadata.ChangeVector = ReplicationUtil.GetChangeVectorEntriesFromTableValueReader(tvr, (int)MetadataFields.ChangeVector);
            metadata.Etag = IPAddress.NetworkToHostOrder(*(long*)tvr.Read((int)MetadataFields.Etag, out size));

            return metadata;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ByteStringContext.ExternalScope GetLowerCaseSliceFrom(TransactionOperationContext context, string str, out Slice slice)
        {
            int size;
            byte* lowerCaseIndexNameBytes;
            ReplicationUtil.GetLowerCaseStringBytes(context, str, out lowerCaseIndexNameBytes, out size);

            return Slice.External(context.Allocator, lowerCaseIndexNameBytes, size, ByteStringType.Immutable, out slice);
        }


        //since both transformers and indexes need to store the same information,
        //this can be used for both
        private void WriteMetadataInternal(Table table, int id, Slice indexName, long etag, ChangeVectorEntry[] changeVector)
        {
            //precautions
            if (etag < 0) throw new ArgumentException("etag must not be negative");
            if (changeVector == null) throw new ArgumentException("changeVector == null, should not be so");

            fixed (ChangeVectorEntry* pChangeVector = changeVector)
            {
                var bitSwappedEtag = Bits.SwapBytes(etag);
                var bitSwappedId = Bits.SwapBytes(id);

                table.Set(new TableValueBuilder
                {
                    {(byte*) &bitSwappedId, sizeof(int)},
                    {(byte*) &bitSwappedEtag, sizeof(long)},
                    indexName,
                    {(byte*) pChangeVector, sizeof(ChangeVectorEntry)*changeVector.Length},
                });
            }
        }

        private long ReadLastEtag(Table table)
        {
            //assuming an active transaction
            if (table.NumberOfEntries == 0)
                return 0;

            var result = table.SeekBackwardFrom(IndexesTableSchema.FixedSizeIndexes[EtagIndexName], long.MaxValue);
            var tvr = result.FirstOrDefault();
            if (tvr == null)
                return 0;
            
            int size;
            return Bits.SwapBytes(*(long*)tvr.Read((int) MetadataFields.Etag, out size));
        }

        public class Metadata
        {
            public int Id;
            public string Name; //PK
            public long Etag;
            public ChangeVectorEntry[] ChangeVector;
        }             

        public enum MetadataFields
        {
            Id = 0,
            Etag = 1,
            Name = 2,
            ChangeVector = 3
        }

        public static class SchemaNameConstants
        {
            public const string IndexMetadataTable = "Indexes";
            public const string GlobalChangeVectorTree = "GlobalChangeVectorTree";
        }

        
    }
}