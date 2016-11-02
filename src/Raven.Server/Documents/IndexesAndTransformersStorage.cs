using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
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

        private readonly TableSchema _indexesSchema = new TableSchema();

        public IndexesAndTransformersStorage(string resourceName)
        {
            Logger = LoggingSource.Instance.GetLogger<IndexesAndTransformersStorage>(resourceName);
            _indexesSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1
            });

            //not sure if this needs to be disposed
            Slice etagsIndexName;
            Slice.From(StorageEnvironment.LabelsContext, "EtagIndexName", out etagsIndexName);

            _indexesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = 1,
                Name = etagsIndexName
            });

        }

        public void Initialize(StorageEnvironment environment, TransactionContextPool contextPool)
        {
            _environment = environment;
            _contextPool = contextPool;

            TransactionOperationContext context;
            using (contextPool.AllocateOperationContext(out context))
            using (var tx = _environment.WriteTransaction(context.PersistentContext))
            {
                _indexesSchema.Create(tx, IndexesSchema.IndexesTree);
                tx.CreateTree(IndexesSchema.GlobalChangeVectorTree);

                tx.Commit();
            }
        }

        public void OnIndexCreated(Index index)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = _environment.WriteTransaction())
            {
                var table = tx.OpenTable(_indexesSchema, IndexesSchema.IndexesTree);
               
                var lastEtag = ReadLastEtag(table);

                var globalChangeVectorTree = tx.ReadTree(IndexesSchema.GlobalChangeVectorTree);
                var globalChangeVector = ReplicationUtil.ReadChangeVectorFrom(globalChangeVectorTree);

                var newEtag = lastEtag + 1;
                var changeVector = ReplicationUtil.UpdateChangeVectorWithNewEtag(_environment.DbId, newEtag, globalChangeVector);

                WriteMetadataInternal(table, index.IndexId, newEtag, changeVector);

                var newGlobalChangeVector = ReplicationUtil.MergeVectors(globalChangeVector, changeVector);
                ReplicationUtil.WriteChangeVectorTo(context, newGlobalChangeVector, globalChangeVectorTree);
            }
        }

        public void OnIndexDeleted(Index index)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = _environment.WriteTransaction())
            {
                var table = tx.OpenTable(_indexesSchema, IndexesSchema.IndexesTree);

                Slice indexIdAsSlice;
                var indexId = index.IndexId;
                Metadata indexMetadata;
                using (Slice.External(context.Allocator, (byte*) &indexId, sizeof(int), out indexIdAsSlice))
                {
                    var tvr = table.ReadByKey(indexIdAsSlice);
                    indexMetadata = TableValueToMetadata(tvr);
                    table.DeleteByKey(indexIdAsSlice);
                }

                WriteTombstoneInternal(indexMetadata.Id,indexMetadata.Etag);
            }
        }

        private void WriteTombstoneInternal(int id, long etag)
        {

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

        //since both transformers and indexes need to store the same information,
        //this can be used for both
        private void WriteMetadataInternal(Table table, int id, long etag, ChangeVectorEntry[] changeVector)
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
                    {(byte*) pChangeVector, sizeof(ChangeVectorEntry)*changeVector.Length},
                });
            }
        }

        private long ReadLastEtag(Table table)
        {
            //assuming an active transaction

            if (table.NumberOfEntries == 0)
                return 0;

            //the primary key will be always etag because it is unique per record
            var result = table.SeekLastByPrimaryKey();
            if (result == null)
                return 0;

            int size;
            return IPAddress.NetworkToHostOrder(*(long*) result.Read((int) MetadataFields.Etag, out size));
        }

        public struct Metadata
        {
            public int Id;
            public long Etag;
            public ChangeVectorEntry[] ChangeVector;
        }

        public struct Tombstone
        {
            public int Id;
            public long Etag;
            public long DeletedEtag;
        }

        public enum TombstoneFields
        {
            Id = 1,
            Etag = 2,
            DeletedEtag = 3
        }


        public enum MetadataFields
        {
            Id = 1,
            Etag = 2,
            ChangeVector = 3
        }

        public static class IndexesSchema
        {
            public const string IndexesTree = "Indexes";
            public const string GlobalChangeVectorTree = "GlobalChangeVectorTree";
        }

        
    }
}