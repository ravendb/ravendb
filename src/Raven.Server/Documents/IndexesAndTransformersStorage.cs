using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Replication.Messages;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
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
            return WriteEntry(index.Name, index.IndexId);
        }

        public long OnIndexDeleted(Index index)
        {
            return WriteEntry(index.Name, -1);
        }

        private long WriteEntry(string indexName, int indexIndexId)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = _environment.WriteTransaction())
            {
                var table = tx.OpenTable(IndexesTableSchema, SchemaNameConstants.IndexMetadataTable);
                var existing = GetIndexMetadataByNameInternal(tx, context, indexName);
                var lastEtag = ReadLastEtag(table);
                var newEtag = lastEtag + 1;

                var changeVectorForWrite = ReplicationUtil.GetChangeVectorForWrite(existing?.ChangeVector, _environment.DbId, newEtag);

                Slice indexNameAsSlice;
                using (DocumentsStorage.GetSliceFromKey(context, indexName, out indexNameAsSlice))
                {
                    //precautions
                    if (newEtag < 0) throw new ArgumentException("etag must not be negative");
                    if (changeVectorForWrite == null) throw new ArgumentException("changeVector == null, should not be so");

                    fixed (ChangeVectorEntry* pChangeVector = changeVectorForWrite)
                    {
                        var bitSwappedEtag = Bits.SwapBytes(newEtag);

                        var bitSwappedId = Bits.SwapBytes(indexIndexId);

                        table.Set(new TableValueBuilder
                        {
                            {(byte*) &bitSwappedId, sizeof(int)},
                            {(byte*) &bitSwappedEtag, sizeof(long)},
                            indexNameAsSlice,
                            {(byte*) pChangeVector, sizeof(ChangeVectorEntry)*changeVectorForWrite.Length},
                        });
                    }
                }

                MergeEntryVectorWithGlobal(tx, context, changeVectorForWrite);

                tx.Commit();
                return newEtag;
            }
        }

        public IEnumerable<IndexEntryMetadata> GetTombstonesFrom(long etag, int start, int take)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = _environment.WriteTransaction())
            {
                int taken = 0;
                int skipped = 0;
                var table = tx.OpenTable(IndexesTableSchema, SchemaNameConstants.IndexMetadataTable);

                var buffer = stackalloc byte[sizeof(int) + sizeof(long)];

                *(int*) buffer = Bits.SwapBytes(-1);
                *(long*) (buffer + sizeof(int)) = Bits.SwapBytes(etag);

                var list = new List<IndexEntryMetadata>();
                Slice slice;
                using (Slice.External(tx.Allocator, buffer, sizeof(int) + sizeof(long), out slice))
                {
                    foreach (var seekResult in table.SeekForwardFrom(IndexesTableSchema.Indexes[MetadataIdAndEtagIndexName], slice))
                    {
                        foreach (var tvr in seekResult.Results)
                        {
                            if (start > skipped)
                            {
                                skipped++;
                                continue;
                            }

                            if (taken++ >= take)
                                break;

                            list.Add(TableValueToMetadata(tvr, context));
                        }
                    }
                    return list;
                }
            }
        }

        private void MergeEntryVectorWithGlobal(Transaction tx,
            TransactionOperationContext context,
            ChangeVectorEntry[] changeVectorForWrite)
        {
            var globalChangeVectorTree = tx.ReadTree(SchemaNameConstants.GlobalChangeVectorTree);
            var globalChangeVector = ReplicationUtil.ReadChangeVectorFrom(globalChangeVectorTree);

            // merge metadata change vector into global change vector
            // --> if we have any entry in global vector smaller than in metadata vector,
            // update the entry in global vector to a larger one
            foreach (var item in ReplicationUtils.MergeVectors(globalChangeVector, changeVectorForWrite))
            {
                var dbId = item.DbId;
                var etagBigEndian = Bits.SwapBytes(item.Etag);
                Slice key;
                Slice value;
                using (Slice.External(context.Allocator, (byte*)&dbId, sizeof(Guid), out key))
                using (Slice.External(context.Allocator, (byte*)&etagBigEndian, sizeof(long), out value))
                    globalChangeVectorTree.Add(key, value);
            }
        }

        public IndexEntryMetadata GetIndexMetadataByName(string name)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = _environment.ReadTransaction())
                return GetIndexMetadataByNameInternal(tx, context, name);
        }

        private IndexEntryMetadata GetIndexMetadataByNameInternal(Transaction tx, TransactionOperationContext context, string name)
        {
            var table = tx.OpenTable(IndexesTableSchema, SchemaNameConstants.IndexMetadataTable);

            Slice nameAsSlice;
            TableValueReader tvr;
            using (DocumentsStorage.GetSliceFromKey(context, name, out nameAsSlice))
                tvr = table.ReadByKey(nameAsSlice);

            if (tvr == null)
                return null;

            return TableValueToMetadata(tvr, context);
        }

        private IndexEntryMetadata TableValueToMetadata(TableValueReader tvr, JsonOperationContext context)
        {
            var metadata = new IndexEntryMetadata();

            int size;
            metadata.Id = Bits.SwapBytes(*(int*)tvr.Read((int)MetadataFields.Id, out size));
            metadata.Name = new LazyStringValue(null, tvr.Read((int)MetadataFields.Name, out size), size, context);
            metadata.ChangeVector = ReplicationUtil.GetChangeVectorEntriesFromTableValueReader(tvr, (int)MetadataFields.ChangeVector);
            metadata.Etag = Bits.SwapBytes(*(long*)tvr.Read((int)MetadataFields.Etag, out size));

            return metadata;
        }


        //since both transformers and indexes need to store the same information,
        //this can be used for both

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
            return Bits.SwapBytes(*(long*)tvr.Read((int)MetadataFields.Etag, out size));
        }

        public class IndexEntryMetadata
        {
            public int Id;
            public LazyStringValue Name; //PK
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