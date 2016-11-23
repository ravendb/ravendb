using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Raven.Abstractions.Exceptions;
using Raven.Client.Replication.Messages;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Transformers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents
{
    public unsafe class IndexesEtagsStorage
    {
        protected readonly Logger Logger;

        private StorageEnvironment _environment;

        private DocumentsContextPool _contextPool;

        private static readonly TableSchema IndexesTableSchema;
        private static readonly Slice EtagIndexName;

        public bool IsInitialized { get; private set; }

        static IndexesEtagsStorage()
        {
            Slice.From(StorageEnvironment.LabelsContext, "EtagIndexName", out EtagIndexName);

            // Table schema is:
            //  - index id - int (-1 if tombstone)
            //  - etag - long
            //  - name - string, lowercase
            //  - type - enum (index / transformer)
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
        }

        public IndexesEtagsStorage(string resourceName)
        {
            Logger = LoggingSource.Instance.GetLogger<IndexesEtagsStorage>(resourceName);
        }

        public DocumentsContextPool ContextPool => _contextPool;

        public void Initialize(StorageEnvironment environment, DocumentsContextPool contextPool, IndexStore indexStore, TransformerStore transformerStore)
        {
            _environment = environment;
            _contextPool = contextPool;

            DocumentsOperationContext context;
            using (contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                IndexesTableSchema.Create(tx.InnerTransaction, SchemaNameConstants.IndexMetadataTable);
                tx.InnerTransaction.CreateTree(SchemaNameConstants.GlobalChangeVectorTree);
                tx.InnerTransaction.CreateTree(SchemaNameConstants.LastReplicatedEtagsTree);
                tx.Commit();
            }

            using (contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                DeleteIndexMetadataForRemovedIndexesAndTransformers(tx.InnerTransaction, context, indexStore, transformerStore);
                tx.Commit();
            }

            IsInitialized = true;
        }

        public long OnIndexCreated(Index index)
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var newEtag = WriteEntry(tx.InnerTransaction, index.Name, IndexEntryType.Index, index.IndexId, context);
                var etag = newEtag;
                tx.Commit();
                return etag;
            }
        }

        public long OnIndexDeleted(Index index)
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var newEtag = WriteEntry(context.Transaction.InnerTransaction, index.Name, IndexEntryType.Index, -1, context);
                var etag = newEtag;
                tx.Commit();
                return etag;
            }
        }

        public long OnTransformerCreated(Transformer transformer)
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var newEtag = WriteEntry(context.Transaction.InnerTransaction, transformer.Name, IndexEntryType.Transformer, transformer.TransformerId, context);
                var etag = newEtag;
                tx.Commit();
                return etag;
            }
        }

        public long OnTransformerDeleted(Transformer transformer)
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var newEtag = WriteEntry(context.Transaction.InnerTransaction, transformer.Name, IndexEntryType.Transformer, -1, context);
                var etag = newEtag;
                tx.Commit();
                return etag;
            }
        }

        private long WriteEntry(Transaction tx, string indexName, IndexEntryType type, int indexIndexId,
            DocumentsOperationContext context)
        {
            var table = tx.OpenTable(IndexesTableSchema, SchemaNameConstants.IndexMetadataTable);
            var existing = GetIndexMetadataByName(tx, context, indexName, false);
            var lastEtag = ReadLastEtag(table);
            var newEtag = lastEtag + 1;

            var changeVectorForWrite = ReplicationUtils.GetChangeVectorForWrite(existing?.ChangeVector, _environment.DbId,
                newEtag);

            //precautions
            if (newEtag < 0) throw new ArgumentException("etag must not be negative");
            if (changeVectorForWrite == null) throw new ArgumentException("changeVector == null, should not be so");

            Slice indexNameAsSlice;
                using (DocumentKeyWorker.GetSliceFromKey(context, indexName, out indexNameAsSlice))
            {
                ThrowIfAlreadyExistsAndOverwriting(indexName, type, indexIndexId, table, indexNameAsSlice, existing);

                fixed (ChangeVectorEntry* pChangeVector = changeVectorForWrite)
                {
                    var bitSwappedEtag = Bits.SwapBytes(newEtag);

                    var bitSwappedId = Bits.SwapBytes(indexIndexId);

                    table.Set(new TableValueBuilder
                        {
                            {(byte*) &bitSwappedId, sizeof(int)},
                            {(byte*) &bitSwappedEtag, sizeof(long)},
                            indexNameAsSlice,
                        {(byte*) &type, 1},
                            {(byte*) pChangeVector, sizeof(ChangeVectorEntry)*changeVectorForWrite.Length},
                        });
                }
            }

            MergeEntryVectorWithGlobal(tx, context.Allocator, changeVectorForWrite);
            return newEtag;
        }

        public void PurgeTombstonesFrom(Transaction tx, JsonOperationContext context, long etag, int take)
        {
            int taken = 0;
            var table = tx.OpenTable(IndexesTableSchema, SchemaNameConstants.IndexMetadataTable);
            var idsToDelete = new List<long>();
            foreach (var tvr in table.SeekForwardFrom(IndexesTableSchema.FixedSizeIndexes[EtagIndexName], etag))
            {
                if (taken++ >= take)
                    break;
                var metadata = TableValueToMetadata(tvr, context, false);
                if (metadata.Id == -1)
                    idsToDelete.Add(tvr.Id);
            }

            foreach (var id in idsToDelete)
                table.Delete(id);

            if (Logger.IsInfoEnabled)
            {
                Logger.Info($"Purged index/metadata tombstones from etag = {etag}, total purged {taken}");
            }

        }

        /// <summary>
        /// this method will fetch all metadata entries - tombstones or otherwise
        /// </summary>
        public List<IndexEntryMetadata> GetAfter(Transaction tx, JsonOperationContext context, long etag, int start, int take)
        {
            int taken = 0;
            int skipped = 0;
            var table = tx.OpenTable(IndexesTableSchema, SchemaNameConstants.IndexMetadataTable);

            var results = new List<IndexEntryMetadata>();
            foreach (var tvr in table.SeekForwardFrom(IndexesTableSchema.FixedSizeIndexes[EtagIndexName], etag))
            {
                if (start > skipped)
                {
                    skipped++;
                    continue;
                }

                if (taken++ >= take)
                    break;

                results.Add(TableValueToMetadata(tvr, context, false));
            }

            return results;
        }

        public ChangeVectorEntry[] GetIndexesAndTransformersChangeVector(Transaction tx)
        {
            var globalChangeVectorTree = tx.ReadTree(SchemaNameConstants.GlobalChangeVectorTree);
            Debug.Assert(globalChangeVectorTree != null);
            return ReplicationUtils.ReadChangeVectorFrom(globalChangeVectorTree);
        }

        public void SetGlobalChangeVector(Transaction tx, ByteStringContext context, Dictionary<Guid, long> changeVector)
        {
            var tree = tx.CreateTree(SchemaNameConstants.GlobalChangeVectorTree);
            ReplicationUtils.WriteChangeVectorTo(context, changeVector, tree);
        }

        public long GetLastReplicateEtagFrom(Transaction tx, string dbId)
        {
            var readTree = tx.ReadTree(SchemaNameConstants.LastReplicatedEtagsTree);
            var readResult = readTree.Read(dbId);

            return readResult?.Reader.ReadLittleEndianInt64() ?? 0;
        }

        public void SetLastReplicateEtagFrom(Transaction tx, ByteStringContext context, string dbId, long etag)
        {
            var etagsTree = tx.CreateTree(SchemaNameConstants.LastReplicatedEtagsTree);
            Slice etagSlice;
            Slice keySlice;
            using (Slice.From(context, dbId, out keySlice))
            using (Slice.External(context, (byte*)&etag, sizeof(long), out etagSlice))
            {
                etagsTree.Add(keySlice, etagSlice);
            }
        }


        private void MergeEntryVectorWithGlobal(Transaction tx,
            ByteStringContext context,
            ChangeVectorEntry[] changeVectorForWrite)
        {
            var globalChangeVectorTree = tx.ReadTree(SchemaNameConstants.GlobalChangeVectorTree);
            var globalChangeVector = ReplicationUtils.ReadChangeVectorFrom(globalChangeVectorTree);

            // merge metadata change vector into global change vector
            // --> if we have any entry in global vector smaller than in metadata vector,
            // update the entry in global vector to a larger one
            foreach (var item in ReplicationUtils.MergeVectors(globalChangeVector, changeVectorForWrite))
            {
                var dbId = item.DbId;
                var etagBigEndian = Bits.SwapBytes(item.Etag);
                Slice key;
                Slice value;
                using (Slice.External(context, (byte*)&dbId, sizeof(Guid), out key))
                using (Slice.External(context, (byte*)&etagBigEndian, sizeof(long), out value))
                    globalChangeVectorTree.Add(key, value);
            }
        }

       
        private void ThrowIfAlreadyExistsAndOverwriting(
            string indexName,
            IndexEntryType type,
            int indexIndexId,
            Table table,
            Slice indexNameAsSlice,
            IndexEntryMetadata existing)
        {
            if (!table.VerifyKeyExists(indexNameAsSlice) || indexIndexId == -1 || existing == null || existing.Id == -1)
            {
                if (Logger.IsInfoEnabled &&
                    indexIndexId != -1 &&
                    existing != null &&
                    existing.Id == -1 &&
                    existing.Type != type)
                {
                    Logger.Info($"Writing {type}, and there is a tombstone of {existing.Type} under the same name. The created {type} will have take the change vector from the tombstone.");
                }

                return;
            }

            string msg;
            switch (type)
            {
                case IndexEntryType.Index:
                    msg =
                        $"Tried to create an index with a name of {indexName}, but an index or a transformer under the same name exist";
                    break;
                case IndexEntryType.Transformer:
                    msg =
                        $"Tried to create an transformer with a name of {indexName}, but an index or a transformer under the same name exist";
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(type),
                        $"Unknown index/transformer type. For the record, I've got {(int)type}..");
            }

            throw new IndexOrTransformerAlreadyExistException(msg);
        }

        public IndexEntryMetadata GetIndexMetadataByName<TTransaction>(Transaction tx, TransactionOperationContext<TTransaction> context, string name, bool returnNullIfTombstone = true)
            where TTransaction : RavenTransaction
        {
            var table = tx.OpenTable(IndexesTableSchema, SchemaNameConstants.IndexMetadataTable);

            Slice nameAsSlice;
            TableValueReader tvr;
            using (DocumentKeyWorker.GetSliceFromKey(context, name, out nameAsSlice))
                tvr = table.ReadByKey(nameAsSlice);

            return tvr == null ? null : TableValueToMetadata(tvr, context, returnNullIfTombstone);
        }


        private IndexEntryMetadata TableValueToMetadata(TableValueReader tvr,
            JsonOperationContext context,
            bool returnNullIfTombstone)
        {
            var metadata = new IndexEntryMetadata();

            int size;
            metadata.Id = Bits.SwapBytes(*(int*)tvr.Read((int)MetadataFields.Id, out size));
            if (returnNullIfTombstone && metadata.Id == -1)
                return null;

            metadata.Name = new LazyStringValue(null, tvr.Read((int)MetadataFields.Name, out size), size, context).ToString();

            metadata.ChangeVector = ReplicationUtils.GetChangeVectorEntriesFromTableValueReader(tvr, (int)MetadataFields.ChangeVector);
            metadata.Type = (IndexEntryType)(*tvr.Read((int)MetadataFields.Type, out size));
            metadata.Etag = Bits.SwapBytes(*(long*)tvr.Read((int)MetadataFields.Etag, out size));

            return metadata;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long ReadLastEtag(Transaction tx)
        {
            return ReadLastEtag(tx.OpenTable(IndexesTableSchema, SchemaNameConstants.IndexMetadataTable));
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

        private void DeleteIndexMetadataForRemovedIndexesAndTransformers(Transaction tx, DocumentsOperationContext context,IndexStore indexStore,
            TransformerStore transformerStore)
        {
            var toRemove = new List<IndexEntryMetadata>();
            var table = tx.OpenTable(IndexesTableSchema,
                SchemaNameConstants.IndexMetadataTable);
            foreach (var tvr in table.SeekForwardFrom(IndexesTableSchema.FixedSizeIndexes[EtagIndexName], 0))
            {
                var metadata = TableValueToMetadata(tvr, context, true);
                if (metadata == null) //noting to do if it is a tombstone
                    continue;

                if (metadata.Type == IndexEntryType.Index &&
                    indexStore.GetIndex(metadata.Id) == null)
                {
                    toRemove.Add(metadata);
                }

                if (metadata.Type == IndexEntryType.Transformer &&
                    transformerStore.GetTransformer(metadata.Id) == null)
                {
                    toRemove.Add(metadata);
                }
            }

            foreach (var metadata in toRemove)
                WriteEntry(tx, metadata.Name, metadata.Type, -1, context);
        }

        public class IndexEntryMetadata
        {
            public int Id;
            public string Name;
            public long Etag;
            public IndexEntryType Type;
            public ChangeVectorEntry[] ChangeVector;
        }

        public enum MetadataFields
        {
            Id = 0,
            Etag = 1,
            Name = 2,
            Type = 3,
            ChangeVector = 4
        }

        public static class SchemaNameConstants
        {
            public const string IndexMetadataTable = "Indexes";
            public const string GlobalChangeVectorTree = "GlobalChangeVectorTree";
            public const string LastReplicatedEtagsTree = "LastReplicatedEtags";
        }


    }

    public enum IndexEntryType : byte
    {
        Index = 1,
        Transformer = 2
    }
}