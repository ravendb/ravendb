using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Exceptions.Indexes;
using Raven.Client.Documents.Replication.Messages;
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

        private TransactionContextPool _contextPool;

        private static readonly TableSchema IndexesTableSchema;
        private static readonly TableSchema ConflictsTableSchema;
        private static readonly Slice EtagIndexName;
        private static readonly Slice NameAndEtagIndexName;

        public bool IsInitialized { get; private set; }

        static IndexesEtagsStorage()
        {
            Slice.From(StorageEnvironment.LabelsContext, "EtagIndexName", out EtagIndexName);
            Slice.From(StorageEnvironment.LabelsContext, "NameAndEtagIndexName", out NameAndEtagIndexName);

            // Table schema is:
            //  - index id - int (-1 if tombstone)
            //  - etag - long
            //  - name - string, lowercase
            //  - type - enum (index / transformer)
            //  - change vector
            //  - is conflicted - boolean 
            //(is conflicted --> a flag, so we will not have to read another table in voron just to check if the index/transformer is conlficted)
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

            //Table schema is:
            //  - name -> string, lowercase
            //  - etag -> long
            //  - type -> enum (index / transformer)
            //  - change vector
            //  - definition of conflicted index/transformer (blittable json)
            ConflictsTableSchema = new TableSchema();
            ConflictsTableSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 1
            });

            ConflictsTableSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)ConflictFields.Name,
                Count = 2,
                Name = NameAndEtagIndexName
            });
        }

        public IndexesEtagsStorage(string resourceName)
        {
            Logger = LoggingSource.Instance.GetLogger<IndexesEtagsStorage>(resourceName);
        }

        public TransactionContextPool ContextPool => _contextPool;

        public void Initialize(StorageEnvironment environment, TransactionContextPool contextPool, IndexStore indexStore, TransformerStore transformerStore)
        {
            _environment = environment;
            _contextPool = contextPool;

            TransactionOperationContext context;
            using (contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                IndexesTableSchema.Create(tx.InnerTransaction, SchemaNameConstants.IndexMetadataTable, 16);
                ConflictsTableSchema.Create(tx.InnerTransaction, SchemaNameConstants.ConflictMetadataTable, 16);

                tx.InnerTransaction.CreateTree(SchemaNameConstants.GlobalChangeVectorTree);
                tx.InnerTransaction.CreateTree(SchemaNameConstants.LastReplicatedEtagsTree);


                DeleteIndexMetadataForRemovedIndexesAndTransformers(tx.InnerTransaction, context, indexStore, transformerStore);
                tx.Commit();
            }

            IsInitialized = true;
        }

        public long OnIndexCreated(Index index)
        {
            TransactionOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var newEtag = WriteEntry(tx.InnerTransaction, index.Name, IndexEntryType.Index, index.IndexId, context);

                tx.Commit();

                return newEtag;
            }
        }

        public long OnIndexDeleted(Index index)
        {
            TransactionOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var newEtag = WriteEntry(tx.InnerTransaction, index.Name, IndexEntryType.Index, -1, context);

                tx.Commit();
                return newEtag;
            }
        }

        public long OnTransformerCreated(Transformer transformer)
        {
            TransactionOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var newEtag = WriteEntry(tx.InnerTransaction, transformer.Name, IndexEntryType.Transformer, transformer.TransformerId, context);

                tx.Commit();
                return newEtag;
            }
        }

        public long OnTransformerDeleted(Transformer transformer)
        {
            TransactionOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var newEtag = WriteEntry(tx.InnerTransaction, transformer.Name, IndexEntryType.Transformer, -1, context);

                tx.Commit();
                return newEtag;
            }
        }

        public void AddConflict(TransactionOperationContext context,
            Transaction tx,
            string name,
            IndexEntryType type,
            ChangeVectorEntry[] changeVector,
            BlittableJsonReaderObject definition)
        {
            if (!TrySetConflictedByName(context, tx, name))
                throw new InvalidOperationException($"When trying to add a conflict on {type} {name}, we couldn't find {name} in the index metadata. Shouldn't happen and likely a bug.");

            var conflictsTable = tx.OpenTable(ConflictsTableSchema, SchemaNameConstants.ConflictMetadataTable);
            var metadataTable = tx.OpenTable(IndexesTableSchema, SchemaNameConstants.IndexMetadataTable);
            Slice indexNameAsSlice;
            var newEtag = GetNewEtag(metadataTable);
            var bitSwappedEtag = Bits.SwapBytes(newEtag);
            using (DocumentKeyWorker.GetSliceFromKey(context, name, out indexNameAsSlice))
            {
                fixed (ChangeVectorEntry* pChangeVector = changeVector)
                {
                    byte byteAsType = (byte)type;
                    TableValueBuilder tableValueBuilder;
                    using (conflictsTable.Allocate(out tableValueBuilder))
                    {
                        tableValueBuilder.Add(indexNameAsSlice);
                        tableValueBuilder.Add(&bitSwappedEtag, sizeof(long));
                        tableValueBuilder.Add(&byteAsType, sizeof(byte));
                        tableValueBuilder.Add((byte*)pChangeVector, sizeof(ChangeVectorEntry) * changeVector.Length);
                        tableValueBuilder.Add(definition.BasePointer, definition.Size);
                        conflictsTable.Set(tableValueBuilder);
                    }
                }
            }
        }

        public bool TrySetConflictedByName(TransactionOperationContext context, Transaction tx, string name)
        {
            var table = tx.OpenTable(IndexesTableSchema, SchemaNameConstants.IndexMetadataTable);
            Debug.Assert(table != null);

            TableValueReader tvr;
            Slice nameAsSlice;
            using (DocumentKeyWorker.GetSliceFromKey(context, name, out nameAsSlice))
            {
                if (table.ReadByKey(nameAsSlice, out tvr) == false)
                    return false;
            }

            var metadata = TableValueToMetadata(ref tvr, context, false);
            WriteEntry(tx, name, metadata.Type, metadata.Id, context, isConflicted: true, allowOverwrite: true);

            return true;
        }

        private long WriteEntry(Transaction tx, string indexName, IndexEntryType type, int indexIndexId,
            TransactionOperationContext context, bool isConflicted = false, bool allowOverwrite = false)
        {
            var table = tx.OpenTable(IndexesTableSchema, SchemaNameConstants.IndexMetadataTable);
            Debug.Assert(table != null);

            var newEtag = GetNewEtag(table);

            Slice nameAsSlice;
            IndexEntryMetadata existing = null;
            ChangeVectorEntry[] changeVectorForWrite;
            using (DocumentKeyWorker.GetSliceFromKey(context, indexName, out nameAsSlice))
            {
                TableValueReader tvr;
                table.ReadByKey(nameAsSlice, out tvr);

                //SetIndexTransformerChangeVectorForLocalChange also merges vectors if conflicts exist
                changeVectorForWrite = SetIndexTransformerChangeVectorForLocalChange(tx, nameAsSlice, ref tvr, newEtag);
                if (tvr.Pointer != null)
                {
                    existing = TableValueToMetadata(ref tvr, context, false);
                }
            }

            //precautions
            if (changeVectorForWrite == null) throw new ArgumentException("changeVector == null, should not be so");

            Slice indexNameAsSlice;
            using (DocumentKeyWorker.GetSliceFromKey(context, indexName, out indexNameAsSlice))
            {
                if (!allowOverwrite)
                {
                    ThrowIfAlreadyExistsAndOverwriting(indexName, type, indexIndexId, table, indexNameAsSlice, existing);
                }

                fixed (ChangeVectorEntry* pChangeVector = changeVectorForWrite)
                {
                    var bitSwappedEtag = Bits.SwapBytes(newEtag);

                    var bitSwappedId = Bits.SwapBytes(indexIndexId);

                    TableValueBuilder tvb;
                    using (table.Allocate(out tvb))
                    {
                        tvb.Add((byte*)&bitSwappedId, sizeof(int));
                        tvb.Add((byte*)&bitSwappedEtag, sizeof(long));
                        tvb.Add(indexNameAsSlice);
                        tvb.Add((byte*)&type, sizeof(byte));
                        tvb.Add((byte*)pChangeVector, sizeof(ChangeVectorEntry) * changeVectorForWrite.Length);
                        tvb.Add((byte*)&isConflicted, sizeof(bool));

                        table.Set(tvb);
                    }
                        
                }
            }

            MergeEntryVectorWithGlobal(tx, context.Allocator, changeVectorForWrite);
            return newEtag;
        }

        private long GetNewEtag(Table table)
        {
            var lastEtag = ReadLastEtag(table);
            var newEtag = lastEtag + 1;
            if (newEtag < 0) throw new ArgumentException("etag must not be negative");
            return newEtag;
        }

        public void PurgeTombstonesFrom(Transaction tx, JsonOperationContext context, long etag, int take)
        {
            int taken = 0;
            var table = tx.OpenTable(IndexesTableSchema, SchemaNameConstants.IndexMetadataTable);

            Debug.Assert(table != null);

            while (true)
            {
                var more = false;
                foreach (var tvr in table.SeekForwardFrom(IndexesTableSchema.FixedSizeIndexes[EtagIndexName], etag, 0))
                {
                    more = true;
                    if (taken++ >= take)
                    {
                        more = false;
                        break;
                    }

                    var metadata = TableValueToMetadata(ref tvr.Reader, context, false);
                    if (metadata.Id == -1)
                    {
                        table.Delete(tvr.Reader.Id);
                        break;
                    }
                }

                if (more == false)
                    break;
            }

            if (Logger.IsInfoEnabled)
            {
                Logger.Info($"Purged index/metadata tombstones from etag = {etag}, total purged {taken}");
            }

        }

        private ChangeVectorEntry[] SetIndexTransformerChangeVectorForLocalChange(Transaction tx, Slice loweredName, ref TableValueReader oldValue, long newEtag)
        {
            if (oldValue.Pointer != null)
            {
                var changeVector = DocumentsStorage.GetChangeVectorEntriesFromTableValueReader(ref oldValue, (int)MetadataFields.ChangeVector);
                return ReplicationUtils.UpdateChangeVectorWithNewEtag(_environment.DbId, newEtag, changeVector);
            }

            return GetMergedConflictChangeVectorsAndDeleteConflicts(tx, loweredName, newEtag);
        }


        private ChangeVectorEntry[] GetMergedConflictChangeVectorsAndDeleteConflicts(Transaction tx, Slice name, long newEtag, ChangeVectorEntry[] existing = null)
        {
            var conflictChangeVectors = DeleteConflictsFor(tx, name);

            //no conflicts, no need to merge
            if (conflictChangeVectors.Count == 0)
            {
                if (existing != null)
                    return ReplicationUtils.UpdateChangeVectorWithNewEtag(_environment.DbId, newEtag, existing);

                return new[]
                {
                    new ChangeVectorEntry
                    {
                        Etag = newEtag,
                        DbId = _environment.DbId
                    }
                };
            }

            // need to merge the conflict change vectors
            var maxEtags = new Dictionary<Guid, long>
            {
                [_environment.DbId] = newEtag
            };

            foreach (var conflictChangeVector in conflictChangeVectors)
                foreach (var entry in conflictChangeVector)
                {
                    long etag;
                    if (maxEtags.TryGetValue(entry.DbId, out etag) == false ||
                        etag < entry.Etag)
                    {
                        maxEtags[entry.DbId] = entry.Etag;
                    }
                }

            var changeVector = new ChangeVectorEntry[maxEtags.Count];

            var index = 0;
            foreach (var maxEtag in maxEtags)
            {
                changeVector[index].DbId = maxEtag.Key;
                changeVector[index].Etag = maxEtag.Value;
                index++;
            }
            return changeVector;
        }

        public IReadOnlyList<ChangeVectorEntry[]> DeleteConflictsFor(Transaction tx, TransactionOperationContext context, string name)
        {
            Slice nameSlice;
            using (DocumentKeyWorker.GetSliceFromKey(context, name, out nameSlice))
                return DeleteConflictsFor(tx, nameSlice);
        }

        private IReadOnlyList<ChangeVectorEntry[]> DeleteConflictsFor(Transaction tx, Slice name)
        {
            var table = tx.OpenTable(ConflictsTableSchema, SchemaNameConstants.ConflictMetadataTable);

            Debug.Assert(table != null);

            var list = new List<ChangeVectorEntry[]>();

            while (true)
            {
                var more = false;
                foreach (var tvr in table.SeekForwardFrom(ConflictsTableSchema.Indexes[NameAndEtagIndexName], name, 0, true))
                {
                    more = true;
                    list.Add(DocumentsStorage.GetChangeVectorEntriesFromTableValueReader(ref tvr.Result.Reader, (int)MetadataFields.ChangeVector));

                    table.Delete(tvr.Result.Reader.Id);
                    break;
                }

                if (more == false)
                    break;
            }

            return list;
        }

        public IEnumerable<IndexConflictEntry> GetConflictsFor(Transaction tx, TransactionOperationContext context, string name, int start, int take)
        {
            var table = tx.OpenTable(ConflictsTableSchema, SchemaNameConstants.ConflictMetadataTable);

            Debug.Assert(table != null);

            Slice nameSlice;
            using (DocumentKeyWorker.GetSliceFromKey(context, name, out nameSlice))
            {
                foreach (var tvr in table.SeekForwardFrom(ConflictsTableSchema.Indexes[NameAndEtagIndexName], nameSlice, start, true))
                {
                    if (take-- <= 0)
                        yield break;

                    yield return TableValueToConflict(ref tvr.Result.Reader, context);
                }
            }
        }

        /// <summary>
        /// this method will fetch all metadata entries - tombstones or otherwise
        /// </summary>
        public List<IndexEntryMetadata> GetAfter(Transaction tx, JsonOperationContext context, long etag, int start, int take)
        {
            int taken = 0;
            var table = tx.OpenTable(IndexesTableSchema, SchemaNameConstants.IndexMetadataTable);

            Debug.Assert(table != null);

            var results = new List<IndexEntryMetadata>();
            foreach (var tvr in table.SeekForwardFrom(IndexesTableSchema.FixedSizeIndexes[EtagIndexName], etag, start))
            {
                if (taken++ >= take)
                    break;

                results.Add(TableValueToMetadata(ref tvr.Reader, context, false));
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

            Debug.Assert(tree != null);

            ReplicationUtils.WriteChangeVectorTo(context, changeVector, tree);
        }

        public long GetLastReplicateEtagFrom(Transaction tx, string dbId)
        {
            var tree = tx.ReadTree(SchemaNameConstants.LastReplicatedEtagsTree);

            Debug.Assert(tree != null);

            var readResult = tree.Read(dbId);

            return readResult?.Reader.ReadLittleEndianInt64() ?? 0;
        }

        public void SetLastReplicateEtagFrom(Transaction tx, ByteStringContext context, string dbId, long etag)
        {
            var etagsTree = tx.CreateTree(SchemaNameConstants.LastReplicatedEtagsTree);
            Debug.Assert(etagsTree != null);

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
            Debug.Assert(globalChangeVectorTree != null);

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
            string name,
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
                        $"Tried to create an index with a name of {name}, but an index or a transformer under the same name exist";
                    break;
                case IndexEntryType.Transformer:
                    msg =
                        $"Tried to create a transformer with a name of {name}, but an index or a transformer under the same name exist";
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

            Debug.Assert(table != null);

            Slice nameAsSlice;
            TableValueReader tvr;
            using (DocumentKeyWorker.GetSliceFromKey(context, name, out nameAsSlice))
            {
                if (table.ReadByKey(nameAsSlice, out tvr) == false)
                    return null;
            }

            return TableValueToMetadata(ref tvr, context, returnNullIfTombstone);
        }

        private IndexEntryMetadata TableValueToMetadata(ref TableValueReader tvr,
            JsonOperationContext context,
            bool returnNullIfTombstone)
        {
            var metadata = new IndexEntryMetadata();

            int size;
            metadata.Id = Bits.SwapBytes(*(int*)tvr.Read((int)MetadataFields.Id, out size));
            if (returnNullIfTombstone && metadata.Id == -1)
                return null;

            metadata.Name = context.AllocateStringValue(null, tvr.Read((int)MetadataFields.Name, out size), size).ToString();

            metadata.ChangeVector = DocumentsStorage.GetChangeVectorEntriesFromTableValueReader(ref tvr, (int)MetadataFields.ChangeVector);
            metadata.Type = (IndexEntryType)(*tvr.Read((int)MetadataFields.Type, out size));
            metadata.Etag = Bits.SwapBytes(*(long*)tvr.Read((int)MetadataFields.Etag, out size));
            metadata.IsConflicted = *(bool*)tvr.Read((int)MetadataFields.IsConflicted, out size);

            return metadata;
        }

        private IndexConflictEntry TableValueToConflict(ref TableValueReader tvr,
            JsonOperationContext context)
        {
            var data = new IndexConflictEntry();

            int size;

            data.Name = context.AllocateStringValue(null, tvr.Read((int)ConflictFields.Name, out size), size).ToString();

            data.ChangeVector = DocumentsStorage.GetChangeVectorEntriesFromTableValueReader(ref tvr, (int)ConflictFields.ChangeVector);
            data.Type = (IndexEntryType)(*tvr.Read((int)ConflictFields.Type, out size));
            data.Etag = Bits.SwapBytes(*(long*)tvr.Read((int)ConflictFields.Etag, out size));

            var ptr = tvr.Read((int)ConflictFields.Definition, out size);
            data.Definition = new BlittableJsonReaderObject(ptr, size, context);

            return data;
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

            var result = table.ReadLast(IndexesTableSchema.FixedSizeIndexes[EtagIndexName]);
            if (result == null)
                return 0;

            int size;
            return Bits.SwapBytes(*(long*)result.Reader.Read((int)MetadataFields.Etag, out size));
        }

        private void DeleteIndexMetadataForRemovedIndexesAndTransformers(Transaction tx, TransactionOperationContext context, IndexStore indexStore,
            TransformerStore transformerStore)
        {
            var toRemove = new List<IndexEntryMetadata>();
            var table = tx.OpenTable(IndexesTableSchema,
                SchemaNameConstants.IndexMetadataTable);
            foreach (var tvr in table.SeekForwardFrom(IndexesTableSchema.FixedSizeIndexes[EtagIndexName], 0, 0))
            {
                var metadata = TableValueToMetadata(ref tvr.Reader, context, true);
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
            public bool IsConflicted;
        }


        public class IndexConflictEntry
        {
            public string Name;
            public long Etag;
            public IndexEntryType Type;
            public ChangeVectorEntry[] ChangeVector;
            public BlittableJsonReaderObject Definition;
        }

        public enum MetadataFields
        {
            Id = 0,
            Etag = 1,
            Name = 2,
            Type = 3,
            ChangeVector = 4,
            IsConflicted = 5
        }

        public enum ConflictFields
        {
            Name = 0,
            Etag = 1,
            Type = 2,
            ChangeVector = 3,
            Definition = 4
        }

        public static class SchemaNameConstants
        {
            public const string IndexMetadataTable = "IndexAndTransformerMetadata";
            public const string ConflictMetadataTable = "IndexAndTransformerConflictsMetadata";
            public const string GlobalChangeVectorTree = "IndexAndTransformerGlobalChangeVectorTree";
            public const string LastReplicatedEtagsTree = "IndexAndTransformerLastReplicatedEtags";
        }
    }

    public enum IndexEntryType : byte
    {
        Index = 1,
        Transformer = 2
    }
}