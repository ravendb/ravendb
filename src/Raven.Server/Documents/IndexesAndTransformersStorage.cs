using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Raven.Client.Replication.Messages;
using Raven.Server.Documents.Indexes;
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
                var globalChangeVector = ReadChangeVectorFrom(globalChangeVectorTree);

                var newEtag = lastEtag + 1;
                var changeVector = UpdateChangeVectorWithNewEtag(_environment.DbId, newEtag, globalChangeVector);

                WriteMetadataInternal(table, index.IndexId, newEtag, changeVector);

                var newGlobalChangeVector = MergeVectors(globalChangeVector, changeVector);
                WriteChangeVectorTo(context, newGlobalChangeVector, globalChangeVectorTree);
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

                using (Slice.External(context.Allocator, (byte*) &indexId, sizeof(int), out indexIdAsSlice))
                {
                    table.DeleteByKey(indexIdAsSlice);
                }

                WriteTombstoneInternal();
            }
        }

        private void WriteTombstoneInternal()
        {

        }

        private static void WriteChangeVectorTo(TransactionOperationContext context, ChangeVectorEntry[] changeVector, Tree tree)
        {
            foreach (var item in changeVector)
            {
                var dbId = item.DbId;
                var etagBigEndian = IPAddress.HostToNetworkOrder(item.Etag);
                Slice key;
                Slice value;
                using (Slice.External(context.Allocator, (byte*)&dbId, sizeof(Guid), out key))
                using (Slice.External(context.Allocator, (byte*)&etagBigEndian, sizeof(long), out value))
                    tree.Add(key, value);
            }

        }

        private static ChangeVectorEntry[] UpdateChangeVectorWithNewEtag(Guid dbId, long newEtag, ChangeVectorEntry[] changeVector)
        {
            var length = changeVector.Length;
            for (int i = 0; i < length; i++)
            {
                if (changeVector[i].DbId == dbId)
                {
                    changeVector[i].Etag = newEtag;
                    return changeVector;
                }
            }
            Array.Resize(ref changeVector, length + 1);
            changeVector[length].DbId = dbId;
            changeVector[length].Etag = newEtag;
            return changeVector;
        }

        private Metadata TableValueToMetadata(TableValueReader tvr)
        {
            var metadata = new Metadata();

            int size;
            metadata.Id = IPAddress.NetworkToHostOrder(*(int*)tvr.Read((int)MetadataFields.Id, out size));
            metadata.ChangeVector = GetChangeVectorEntriesFromTableValueReader(tvr, (int)MetadataFields.ChangeVector);
            metadata.Etag = IPAddress.NetworkToHostOrder(*(long*)tvr.Read((int)MetadataFields.Etag, out size));

            return metadata;
        }

        private static TEnum GetEnumFromTableValueReader<TEnum>(TableValueReader tvr, int index)
        {
            int size;
            var storageTypeNum = *(int*)tvr.Read(index, out size);
            return (TEnum)Enum.ToObject(typeof(TEnum), storageTypeNum);
        }

        private static ChangeVectorEntry[] GetChangeVectorEntriesFromTableValueReader(TableValueReader tvr, int index)
        {
            int size;
            var pChangeVector = (ChangeVectorEntry*)tvr.Read(index, out size);
            var changeVector = new ChangeVectorEntry[size / sizeof(ChangeVectorEntry)];
            for (int i = 0; i < changeVector.Length; i++)
            {
                changeVector[i] = pChangeVector[i];
            }
            return changeVector;
        }

        private static ChangeVectorEntry[] ReadChangeVectorFrom(Tree tree)
        {
            var changeVector = new ChangeVectorEntry[tree.State.NumberOfEntries];
            using (var iter = tree.Iterate(false))
            {
                if (iter.Seek(Slices.BeforeAllKeys) == false)
                    return changeVector;
                var buffer = new byte[sizeof(Guid)];
                int index = 0;
                do
                {
                    var read = iter.CurrentKey.CreateReader().Read(buffer, 0, sizeof(Guid));
                    if (read != sizeof(Guid))
                        throw new InvalidDataException($"Expected guid, but got {read} bytes back for change vector");

                    changeVector[index].DbId = new Guid(buffer);
                    changeVector[index].Etag = iter.CreateReaderForCurrent().ReadBigEndianInt64();
                    index++;
                } while (iter.MoveNext());
            }
            return changeVector;
        }

        private static ChangeVectorEntry[] MergeVectors(ChangeVectorEntry[] vectorA, ChangeVectorEntry[] vectorB)
        {
            var merged = new ChangeVectorEntry[Math.Max(vectorA.Length, vectorB.Length)];
            var inx = 0;
            var largerVector = (vectorA.Length >= vectorB.Length) ? vectorA : vectorB;
            var smallerVector = (largerVector == vectorA) ? vectorB : vectorA;
            foreach (var entryA in largerVector)
            {
                var etagA = entryA.Etag;
                var first = new ChangeVectorEntry();
                foreach (var e in smallerVector)
                {
                    if (e.DbId == entryA.DbId)
                    {
                        first = e;
                        break;
                    }
                }
                var etagB = first.Etag;

                merged[inx++] = new ChangeVectorEntry
                {
                    DbId = entryA.DbId,
                    Etag = Math.Max(etagA, etagB)
                };
            }
            return merged;
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