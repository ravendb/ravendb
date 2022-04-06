using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Voron;
using Voron.Data.Tables;
using static Raven.Server.Documents.DocumentsStorage;
using static Raven.Server.Documents.Schemas.Attachments;
using static Raven.Server.Documents.Schemas.Collections;
using static Raven.Server.Documents.Schemas.Conflicts;
using static Raven.Server.Documents.Schemas.Documents;
using static Raven.Server.Documents.Schemas.Revisions;
using static Raven.Server.Documents.Schemas.Tombstones;

namespace Raven.Server.Storage.Schema.Updates.Documents
{
    public unsafe class From40010 : ISchemaUpdate
    {
        public int From => 40_010;
        public int To => 40_011;
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Documents;

        public bool Update(UpdateStep step)
        {
            // Update collections
            using (step.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var readTable = step.ReadTx.OpenTable(CollectionsSchemaBase, CollectionsSlice);
                if (readTable != null)
                {
                    var writeTable = step.WriteTx.OpenTable(CollectionsSchemaBase, CollectionsSlice);
                    foreach (var read in readTable.SeekByPrimaryKey(Slices.BeforeAllKeys, 0))
                    {
                        using (TableValueReaderUtil.CloneTableValueReader(context, read))
                        {
                            var collection = TableValueToString(context, (int)CollectionsTable.Name, ref read.Reader);
                            using (DocumentIdWorker.GetStringPreserveCase(context, collection, out Slice collectionSlice))
                            using (writeTable.Allocate(out TableValueBuilder write))
                            {
                                write.Add(collectionSlice);
                                var pk = read.Reader.Read((int)CollectionsTable.Name, out int size);
                                using (Slice.External(context.Allocator, pk, size, out var pkSlice))
                                {
                                    writeTable.DeleteByKey(pkSlice);
                                }
                                writeTable.Insert(write);
                            }
                        }
                    }
                }
            }

            // Update tombstones collection value
            using (step.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                foreach (var collection in step.DocumentsStorage.GetTombstoneCollections(step.ReadTx))
                {
                    string tableName;
                    if (collection == AttachmentsTombstones ||
                        collection == RevisionsTombstones)
                    {
                        tableName = collection;
                    }
                    else
                    {
                        var collectionName = new CollectionName(collection);
                        tableName = collectionName.GetTableName(CollectionTableType.Tombstones);
                    }

                    var readTable = step.ReadTx.OpenTable(TombstonesSchemaBase, tableName);
                    if (readTable == null)
                        continue;

                    var writeTable = step.WriteTx.OpenTable(TombstonesSchemaBase, tableName);
                    // We seek by an index instead the PK because 
                    // we weed to ensure that we aren't accessing an IsGlobal key
                    foreach (var read in readTable.SeekForwardFrom(TombstonesSchemaBase.FixedSizeIndexes[CollectionEtagsSlice], 0, 0))
                    {
                        // We copy the memory of the read so AssertNoReferenceToOldData won't throw.
                        // This is done instead of moving AssertNoReferenceToOldData to assert later 
                        // after we allocate the new write memory.
                        using (TableValueReaderUtil.CloneTableValueReader(context, read))
                        {
                            var type = *(Tombstone.TombstoneType*)read.Reader.Read((int)TombstoneTable.Type, out _);
                            var oldCollection = TableValueToString(context, (int)TombstoneTable.Collection, ref read.Reader);
                            using (DocumentIdWorker.GetStringPreserveCase(context, oldCollection, out Slice collectionSlice))
                            using (writeTable.Allocate(out TableValueBuilder write))
                            {
                                write.Add(read.Reader.Read((int)TombstoneTable.LowerId, out int size), size);
                                write.Add(read.Reader.Read((int)TombstoneTable.Etag, out size), size);
                                write.Add(read.Reader.Read((int)TombstoneTable.DeletedEtag, out size), size);
                                write.Add(read.Reader.Read((int)TombstoneTable.TransactionMarker, out size), size);
                                write.Add(read.Reader.Read((int)TombstoneTable.Type, out size), size);
                                if (type == Tombstone.TombstoneType.Attachment)
                                {
                                    write.Add(read.Reader.Read((int)TombstoneTable.Collection, out size), size);
                                }
                                else
                                {
                                    write.Add(collectionSlice);
                                }
                                write.Add(read.Reader.Read((int)TombstoneTable.Flags, out size), size);
                                write.Add(read.Reader.Read((int)TombstoneTable.ChangeVector, out size), size);
                                write.Add(read.Reader.Read((int)TombstoneTable.LastModified, out size), size);
                                writeTable.Set(write);
                            }
                        }
                    }
                }
            }

            // Update conflicts' collection value
            using (step.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var readTable = step.ReadTx.OpenTable(ConflictsSchemaBase, ConflictsSlice);
                if (readTable != null)
                {
                    var writeTable = step.WriteTx.OpenTable(ConflictsSchemaBase, ConflictsSlice);
                    foreach (var read in readTable.SeekByPrimaryKey(Slices.BeforeAllKeys, 0))
                    {
                        using (TableValueReaderUtil.CloneTableValueReader(context, read))
                        {
                            var oldCollection = TableValueToString(context, (int)ConflictsTable.Collection, ref read.Reader);
                            using (DocumentIdWorker.GetStringPreserveCase(context, oldCollection, out Slice collectionSlice))
                            using (writeTable.Allocate(out TableValueBuilder write))
                            {
                                write.Add(read.Reader.Read((int)ConflictsTable.LowerId, out int size), size);
                                write.Add(read.Reader.Read((int)ConflictsTable.RecordSeparator, out size), size);
                                write.Add(read.Reader.Read((int)ConflictsTable.ChangeVector, out size), size);
                                write.Add(read.Reader.Read((int)ConflictsTable.Id, out size), size);
                                write.Add(read.Reader.Read((int)ConflictsTable.Data, out size), size);
                                write.Add(read.Reader.Read((int)ConflictsTable.Etag, out size), size);
                                write.Add(collectionSlice);
                                write.Add(read.Reader.Read((int)ConflictsTable.LastModified, out size), size);
                                write.Add(read.Reader.Read((int)ConflictsTable.Flags, out size), size);
                                writeTable.Set(write);
                            }
                        }
                    }
                }
            }

            return true;
        }
    }
}
