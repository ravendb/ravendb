using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using static Raven.Server.Documents.ConflictsStorage;
using static Raven.Server.Documents.DocumentsStorage;

namespace Raven.Server.Storage.Schema.Updates.Documents
{
    public unsafe class From10 : ISchemaUpdate
    {
        public bool Update(Transaction readTx, Transaction writeTx, ConfigurationStorage configurationStorage, DocumentsStorage documentsStorage)
        {
            // Update collections
            using (documentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var readTable = readTx.OpenTable(CollectionsSchema, CollectionsSlice);
                var writeTable = writeTx.OpenTable(CollectionsSchema, CollectionsSlice);
                foreach (var read in readTable.SeekByPrimaryKey(Slices.BeforeAllKeys, 0))
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

            // Update tombstones's collection value
            using (documentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                foreach (var tombstoneCollection in documentsStorage.GetTombstoneCollections(readTx))
                {
                    // context.Transaction = new DocumentsTransaction(context, writeTx, new DocumentsChanges());

                    // var collectionName = documentsStorage.ExtractCollectionName(context, "users");
                    var collectionName = new CollectionName("users");
                    var tableName = collectionName.GetTableName(CollectionTableType.Tombstones);
                    var readTable = readTx.OpenTable(TombstonesSchema, tableName);
                    var writeTable = writeTx.OpenTable(TombstonesSchema, tableName);
                    foreach (var read in readTable.SeekByPrimaryKey(Slices.BeforeAllKeys, 0))
                    {
                        var type = *(DocumentTombstone.TombstoneType*)read.Reader.Read((int)TombstoneTable.Type, out int _);
                        var oldCollection = TableValueToString(context, (int)TombstoneTable.Collection, ref read.Reader);
                        using (DocumentIdWorker.GetStringPreserveCase(context, oldCollection, out Slice collectionSlice))
                        using (writeTable.Allocate(out TableValueBuilder write))
                        {
                            write.Add(read.Reader.Read((int)TombstoneTable.LowerId, out int size), size);
                            write.Add(read.Reader.Read((int)TombstoneTable.Etag, out size), size);
                            write.Add(read.Reader.Read((int)TombstoneTable.DeletedEtag, out size), size);
                            write.Add(read.Reader.Read((int)TombstoneTable.TransactionMarker, out size), size);
                            write.Add(read.Reader.Read((int)TombstoneTable.Type, out size), size);
                            if (type == DocumentTombstone.TombstoneType.Attachment)
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

            // Update conflicts' collection value
            using (documentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var readTable = readTx.OpenTable(ConflictsSchema, ConflictsSlice);
                var writeTable = writeTx.OpenTable(ConflictsSchema, ConflictsSlice);
                foreach (var read in readTable.SeekByPrimaryKey(Slices.BeforeAllKeys, 0))
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

            return true;
        }
    }
}
