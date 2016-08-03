// -----------------------------------------------------------------------
//  <copyright file="From10To11.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Storage.Voron.Impl;
using Raven.Database.Storage.Voron.StorageActions.StructureSchemas;
using Raven.Json.Linq;
using Sparrow;
using Voron;
using Voron.Impl;
using Voron.Trees;

namespace Raven.Database.Storage.Voron.Schema.Updates
{
    internal class From12To13 : SchemaUpdateBase
    {
        public override string FromSchemaVersion
        {
            get { return "1.2"; }
        }
        public override string ToSchemaVersion
        {
            get { return "1.3"; }
        }

        public override void Update(TableStorage tableStorage, Action<string> output)
        {
            using (var tx = tableStorage.Environment.NewTransaction(TransactionFlags.ReadWrite))
            {
                tableStorage.Environment.DeleteTree(tx, tableStorage.ScheduledReductions.GetIndexKey(Tables.ScheduledReductions.Indices.ByView));
                tableStorage.Environment.DeleteTree(tx, tableStorage.ScheduledReductions.GetIndexKey(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey));

                tableStorage.Environment.DeleteTree(tx, tableStorage.ReduceKeyTypes.GetIndexKey(Tables.ReduceKeyTypes.Indices.ByView));

                tableStorage.Environment.DeleteTree(tx, tableStorage.ReduceKeyCounts.GetIndexKey(Tables.ReduceKeyCounts.Indices.ByView));

                tableStorage.Environment.DeleteTree(tx, tableStorage.ReduceResults.GetIndexKey(Tables.ReduceResults.Indices.ByView));
                tableStorage.Environment.DeleteTree(tx, tableStorage.ReduceResults.GetIndexKey(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevel));
                tableStorage.Environment.DeleteTree(tx, tableStorage.ReduceResults.GetIndexKey(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndBucket));
                tableStorage.Environment.DeleteTree(tx, tableStorage.ReduceResults.GetIndexKey(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket));

                tableStorage.Environment.DeleteTree(tx, tableStorage.MappedResults.GetIndexKey(Tables.MappedResults.Indices.ByView));
                // we didn't changed this one format, explicitly ignored
                //tableStorage.Environment.DeleteTree(tx, Tables.MappedResults.Indices.ByViewAndDocumentId);
                tableStorage.Environment.DeleteTree(tx, tableStorage.MappedResults.GetIndexKey(Tables.MappedResults.Indices.ByViewAndReduceKey));
                tableStorage.Environment.DeleteTree(tx, tableStorage.MappedResults.GetIndexKey(Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket));


                tx.Commit();
            }
            MigrateIndexes(tableStorage, output, Tables.ReduceKeyTypes.TableName,
                tx => new
                {
                    reduceKeyTypesByView = tableStorage.Environment.CreateTree(tx, tableStorage.ReduceKeyTypes.GetIndexKey(Tables.ReduceKeyTypes.Indices.ByView))

                }, (state, it) =>
                {
                    var current = it.ReadStructForCurrent(tableStorage.ReduceKeyTypes.Schema);

                    var viewId = current.ReadInt(ReduceKeyTypeFields.IndexId);
                    state.reduceKeyTypesByView.MultiAdd(CreateViewKey(viewId), it.CurrentKey);
                });

            MigrateIndexes(tableStorage, output, Tables.ScheduledReductions.TableName,
               tx => new
               {
                   byView = tableStorage.Environment.CreateTree(tx, tableStorage.ScheduledReductions.GetIndexKey(Tables.ScheduledReductions.Indices.ByView)),
                   byViewAndLevelAndReduceKey = tableStorage.Environment.CreateTree(tx, tableStorage.ScheduledReductions.GetIndexKey(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey))

               }, (state, it) =>
               {
                   var current = it.ReadStructForCurrent(tableStorage.ScheduledReductions.Schema);

                   var view = current.ReadInt(ScheduledReductionFields.IndexId);
                   var reduceKey = current.ReadString(ScheduledReductionFields.ReduceKey);
                   var level = current.ReadInt(ScheduledReductionFields.Level);
                   var bucket = current.ReadInt(ScheduledReductionFields.Bucket);
                   var bytes = current.ReadBytes(ScheduledReductionFields.Etag);

                   var scheduleReductionKey = CreateScheduleReductionKey(view, level, reduceKey);

                   state.byView.MultiAdd(CreateViewKey(view), it.CurrentKey);
                   state.byViewAndLevelAndReduceKey.MultiAdd(scheduleReductionKey, CreateBucketAndEtagKey(bucket, Etag.Parse(bytes)));
               });

            MigrateIndexes(tableStorage, output, Tables.ReduceKeyCounts.TableName,
              tx => new
              {
                  reduceKeyCountsByView = tableStorage.Environment.CreateTree(tx, tableStorage.ReduceKeyCounts.GetIndexKey(Tables.ReduceKeyCounts.Indices.ByView))
              }, (state, it) =>
              {
                  var current = it.ReadStructForCurrent(tableStorage.ReduceKeyTypes.Schema);

                  var viewId = current.ReadInt(ReduceKeyTypeFields.IndexId);
                  state.reduceKeyCountsByView.MultiAdd(CreateViewKey(viewId), it.CurrentKey);
              });

            MigrateIndexes(tableStorage, output, Tables.ReduceResults.TableName,
                tx => new
                {
                    reduceResultsByView = tableStorage.Environment.CreateTree(tx, tableStorage.ReduceResults.GetIndexKey(Tables.ReduceResults.Indices.ByView)),
                    reduceResultsByViewAndKeyAndLevel = tableStorage.Environment.CreateTree(tx, tableStorage.ReduceResults.GetIndexKey(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevel)),
                    reduceResultsByViewAndKeyAndLevelAndBucket = tableStorage.Environment.CreateTree(tx, tableStorage.ReduceResults.GetIndexKey(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndBucket)),
                    reduceResultsByViewAndKeyAndLevelAndSourceBucket = tableStorage.Environment.CreateTree(tx, tableStorage.ReduceResults.GetIndexKey(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket))

                }, (state, it) =>
                {
                    var current = it.ReadStructForCurrent(tableStorage.ReduceResults.Schema);

                    var viewId = current.ReadInt(ReduceResultFields.IndexId);
                    var reduceKey = current.ReadString(ReduceResultFields.ReduceKey);
                    var level = current.ReadInt(ReduceResultFields.Level);
                    var bucket = current.ReadInt(ReduceResultFields.Bucket);
                    var sourceBucket = current.ReadInt(ReduceResultFields.SourceBucket);
                    state.reduceResultsByView.MultiAdd(CreateViewKey(viewId), it.CurrentKey);
                    state.reduceResultsByViewAndKeyAndLevel.MultiAdd(CreateReduceResultsKey(viewId, reduceKey, level), it.CurrentKey);
                    state.reduceResultsByViewAndKeyAndLevelAndBucket.MultiAdd(CreateReduceResultsWithBucketKey(viewId, reduceKey, level, bucket), it.CurrentKey);
                    state.reduceResultsByViewAndKeyAndLevelAndSourceBucket.MultiAdd(CreateReduceResultsWithBucketKey(viewId, reduceKey, level, sourceBucket), it.CurrentKey);
                });

            MigrateIndexes(tableStorage, output, Tables.MappedResults.TableName,
                tx => new
                {
                    mappedResultsByView = tableStorage.Environment.CreateTree(tx, tableStorage.MappedResults.GetIndexKey(Tables.MappedResults.Indices.ByView)),
                    // we didn't changed this one format, explicitly ignored
                    // var mappedResultsByViewAndDocumentId = tableStorage.Environment.CreateTree(tx, Tables.MappedResults.Indices.ByViewAndDocumentId);
                    mappedResultsByViewAndReduceKey = tableStorage.Environment.CreateTree(tx, tableStorage.MappedResults.GetIndexKey(Tables.MappedResults.Indices.ByViewAndReduceKey)),
                    mappedResultsByViewAndReduceKeyAndSourceBucket = tableStorage.Environment.CreateTree(tx, tableStorage.MappedResults.GetIndexKey(Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket))

                }, (state, it) =>
                {
                    var current = it.ReadStructForCurrent(tableStorage.MappedResults.Schema);

                    var viewId = current.ReadInt(MappedResultFields.IndexId);
                    var reduceKey = current.ReadString(MappedResultFields.ReduceKey);
                    var bucket = current.ReadInt(MappedResultFields.Bucket);

                    state.mappedResultsByView.MultiAdd(CreateViewKey(viewId), it.CurrentKey);
                    state.mappedResultsByViewAndReduceKey.MultiAdd(CreateMappedResultKey(viewId, reduceKey), it.CurrentKey);
                    state.mappedResultsByViewAndReduceKeyAndSourceBucket.MultiAdd(CreateMappedResultWithBucketKey(viewId, reduceKey, bucket), it.CurrentKey);
                });

            UpdateSchemaVersion(tableStorage, output);
        }

        private void MigrateIndexes<TState>(TableStorage tableStorage, Action<string> output,
            string tableName,
            Func<Transaction, TState> initState,
            Action<TState, TreeIterator> processEntry)
        {
            var sp = Stopwatch.StartNew();
            var lastKey = Slice.BeforeAllKeys;
            var count = 0;
            var totalCount = 0L;
            var hasMore = true;
            while (hasMore)
            {
                hasMore = false;
                using (var tx = tableStorage.Environment.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var table = tx.ReadTree(tableName);

                    output(string.Format("Migrating {0}, with {1:#,#;;0} entries", tableName, table.State.EntriesCount));

                    var state = initState(tx);

                    using (var it = table.Iterate())
                    {
                        if (it.Seek(lastKey))
                        {
                            do
                            {

                                processEntry(state, it);
                                totalCount++;
                                if (++count > 50000)
                                {
                                    output("Migrated 50,000 records from "+ tableName + ", pulsing transaction");
                                    // now move to the next one and so we'll run with it next time
                                    if (it.MoveNext())
                                    {
                                        lastKey = it.CurrentKey.Clone();
                                        count = 0;
                                        hasMore = true;
                                    }
                                    break;
                                }
                            } while (it.MoveNext());
                        }
                    }

                    tx.Commit();
                }
            }
            output(string.Format("Finished migration {0} total of {2:#,#;;0} records in {1}", tableName, sp.Elapsed, totalCount));
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected Slice CreateViewKey(int view)
        {
            var sliceWriter = new SliceWriter(4);
            sliceWriter.WriteBigEndian(view);
            return sliceWriter.CreateSlice();
        }


        private Slice CreateBucketAndEtagKey(int bucket, Etag id)
        {
            var sliceWriter = new SliceWriter(20);
            sliceWriter.WriteBigEndian(bucket);
            sliceWriter.Write(id.ToByteArray());
            return sliceWriter.CreateSlice();
        }
        private Slice CreateScheduleReductionKey(int view, int level, string reduceKey)
        {
            var sliceWriter = new SliceWriter(16);
            sliceWriter.WriteBigEndian(view);
            sliceWriter.WriteBigEndian(level);
            sliceWriter.WriteBigEndian(Hashing.XXHash64.CalculateRaw(reduceKey));

            return sliceWriter.CreateSlice();
        }

        private Slice CreateReduceResultsKey(int view, string reduceKey, int level)
        {
            var sliceWriter = new SliceWriter(16);
            sliceWriter.WriteBigEndian(view);
            sliceWriter.WriteBigEndian(Hashing.XXHash64.CalculateRaw(reduceKey));
            sliceWriter.WriteBigEndian(level);


            return sliceWriter.CreateSlice();
        }

        private Slice CreateReduceResultsWithBucketKey(int view, string reduceKey, int level, int bucket)
        {
            var sliceWriter = new SliceWriter(20);
            sliceWriter.WriteBigEndian(view);
            sliceWriter.WriteBigEndian(Hashing.XXHash64.CalculateRaw(reduceKey));
            sliceWriter.WriteBigEndian(level);
            sliceWriter.WriteBigEndian(bucket);

            return sliceWriter.CreateSlice();
        }


        private Slice CreateMappedResultKey(int view, string reduceKey)
        {
            var sliceWriter = new SliceWriter(12);
            sliceWriter.WriteBigEndian(view);
            sliceWriter.WriteBigEndian(Hashing.XXHash64.CalculateRaw(reduceKey));

            return sliceWriter.CreateSlice();
        }

        private Slice CreateMappedResultWithBucketKey(int view, string reduceKey, int bucket)
        {
            var sliceWriter = new SliceWriter(16);
            sliceWriter.WriteBigEndian(view);
            sliceWriter.WriteBigEndian(Hashing.XXHash64.CalculateRaw(reduceKey));
            sliceWriter.WriteBigEndian(bucket);

            return sliceWriter.CreateSlice();
        }

    }
}
