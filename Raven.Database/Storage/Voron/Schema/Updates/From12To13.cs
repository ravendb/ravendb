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
                tableStorage.Environment.DeleteTree(tx, Tables.ReduceKeyTypes.Indices.ByView);

                tableStorage.Environment.DeleteTree(tx, Tables.ReduceKeyCounts.Indices.ByView);

                tableStorage.Environment.DeleteTree(tx, Tables.ReduceResults.Indices.ByView);
                tableStorage.Environment.DeleteTree(tx, Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevel);
                tableStorage.Environment.DeleteTree(tx, Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndBucket);
                tableStorage.Environment.DeleteTree(tx, Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket);

                tableStorage.Environment.DeleteTree(tx, Tables.MappedResults.Indices.ByView);
                // we didn't changed this one format, explicitly ignored
                //tableStorage.Environment.DeleteTree(tx, Tables.MappedResults.Indices.ByViewAndDocumentId);
                tableStorage.Environment.DeleteTree(tx, Tables.MappedResults.Indices.ByViewAndReduceKey);
                tableStorage.Environment.DeleteTree(tx, Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket);


                tx.Commit();
            }
            MigrateIndexes(tableStorage, output, Tables.ReduceKeyTypes.TableName,
                tx => new
                {
                    reduceKeyTypesByView = tx.ReadTree(Tables.ReduceKeyTypes.Indices.ByView)

                }, (state, it) =>
                {
                    var current = it.ReadStructForCurrent(tableStorage.ReduceKeyTypes.Schema);

                    var viewId = current.ReadInt(ReduceKeyTypeFields.IndexId);
                    state.reduceKeyTypesByView.MultiAdd(CreateViewKey(viewId), it.CurrentKey);
                });

            MigrateIndexes(tableStorage, output, Tables.ReduceKeyCounts.TableName,
              tx => new
              {
                  reduceKeyCountsByView = tx.ReadTree(Tables.ReduceKeyCounts.Indices.ByView)
              }, (state, it) =>
              {
                  var current = it.ReadStructForCurrent(tableStorage.ReduceKeyTypes.Schema);

                  var viewId = current.ReadInt(ReduceKeyTypeFields.IndexId);
                  state.reduceKeyCountsByView.MultiAdd(CreateViewKey(viewId), it.CurrentKey);
              });

            MigrateIndexes(tableStorage, output, Tables.ReduceResults.TableName,
                tx => new
                {
                    reduceResultsByView = tx.ReadTree(Tables.ReduceResults.Indices.ByView),
                    reduceResultsByViewAndKeyAndLevel = tx.ReadTree(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevel),
                    reduceResultsByViewAndKeyAndLevelAndBucket = tx.ReadTree(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndBucket),
                    reduceResultsByViewAndKeyAndLevelAndSourceBucket = tx.ReadTree(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket)

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
                    mappedResultsByView = tx.ReadTree(Tables.MappedResults.Indices.ByView),
                    // we didn't changed this one format, explicitly ignored
                    // var mappedResultsByViewAndDocumentId = tx.ReadTree(Tables.MappedResults.Indices.ByViewAndDocumentId);
                    mappedResultsByViewAndReduceKey = tx.ReadTree(Tables.MappedResults.Indices.ByViewAndReduceKey),
                    mappedResultsByViewAndReduceKeyAndSourceBucket = tx.ReadTree(Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket)

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
            var hasMore = true;
            while (hasMore)
            {
                using (var tx = tableStorage.Environment.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var table = tx.ReadTree(tableName);

                    output("Migrating " + tableName + ", with " + table.State.EntriesCount + " entries");

                    var state = initState(tx);

                    using (var it = table.Iterate())
                    {
                        if (it.Seek(lastKey))
                        {
                            do
                            {

                                processEntry(state, it);

                                if (++count > 50000)
                                {
                                    output("Migrated 50,000 records from "+ tableName + ", pulsing transaction");
                                    // now move to the next one and so we'll run with it next time
                                    if (it.MoveNext())
                                    {
                                        lastKey = it.CurrentKey.Clone();
                                        count = 0;
                                        break;
                                    }
                                    hasMore = false;
                                    break;
                                }
                            } while (it.MoveNext());
                            hasMore = false;
                        }
                    }

                    tx.Commit();
                }
                output("Finished migration " + tableName + " in " + sp.Elapsed);
            }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected Slice CreateViewKey(int view)
        {
            var sliceWriter = new SliceWriter(4);
            sliceWriter.WriteBigEndian(view);
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