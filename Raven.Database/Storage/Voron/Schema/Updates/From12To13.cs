// -----------------------------------------------------------------------
//  <copyright file="From10To11.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Runtime.CompilerServices;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Storage.Voron.Impl;
using Raven.Database.Storage.Voron.StorageActions.StructureSchemas;
using Raven.Json.Linq;
using Sparrow;
using Voron;
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
            using (var tx = tableStorage.Environment.NewTransaction(TransactionFlags.ReadWrite))
            {
                var reduceKeyTypes = tx.ReadTree(Tables.ReduceKeyTypes.TableName);
                var reduceKeyTypesByView = tx.ReadTree(Tables.ReduceKeyTypes.Indices.ByView);
                using (var it = reduceKeyTypes.Iterate())
                {
                    if (it.Seek(Slice.BeforeAllKeys))
                    {
                        do
                        {
                            var current = it.ReadStructForCurrent(tableStorage.ReduceKeyTypes.Schema);

                            var viewId = current.ReadInt(ReduceKeyTypeFields.IndexId);
                            reduceKeyTypesByView.MultiAdd(CreateViewKey(viewId), it.CurrentKey);

                        } while (it.MoveNext());
                    }
                }

                tx.Commit();
            }
            using (var tx = tableStorage.Environment.NewTransaction(TransactionFlags.ReadWrite))
            {
                var reduceKeyCounts = tx.ReadTree(Tables.ReduceKeyCounts.TableName);
                var reduceKeyCountsByView = tx.ReadTree(Tables.ReduceKeyCounts.Indices.ByView);
                using (var it = reduceKeyCounts.Iterate())
                {
                    if (it.Seek(Slice.BeforeAllKeys))
                    {
                        do
                        {
                            var current = it.ReadStructForCurrent(tableStorage.ReduceKeyCounts.Schema);

                            var viewId = current.ReadInt(ReduceKeyCountFields.IndexId);
                            reduceKeyCountsByView.MultiAdd(CreateViewKey(viewId), it.CurrentKey);

                        } while (it.MoveNext());
                    }
                }

                tx.Commit();
            }
            using (var tx = tableStorage.Environment.NewTransaction(TransactionFlags.ReadWrite))
            {
                var reduceResults = tx.ReadTree(Tables.ReduceResults.TableName);
                var reduceResultsByView = tx.ReadTree(Tables.ReduceResults.Indices.ByView);
                var reduceResultsByViewAndKeyAndLevel = tx.ReadTree(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevel);
                var reduceResultsByViewAndKeyAndLevelAndBucket = tx.ReadTree(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndBucket);
                var reduceResultsByViewAndKeyAndLevelAndSourceBucket = tx.ReadTree(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket);

                using (var it = reduceResults.Iterate())
                {
                    if (it.Seek(Slice.BeforeAllKeys))
                    {
                        do
                        {
                            var current = it.ReadStructForCurrent(tableStorage.ReduceResults.Schema);

                            var viewId = current.ReadInt(ReduceResultFields.IndexId);
                            var reduceKey = current.ReadString(ReduceResultFields.ReduceKey);
                            var level = current.ReadInt(ReduceResultFields.Level);
                            var bucket = current.ReadInt(ReduceResultFields.Bucket);
                            var sourceBucket = current.ReadInt(ReduceResultFields.SourceBucket);
                            reduceResultsByView.MultiAdd(CreateViewKey(viewId), it.CurrentKey);
                            reduceResultsByViewAndKeyAndLevel.MultiAdd(CreateReduceResultsKey(viewId, reduceKey, level), it.CurrentKey);
                            reduceResultsByViewAndKeyAndLevelAndBucket.MultiAdd(CreateReduceResultsWithBucketKey(viewId, reduceKey, level, bucket), it.CurrentKey);
                            reduceResultsByViewAndKeyAndLevelAndSourceBucket.MultiAdd(CreateReduceResultsWithBucketKey(viewId, reduceKey, level, sourceBucket), it.CurrentKey);

                        } while (it.MoveNext());
                    }
                }

                tx.Commit();
            }

            using (var tx = tableStorage.Environment.NewTransaction(TransactionFlags.ReadWrite))
            {
                var mappedResults = tx.ReadTree(Tables.MappedResults.TableName);
                var mappedResultsByView = tx.ReadTree(Tables.MappedResults.Indices.ByView);
                // we didn't changed this one format, explicitly ignored
                // var mappedResultsByViewAndDocumentId = tx.ReadTree(Tables.MappedResults.Indices.ByViewAndDocumentId);
                var mappedResultsByViewAndReduceKey = tx.ReadTree(Tables.MappedResults.Indices.ByViewAndReduceKey);
                var mappedResultsByViewAndReduceKeyAndSourceBucket = tx.ReadTree(Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket);

                using (var it = mappedResults.Iterate())
                {
                    if (it.Seek(Slice.BeforeAllKeys))
                    {
                        do
                        {
                            var current = it.ReadStructForCurrent(tableStorage.MappedResults.Schema);

                            var viewId = current.ReadInt(MappedResultFields.IndexId);
                            var reduceKey = current.ReadString(MappedResultFields.ReduceKey);
                            var bucket = current.ReadInt(MappedResultFields.Bucket);

                            mappedResultsByView.MultiAdd(CreateViewKey(viewId), it.CurrentKey);
                            mappedResultsByViewAndReduceKey.MultiAdd(CreateMappedResultKey(viewId, reduceKey), it.CurrentKey);
                            mappedResultsByViewAndReduceKeyAndSourceBucket.MultiAdd(CreateMappedResultWithBucketKey(viewId, reduceKey, bucket), it.CurrentKey);

                        } while (it.MoveNext());
                    }
                }

                tx.Commit();
            }

            UpdateSchemaVersion(tableStorage, output);
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