﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Server.Commercial;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents.TimeSeries
{
    public unsafe class TimeSeriesStorage
    {
        public const int MaxSegmentSize = 2048;

        private static readonly Slice AllTimeSeriesEtagSlice;

        private static Slice CollectionTimeSeriesEtagsSlice;

        private static readonly Slice TimeSeriesKeysSlice;

        private static readonly TableSchema TimeSeriesSchema = new TableSchema
        {
            TableType = (byte)TableType.TimeSeries
        };


        private readonly DocumentDatabase _documentDatabase;
        private readonly DocumentsStorage _documentsStorage;

        private HashSet<string> _tableCreated = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        static TimeSeriesStorage()
        {
            using (StorageEnvironment.GetStaticContext(out ByteStringContext ctx))
            {
                Slice.From(ctx, "AllTimeSeriesEtag", ByteStringType.Immutable, out AllTimeSeriesEtagSlice);
                Slice.From(ctx, "CollectionTimeSeriesEtags", ByteStringType.Immutable, out CollectionTimeSeriesEtagsSlice);
                Slice.From(ctx, "TimeSeriesKeys", ByteStringType.Immutable, out TimeSeriesKeysSlice);
            }

            TimeSeriesSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)TimeSeriesTable.TimeSeriesKey,
                Count = 1,
                Name = TimeSeriesKeysSlice,
                IsGlobal = true
            });

            TimeSeriesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)TimeSeriesTable.Etag,
                Name = AllTimeSeriesEtagSlice,
                IsGlobal = true
            });

            TimeSeriesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)TimeSeriesTable.Etag,
                Name = DocumentsStorage.CollectionEtagsSlice
            });
        }


        public TimeSeriesStorage(DocumentDatabase documentDatabase)
        {
            _documentDatabase = documentDatabase;
            _documentsStorage = documentDatabase.DocumentsStorage;
        }

        public string AppendTimestamp(DocumentsOperationContext context, string documentId, string collection, string name, DateTime timestamp, Span<double> values,
            string tag,
            bool fromReplication)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                Debug.Assert(false); // never hit
            }

            
            long newEtag = _documentsStorage.GenerateNextEtag();
            string databaseChangeVector = context.LastDatabaseChangeVector ?? DocumentsStorage.GetDatabaseChangeVector(context);
            string changeVector = ChangeVectorUtils.TryUpdateChangeVector(_documentDatabase, databaseChangeVector, newEtag).ChangeVector;
            context.LastDatabaseChangeVector = changeVector;

            CollectionName collectionName = _documentsStorage.ExtractCollectionName(context, collection);
            Table table = GetTimeSeriesTable(context.Transaction.InnerTransaction, collectionName);

            using (Slice.From(context.Allocator, tag, out Slice tagSlice))
            using (DocumentIdWorker.GetSliceFromId(context, documentId, out Slice documentKeyPrefix, SpecialChars.RecordSeparator))
            using (Slice.From(context.Allocator, name, out Slice timeSeriesName))
            using (context.Allocator.Allocate(documentKeyPrefix.Size + timeSeriesName.Size + 1 /* separator */ + sizeof(long) /*  segment start */,
                out ByteString timeSeriesKeyBuffer))
            using (CreateTimeSeriesKeyPrefixSlice(context, timeSeriesKeyBuffer, documentKeyPrefix, timeSeriesName, out Slice timeSeriesPrefixSlice))
            using (CreateTimeSeriesKeySlice(context, timeSeriesKeyBuffer, timeSeriesPrefixSlice, timestamp, out Slice timeSeriesKeySlice))
            using (Slice.From(context.Allocator, changeVector, out Slice cv))
            using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
            {
                if (tagSlice.Size > byte.MaxValue)
                    throw new ArgumentOutOfRangeException(nameof(tag), $"Tag '{tag}' is too big (max 255 bytes) for document '{documentId}' in time series: {name}");



                byte* buffer = stackalloc byte[MaxSegmentSize];
                if (table.SeekOneBackwardByPrimaryKeyPrefix(timeSeriesPrefixSlice, timeSeriesKeySlice, out TableValueReader tvr) == false)
                {
                    // no matches for this series at all, need to create new segment

                    return AppendNewSegment(buffer, tagSlice, timeSeriesKeySlice, cv, collectionSlice, values);
                }
                
                // we found the segment that should contain the new value, now we need to check if we need can append
                // it to the end or need to split it

                byte* key = tvr.Read((int)TimeSeriesTable.TimeSeriesKey, out int keySize);
                long baselineMilliseconds = Bits.SwapBytes(
                    *(long*)(key + keySize - sizeof(long))
                );
                
                var baseline = new DateTime(baselineMilliseconds);
                
                // TODO: Need to handle different number of values
                

                var delta = timestamp.Ticks * 10_000 - baselineMilliseconds;

                var segmentReadOnlyBuffer = tvr.Read((int)TimeSeriesTable.Segment, out int size);
                var readOnlySegment = new TimeSeriesValuesSegment(segmentReadOnlyBuffer, size);
                var canAppend = timestamp > readOnlySegment.GetLastTimestamp(baseline);
                if ( canAppend && 
                    delta < int.MaxValue) // if the range is too big (over 50 days, using ms precision), we need a new segment
                {
                    // this is the simplest scenario, we can just add it.
                    Memory.Copy(buffer, segmentReadOnlyBuffer, size);
                    var newSegment = new TimeSeriesValuesSegment(buffer, MaxSegmentSize);
                    // checking if we run out of space here, in which can we'll create new segment
                    if (newSegment.Append((int)delta, values, tagSlice.AsSpan()))
                    {
                        
                        AppendExistingSegment(key, keySize, cv, buffer, newSegment.NumberOfBytes, collectionSlice);
                        return changeVector;
                    }
                }

                if (canAppend)
                {
                    // either the range is too high to fit in a single segment (~50 days) or the 
                    // previous segment is full, we can just create a completely new segment with the 
                    // new value
                    return AppendNewSegment(buffer, tagSlice, timeSeriesKeySlice, cv, collectionSlice, values);
                }
                
                // here we have a complex scenario, we need to add it in the middle of the current segment
                // to do that, we have to re-create it from scratch
                
                var splitSegment = new TimeSeriesValuesSegment(buffer, MaxSegmentSize);
                splitSegment.Initialize(values.Length);

                var enumerator = readOnlySegment.GetEnumerator();

                var valuesBuffer = stackalloc double[readOnlySegment.NumberOfValues];
                var currentValues = new Span<double>(valuesBuffer, readOnlySegment.NumberOfValues);
                var stateBuffer = stackalloc TimeStampState[readOnlySegment.NumberOfValues];
                var state = new Span<TimeStampState>(stateBuffer, readOnlySegment.NumberOfValues);
                var currentTag = new Span<byte>();

                var addedCurrent = false;
                // TODO: Need to handle different number of values

                while (enumerator.MoveNext(out var currentTimestamp, currentValues, state, ref currentTag))
                {
                    var current = baseline.AddMilliseconds(currentTimestamp);
                    if (current < timestamp)
                    {
                        // no need to check if we added it, since we know it fits
                        splitSegment.Append(currentTimestamp, currentValues, currentTag);
                        continue;
                    }
                    
                   
                    if (addedCurrent == false)
                    {
                        addedCurrent = true;

                        var shouldAdd = true;
                        
                        // if the time stamps are equal, we need to decide who to take
                        if (current == timestamp)
                        {
                            shouldAdd = fromReplication == false || // if not from replication, this value overrides
                                        values.SequenceCompareTo(currentValues) > 0; // if from replication, the largest value wins
                        }

                        if (shouldAdd)
                        {
                            if (splitSegment.Append(currentTimestamp, values, tagSlice.AsSpan()) == false)
                            {
                                changeVector = FlushCurrentSegment(cv, buffer, ref splitSegment, collectionSlice, currentTimestamp, timeSeriesKeyBuffer, currentValues, currentTag, ref key, ref keySize, ref baseline);
                            }
                            if(current == timestamp)
                                continue; // we overwrote the one from the current segment, skip this
                        }
                    }


                    if (splitSegment.Append(currentTimestamp, currentValues, currentTag) == false)
                    {
                        changeVector = FlushCurrentSegment(cv, buffer, ref splitSegment, collectionSlice, currentTimestamp, timeSeriesKeyBuffer, currentValues, currentTag, ref key, ref keySize, ref baseline);
                    }
                }

                AppendExistingSegment(key, keySize, cv, buffer, splitSegment.NumberOfBytes, collectionSlice);

                return changeVector;
            }

            string AppendNewSegment(byte* buffer, Slice tagSlice, Slice timeSeriesKeySlice, Slice cv, Slice collectionSlice, Span<double> valuesCopy)
            {
                int deltaFromStart = 0;

                var newSegment = new TimeSeriesValuesSegment(buffer, MaxSegmentSize);
                newSegment.Initialize(valuesCopy.Length);
                newSegment.Append(deltaFromStart, valuesCopy, tagSlice.AsSpan());

                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(timeSeriesKeySlice);
                    tvb.Add(Bits.SwapBytes(newEtag));
                    tvb.Add(cv);
                    tvb.Add(buffer, newSegment.NumberOfBytes);
                    tvb.Add(collectionSlice);
                    tvb.Add(context.TransactionMarkerOffset);

                    table.Insert(tvb);
                }

                return changeVector;
            }

            void AppendExistingSegment(byte* key, int keySize, Slice cv, byte* segmentBuffer, int segmentSize, Slice collectionSlice)
            {
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(key, keySize);
                    tvb.Add(Bits.SwapBytes(newEtag));
                    tvb.Add(cv);
                    tvb.Add(segmentBuffer, segmentSize);
                    tvb.Add(collectionSlice);
                    tvb.Add(context.TransactionMarkerOffset);

                    table.Set(tvb);
                }
            }

            string FlushCurrentSegment(Slice cv, byte* buffer, ref TimeSeriesValuesSegment splitSegment, Slice collectionSlice, int currentTimestamp, ByteString timeSeriesKeyBuffer,
                Span<double> currentValues, Span<byte> currentTag, ref byte* key, ref int keySize, ref DateTime baseline)
            {
                AppendExistingSegment(key, keySize, cv, buffer, splitSegment.NumberOfBytes, collectionSlice);
                baseline = baseline.AddMilliseconds(currentTimestamp);

                *(long*)(timeSeriesKeyBuffer.Ptr + timeSeriesKeyBuffer.Length - sizeof(int)) = Bits.SwapBytes(baseline.Ticks * 10_000);
                key = timeSeriesKeyBuffer.Ptr;
                keySize = timeSeriesKeyBuffer.Length;

                newEtag = _documentsStorage.GenerateNextEtag();
                changeVector = ChangeVectorUtils.TryUpdateChangeVector(_documentDatabase, databaseChangeVector, newEtag).ChangeVector;
                context.LastDatabaseChangeVector = changeVector;

                splitSegment.Initialize(currentValues.Length);

                var result = splitSegment.Append(currentTimestamp, currentValues, currentTag);
                if (result == false)
                    throw new InvalidOperationException($"After renewal of segment, was unable to append a new value. Shouldn't happen. Doc: {documentId}, name: {name}");
                return changeVector;
            }
        }

        private static ByteStringContext<ByteStringMemoryCache>.ExternalScope CreateTimeSeriesKeySlice(DocumentsOperationContext context, ByteString buffer,
            Slice timeSeriesPrefixSlice, DateTime timestamp, out Slice timeSeriesKeySlice)
        {
            var scope = Slice.External(context.Allocator, buffer.Ptr, buffer.Length, out timeSeriesKeySlice);
            long ticks = timestamp.Ticks * 10_000; // shift to millisecond precision
            *(long*)(buffer.Ptr + timeSeriesPrefixSlice.Size) = Bits.SwapBytes(ticks);
            return scope;
        }

        private static ByteStringContext<ByteStringMemoryCache>.ExternalScope CreateTimeSeriesKeyPrefixSlice(DocumentsOperationContext context, ByteString buffer,
            Slice documentIdPrefix, Slice timeSeriesName, out Slice timeSeriesPrefixSlice)
        {
            documentIdPrefix.CopyTo(buffer.Ptr);
            timeSeriesName.CopyTo(buffer.Ptr + documentIdPrefix.Size);
            int pos = documentIdPrefix.Size + timeSeriesName.Size;
            buffer.Ptr[pos++] = SpecialChars.RecordSeparator;
            var scope = Slice.External(context.Allocator, buffer.Ptr, pos, out timeSeriesPrefixSlice);
            return scope;
        }

        public Table GetTimeSeriesTable(Transaction tx, CollectionName collection)
        {
            string tableName = collection.GetTableName(CollectionTableType.CounterGroups);

            if (tx.IsWriteTransaction && _tableCreated.Contains(collection.Name) == false)
            {
                // RavenDB-11705: It is possible that this will revert if the transaction
                // aborts, so we must record this only after the transaction has been committed
                // note that calling the Create() method multiple times is a noop
                TimeSeriesSchema.Create(tx, tableName, 16);
                tx.LowLevelTransaction.OnDispose += _ =>
                {
                    if (tx.LowLevelTransaction.Committed == false)
                        return;

                    // not sure if we can _rely_ on the tx write lock here, so let's be safe and create
                    // a new instance, just in case 
                    _tableCreated = new HashSet<string>(_tableCreated, StringComparer.OrdinalIgnoreCase)
                    {
                        collection.Name
                    };
                };
            }

            return tx.OpenTable(TimeSeriesSchema, tableName);
        }

        private enum TimeSeriesTable
        {
            // Format of this is:
            // lower document id, record separator, lower time series name, record separator, segment start  
            TimeSeriesKey = 0,
            Etag = 1,
            ChangeVector = 2,
            Segment = 3,
            Collection = 4,
            TransactionMarker = 5
        }
    }
}
