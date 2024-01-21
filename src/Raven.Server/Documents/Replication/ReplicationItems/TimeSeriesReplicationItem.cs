using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using Raven.Client;
using Raven.Client.Util;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.TimeSeries;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Voron;

namespace Raven.Server.Documents.Replication.ReplicationItems
{
    public sealed class TimeSeriesDeletedRangeItem : ReplicationBatchItem
    {
        public Slice Key;
        public LazyStringValue Collection;
        public DateTime From;
        public DateTime To;

        public override long Size => base.Size + // common
                                     sizeof(long) + // from time
                                     sizeof(long) + // to time
                                     sizeof(int) + // size of doc collection
                                     Collection.Size; // doc collection;

        public override DynamicJsonValue ToDebugJson()
        {
            var djv = base.ToDebugJson();
            djv[nameof(Collection)] = Collection?.ToString(CultureInfo.InvariantCulture) ?? Constants.Documents.Collections.EmptyCollection;
            djv[nameof(Key)] = CompoundKeyHelper.ExtractDocumentId(Key);
            djv[nameof(From)] = From.ToString("O");
            djv[nameof(To)] = To.ToString("O");
            return djv;
        }

        public override long AssertChangeVectorSize() => Size;

        public override unsafe void Write(Slice changeVector, Stream stream, byte[] tempBuffer, OutgoingReplicationStatsScope stats)
        {
            fixed (byte* pTemp = tempBuffer)
            {
                if (AssertChangeVectorSize() > tempBuffer.Length)
                    ThrowTooManyChangeVectorEntries(this, Key.ToString());

                var tempBufferPos = WriteCommon(changeVector, pTemp);

                *(int*)(pTemp + tempBufferPos) = Key.Size;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, Key.Content.Ptr, Key.Size);
                tempBufferPos += Key.Size;

                *(long*)(pTemp + tempBufferPos) = From.Ticks;
                tempBufferPos += sizeof(long);

                *(long*)(pTemp + tempBufferPos) = To.Ticks;
                tempBufferPos += sizeof(long);

                *(int*)(pTemp + tempBufferPos) = Collection.Size;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, Collection.Buffer, Collection.Size);
                tempBufferPos += Collection.Size;

                stream.Write(tempBuffer, 0, tempBufferPos);
                stats.RecordTimeSeriesDeletedRangeOutput(Size);
            }
        }

        public override unsafe void Read(JsonOperationContext context, ByteStringContext allocator, IncomingReplicationStatsScope stats)
        {
            using (stats.For(ReplicationOperation.Incoming.TimeSeriesDeletedRangeRead))
            {
                var keySize = *(int*)Reader.ReadExactly(sizeof(int));
                var key = Reader.ReadExactly(keySize);
                ToDispose(Slice.From(allocator, key, keySize, ByteStringType.Immutable, out Key));

                From = new DateTime(*(long*)Reader.ReadExactly(sizeof(long)));
                To = new DateTime(*(long*)Reader.ReadExactly(sizeof(long)));

                SetLazyStringValueFromString(context, out Collection);
                Debug.Assert(Collection != null);

                stats.RecordTimeSeriesDeletedRangeRead(Size);
            }
        }

        protected override ReplicationBatchItem CloneInternal(JsonOperationContext context, ByteStringContext allocator)
        {
            return new TimeSeriesDeletedRangeItem
            {
                Collection = Collection.Clone(context),
                From = From,
                To = To
            };
        }

        protected override void InnerDispose()
        {
            Collection?.Dispose();
        }
    }

    public sealed class TimeSeriesReplicationItem : ReplicationBatchItem
    {
        public Slice Key; // docId|lower-name|baseline
        public LazyStringValue Name; // original casing
        public LazyStringValue Collection;
        public TimeSeriesValuesSegment Segment;

        public override long Size => base.Size + // common

                                     sizeof(int) + // segment key size
                                     Key.Size + // segment key

                                     sizeof(int) + // size of the segment
                                     Segment.NumberOfBytes + // data

                                     sizeof(int) + // size of doc collection
                                     Collection.Size + // doc collection

                                     sizeof(int) + // size of name
                                     Name.Size; // name;

        public override DynamicJsonValue ToDebugJson()
        {
            var djv = base.ToDebugJson();
            djv[nameof(Collection)] = Collection?.ToString(CultureInfo.InvariantCulture) ?? Constants.Documents.Collections.EmptyCollection;
            djv[nameof(Name)] = Name.ToString(CultureInfo.InvariantCulture);
            djv[nameof(Key)] = CompoundKeyHelper.ExtractDocumentId(Key);
            return djv;
        }

        public override long AssertChangeVectorSize() => Size;

        public override unsafe void Write(Slice changeVector, Stream stream, byte[] tempBuffer, OutgoingReplicationStatsScope stats)
        {
            fixed (byte* pTemp = tempBuffer)
            {
                if (AssertChangeVectorSize() > tempBuffer.Length)
                    ThrowTooManyChangeVectorEntries(this, Key.ToString());

                var tempBufferPos = WriteCommon(changeVector, pTemp);

                *(int*)(pTemp + tempBufferPos) = Key.Size;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, Key.Content.Ptr, Key.Size);
                tempBufferPos += Key.Size;

                *(int*)(pTemp + tempBufferPos) = Segment.NumberOfBytes;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, Segment.Ptr, Segment.NumberOfBytes);
                tempBufferPos += Segment.NumberOfBytes;

                *(int*)(pTemp + tempBufferPos) = Collection.Size;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, Collection.Buffer, Collection.Size);
                tempBufferPos += Collection.Size;

                *(int*)(pTemp + tempBufferPos) = Name.Size;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, Name.Buffer, Name.Size);
                tempBufferPos += Name.Size;

                stream.Write(tempBuffer, 0, tempBufferPos);
                stats.RecordTimeSeriesOutput(Size);
            }
        }

        public override unsafe void Read(JsonOperationContext context, ByteStringContext allocator, IncomingReplicationStatsScope stats)
        {
            using (stats.For(ReplicationOperation.Incoming.TimeSeriesRead))
            {
                var keySize = *(int*)Reader.ReadExactly(sizeof(int));
                var key = Reader.ReadExactly(keySize);
                ToDispose(Slice.From(allocator, key, keySize, ByteStringType.Immutable, out Key));

                var segmentSize = *(int*)Reader.ReadExactly(sizeof(int));
                var mem = Reader.AllocateMemory(segmentSize);
                Memory.Copy(mem, Reader.ReadExactly(segmentSize), segmentSize);
                Segment = new TimeSeriesValuesSegment(mem, segmentSize);

                SetLazyStringValueFromString(context, out Collection);
                Debug.Assert(Collection != null);

                SetLazyStringValueFromString(context, out Name);
                Debug.Assert(Name != null);

                stats.RecordTimeSeriesRead(Size);
            }
        }


        protected override ReplicationBatchItem CloneInternal(JsonOperationContext context, ByteStringContext allocator)
        {
            var item = new TimeSeriesReplicationItem
            {
                Collection = Collection.Clone(context)
            };

            var mem = Segment.Clone(context, out item.Segment);
            item.Key = Key.Clone(allocator);

            item.ToDispose(new DisposableAction(() =>
            {
                item.Key.Release(allocator);
                context.ReturnMemory(mem);
            }));
            
            return item;
        }

        protected override void InnerDispose()
        {
            Collection?.Dispose();
        }
    }
}
