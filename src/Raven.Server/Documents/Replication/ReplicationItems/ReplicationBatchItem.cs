using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Replication.ReplicationItems
{
    public abstract class ReplicationBatchItem : IDisposable
    {
        public long Etag;
        public short TransactionMarker;
        public ReplicationItemType Type;
        public string ChangeVector;
        public long LastModifiedTicks;

        protected Reader Reader;

        private List<IDisposable> _garbage;

        public abstract long AssertChangeVectorSize();

        public abstract long Size { get; }

        public abstract void Write(Slice changeVector, Stream stream, byte[] tempBuffer, OutgoingReplicationStatsScope stats);

        public abstract void Read(DocumentsOperationContext context, IncomingReplicationStatsScope stats);

        protected abstract ReplicationBatchItem CloneInternal(JsonOperationContext context);

        public ReplicationBatchItem Clone(JsonOperationContext context)
        {
            var item = CloneInternal(context);

            item.Type = Type;
            item.ChangeVector = ChangeVector;
            item.LastModifiedTicks = LastModifiedTicks;
            item.Etag = Etag;
            item.TransactionMarker = TransactionMarker;
            return item;
        }

        public static unsafe ReplicationBatchItem ReadTypeAndInstantiate(Reader reader)
        {
            var type = *(ReplicationItemType*)reader.ReadExactly(sizeof(byte));

            switch (type)
            {
                case ReplicationItemType.Document:
                case ReplicationItemType.DocumentTombstone:
                    return new DocumentReplicationItem { Type = type, Reader = reader };
                case ReplicationItemType.Attachment:
                case ReplicationItemType.AttachmentStream:
                    return new AttachmentReplicationItem { Type = type, Reader = reader };
                case ReplicationItemType.AttachmentTombstone:
                    return new AttachmentTombstoneReplicationItem { Type = type, Reader = reader };
                case ReplicationItemType.RevisionTombstone:
                    return new RevisionTombstoneReplicationItem { Type = type, Reader = reader };
                case ReplicationItemType.LegacyCounter:
                    throw new InvalidOperationException($"Received an item of type '{type}'. Replication of counters and counter tombstones between 4.1.x and {ServerVersion.Version} is not supported.");
                case ReplicationItemType.CounterGroup:
                    return new CounterReplicationItem { Type = type, Reader = reader };
                case ReplicationItemType.TimeSeriesSegment:
                    return new TimeSeriesReplicationItem { Type = type, Reader = reader };
                case ReplicationItemType.DeletedTimeSeriesRange:
                    return new TimeSeriesDeletedRangeItem { Type = type, Reader = reader };
                default:
                    throw new ArgumentOutOfRangeException(type.ToString());
            }
        }

        public unsafe void ReadChangeVectorAndMarker()
        {
            var changeVectorSize = *(int*)Reader.ReadExactly(sizeof(int));

            if (changeVectorSize != 0)
                ChangeVector = Encoding.UTF8.GetString(Reader.ReadExactly(changeVectorSize), changeVectorSize);

            TransactionMarker = *(short*)Reader.ReadExactly(sizeof(short));
        }

        protected unsafe int WriteCommon(Slice changeVector, byte* tempBuffer)
        {
            var tempBufferPos = 0;
            tempBuffer[tempBufferPos++] = (byte)Type;

            *(int*)(tempBuffer + tempBufferPos) = changeVector.Size;
            tempBufferPos += sizeof(int);
            Memory.Copy(tempBuffer + tempBufferPos, changeVector.Content.Ptr, changeVector.Size);
            tempBufferPos += changeVector.Size;

            *(short*)(tempBuffer + tempBufferPos) = TransactionMarker;
            tempBufferPos += sizeof(short);
            return tempBufferPos;
        }

        protected unsafe void SetLazyStringValue(DocumentsOperationContext context, ref LazyStringValue prop)
        {
            var size = *(int*)Reader.ReadExactly(sizeof(int));
            if (size < 0)
                return;

            if (size == 0)
            {
                prop = context.Empty;
                return;
            }

            var mem = Reader.AllocateMemory(size);
            Memory.Copy(mem, Reader.ReadExactly(size), size);
            prop = context.AllocateStringValue(null, mem, size);
        }

        protected unsafe void SetLazyStringValueFromString(DocumentsOperationContext context, out LazyStringValue prop)
        {
            prop = null;
            var size = *(int*)Reader.ReadExactly(sizeof(int));
            if (size < 0)
                return;

            // This is a special (broken) case.
            // On the source it is stored as Slice in LSV format which is wrong(?) unlike the normal LSV the escaping position is kept before the value itself.
            // and therefore the escaping doesn't include in the LSV size.
            // Additionally, we are over-allocating so writing this value doesn't cause a failure (we look for the escaping after the value)
            // this also work, because we don't pass those values between contexts, if we need to do so, we convert it to string first.

            // TODO: this is inefficient, can skip string allocation
            prop = context.GetLazyString(Encoding.UTF8.GetString(Reader.ReadExactly(size), size));
        }

        public enum ReplicationItemType : byte
        {
            Document = 1,
            DocumentTombstone = 2,
            Attachment = 3,
            AttachmentStream = 4,
            AttachmentTombstone = 5,
            RevisionTombstone = 6,
            LegacyCounter = 7,

            CounterGroup = 9,
            TimeSeriesSegment = 10,
            DeletedTimeSeriesRange = 11
        }

        public void ToDispose(IDisposable obj)
        {
            if (_garbage == null)
                _garbage = new List<IDisposable>();

            _garbage.Add(obj);
        }

        public abstract void InnerDispose();

        public void Dispose()
        {
            InnerDispose();

            if (_garbage == null)
                return;

            foreach (var disposable in _garbage)
            {
                disposable.Dispose();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ThrowTooManyChangeVectorEntries(ReplicationBatchItem item, string id)
        {
            throw new ArgumentOutOfRangeException(nameof(item),
                $"{item.Type} '{id}' has too many change vector entries to replicate: {item.ChangeVector.Length}");
        }
    }

    public class Reader
    {
        private readonly (IDisposable ReleaseBuffer, JsonOperationContext.MemoryBuffer Buffer) _copiedBuffer;
        private readonly Stream _stream;
        private readonly IncomingReplicationHandler.IncomingReplicationAllocator _allocator;

        public Reader(Stream stream, (IDisposable ReleaseBuffer, JsonOperationContext.MemoryBuffer Buffer) copiedBuffer, IncomingReplicationHandler.IncomingReplicationAllocator allocator)
        {
            _copiedBuffer = copiedBuffer;
            _stream = stream;
            _allocator = allocator;
        }

        internal unsafe byte* ReadExactly(int size)
        {
            var diff = _copiedBuffer.Buffer.Valid - _copiedBuffer.Buffer.Used;
            if (diff >= size)
            {
                var result = _copiedBuffer.Buffer.Address + _copiedBuffer.Buffer.Used;
                _copiedBuffer.Buffer.Used += size;
                return result;
            }
            return ReadExactlyUnlikely(size, diff);
        }

        private unsafe byte* ReadExactlyUnlikely(int size, int diff)
        {
            Memory.Move(
                _copiedBuffer.Buffer.Address,
                _copiedBuffer.Buffer.Address + _copiedBuffer.Buffer.Used,
                diff);
            _copiedBuffer.Buffer.Valid = diff;
            _copiedBuffer.Buffer.Used = 0;
            while (diff < size)
            {
                var read = _stream.Read(_copiedBuffer.Buffer.Memory.Memory.Span.Slice(diff, _copiedBuffer.Buffer.Size - diff));
                if (read == 0)
                    throw new EndOfStreamException();

                _copiedBuffer.Buffer.Valid += read;
                diff += read;
            }
            var result = _copiedBuffer.Buffer.Address + _copiedBuffer.Buffer.Used;
            _copiedBuffer.Buffer.Used += size;
            return result;
        }

        internal unsafe void ReadExactly(byte* ptr, int size)
        {
            var written = 0;

            while (size > 0)
            {
                var available = _copiedBuffer.Buffer.Valid - _copiedBuffer.Buffer.Used;
                if (available == 0)
                {
                    var read = _stream.Read(_copiedBuffer.Buffer.Memory.Memory.Span);
                    if (read == 0)
                        throw new EndOfStreamException();

                    _copiedBuffer.Buffer.Valid = read;
                    _copiedBuffer.Buffer.Used = 0;
                    continue;
                }

                var min = Math.Min(size, available);
                var result = _copiedBuffer.Buffer.Address + _copiedBuffer.Buffer.Used;
                Memory.Copy(ptr + written, result, (uint)min);
                written += min;
                _copiedBuffer.Buffer.Used += min;
                size -= min;
            }
        }

        internal void ReadExactly(long size, Stream file)
        {
            while (size > 0)
            {
                var available = _copiedBuffer.Buffer.Valid - _copiedBuffer.Buffer.Used;
                if (available == 0)
                {
                    var read = _stream.Read(_copiedBuffer.Buffer.Memory.Memory.Span);
                    if (read == 0)
                        throw new EndOfStreamException();

                    _copiedBuffer.Buffer.Valid = read;
                    _copiedBuffer.Buffer.Used = 0;
                    continue;
                }
                var min = (int)Math.Min(size, available);
                file.Write(_copiedBuffer.Buffer.Memory.Memory.Span.Slice(_copiedBuffer.Buffer.Used, min));
                _copiedBuffer.Buffer.Used += min;
                size -= min;
            }
        }

        internal unsafe byte* AllocateMemory(int size)
        {
            return _allocator.AllocateMemory(size);
        }
    }
}
