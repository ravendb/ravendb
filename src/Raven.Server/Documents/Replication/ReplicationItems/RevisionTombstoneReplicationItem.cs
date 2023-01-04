using System;
using System.Diagnostics;
using System.IO;
using Raven.Server.Documents.Replication.Stats;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;

namespace Raven.Server.Documents.Replication.ReplicationItems
{
    public class RevisionTombstoneReplicationItem : ReplicationBatchItem
    {
        public LazyStringValue Collection;
        public LazyStringValue Id;
        public DocumentFlags Flags;

        public override long AssertChangeVectorSize()
        {
            return sizeof(byte) + // type
                   sizeof(int) + // # of change vectors
                   Encodings.Utf8.GetByteCount(ChangeVector) +
                   sizeof(short) + // transaction marker
                   sizeof(long) + // last modified
                   sizeof(int) + // size of key
                   Id.Size +
                   sizeof(int) + // size of collection
                   Collection.Size;
        }

        public override long Size => 0;

        public override unsafe void Write(Slice changeVector, Stream stream, byte[] tempBuffer, OutgoingReplicationStatsScope stats)
        {
            fixed (byte* pTemp = tempBuffer)
            {
                if (AssertChangeVectorSize() > tempBuffer.Length)
                    ThrowTooManyChangeVectorEntries(this, Id);

                var tempBufferPos = WriteCommon(changeVector, pTemp);

                *(long*)(pTemp + tempBufferPos) = LastModifiedTicks;
                tempBufferPos += sizeof(long);

                *(int*)(pTemp + tempBufferPos) = Id.Size;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, Id.Buffer, Id.Size);
                tempBufferPos += Id.Size;

                *(int*)(pTemp + tempBufferPos) = Collection.Size;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, Collection.Buffer, Collection.Size);
                tempBufferPos += Collection.Size;

                stream.Write(tempBuffer, 0, tempBufferPos);

                stats.RecordRevisionTombstoneOutput();
            }
        }

        public override unsafe void Read(JsonOperationContext context, ByteStringContext allocator, IncomingReplicationStatsScope stats)
        {
            using (stats.For(ReplicationOperation.Incoming.TombstoneRead))
            {
                stats.RecordRevisionTombstoneRead();
                LastModifiedTicks = *(long*)Reader.ReadExactly(sizeof(long));
                SetLazyStringValueFromString(context, out Id);
                SetLazyStringValueFromString(context, out Collection);
                Debug.Assert(Collection != null);
            }
        }

        protected override ReplicationBatchItem CloneInternal(JsonOperationContext context, ByteStringContext allocator)
        {
            return new RevisionTombstoneReplicationItem
            {
                Collection = Collection.Clone(context),
                Id = Id.Clone(context)
            };
        }

        public unsafe void StripDocumentIdFromKeyIfNeeded(JsonOperationContext context)
        {
            var p = Id.Buffer;
            var size = Id.Size;
            int sizeOfDocId = 0;
            for (; sizeOfDocId < size; sizeOfDocId++)
            {
                if (p[sizeOfDocId] == SpecialChars.RecordSeparator)
                    break;
            }

            var changeVectorIndex = sizeOfDocId + 1;
            if (changeVectorIndex >= size)
                return;

            Id = context.AllocateStringValue(null, p + changeVectorIndex, size - sizeOfDocId - 1);
        }

        public static ByteStringContext.InternalScope TryExtractChangeVectorSliceFromKey(ByteStringContext allocator, LazyStringValue key, out Slice changeVectorSlice)
        {
            var index = key.IndexOf((char)SpecialChars.RecordSeparator, StringComparison.OrdinalIgnoreCase);
            var changeVectorIndex = index + 1;
            var changeVector = changeVectorIndex >= key.Size ? key : key.Substring(changeVectorIndex);
            return Slice.From(allocator, changeVector, out changeVectorSlice);
        }

        public override void InnerDispose()
        {
        }
    }
}
