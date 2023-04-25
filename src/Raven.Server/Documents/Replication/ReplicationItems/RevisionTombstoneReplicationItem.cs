using System;
using System.Diagnostics;
 using System.Globalization;
using System.IO;
using Raven.Server.Documents.Replication.Stats;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
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

        public override DynamicJsonValue ToDebugJson()
        {
            var djv = base.ToDebugJson();
            djv[nameof(Collection)] = Collection.ToString(CultureInfo.InvariantCulture);
            djv[nameof(Id)] = Id.ToString(CultureInfo.InvariantCulture);
            return djv;
        }
        
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
            var index = Id.IndexOf((char)SpecialChars.RecordSeparator, StringComparison.OrdinalIgnoreCase);
            if (index == -1)
                return;

            Id = context.AllocateStringValue(null, Id.Buffer + index + 1, Id.Size - index - 1);
        }

        public static ByteStringContext.InternalScope TryExtractChangeVectorSliceFromKey(ByteStringContext allocator, LazyStringValue key, out Slice changeVectorSlice)
        {
            TryExtractDocumentIdAndChangeVectorFromKey(key, out _, out var changeVector);
            return Slice.From(allocator, changeVector, out changeVectorSlice);
        }

        public static void TryExtractDocumentIdAndChangeVectorFromKey(LazyStringValue key, out string docId, out string changeVector)
        {
            var index = key.IndexOf((char)SpecialChars.RecordSeparator, StringComparison.OrdinalIgnoreCase);
            if (index == -1)
            {
                docId = null;
                changeVector = key;
            }
            else
            {
                docId = key.Substring(0, index);
                changeVector = key.Substring(index + 1);
            }
        }

        protected override void InnerDispose()
        {
            Id?.Dispose();
            Collection?.Dispose();
        }
    }
}
