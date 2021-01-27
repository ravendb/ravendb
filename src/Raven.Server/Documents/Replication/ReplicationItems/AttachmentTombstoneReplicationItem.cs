using System.IO;
using Raven.Client.Util;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;

namespace Raven.Server.Documents.Replication.ReplicationItems
{
    public class AttachmentTombstoneReplicationItem : ReplicationBatchItem
    {
        public Slice Key;

        public override long AssertChangeVectorSize()
        {
            return sizeof(byte) + // type
                   sizeof(int) + // # of change vectors
                   Encodings.Utf8.GetByteCount(ChangeVector) +
                   sizeof(short) + // transaction marker
                   sizeof(long) + // last modified
                   sizeof(int) + // size of key
                   Key.Size;
        }

        public override long Size => 0;

        public override unsafe void Write(Slice changeVector, Stream stream, byte[] tempBuffer, OutgoingReplicationStatsScope stats)
        {
            fixed (byte* pTemp = tempBuffer)
            {
                if (AssertChangeVectorSize() > tempBuffer.Length)
                    ThrowTooManyChangeVectorEntries(this, Key.ToString());

                var tempBufferPos = WriteCommon(changeVector, pTemp);

                *(long*)(pTemp + tempBufferPos) = LastModifiedTicks;
                tempBufferPos += sizeof(long);

                *(int*)(pTemp + tempBufferPos) = Key.Size;
                tempBufferPos += sizeof(int);

                Memory.Copy(pTemp + tempBufferPos, Key.Content.Ptr, Key.Size);
                tempBufferPos += Key.Size;

                stream.Write(tempBuffer, 0, tempBufferPos);

                stats.RecordAttachmentTombstoneOutput();
            }
        }

        public override unsafe void Read(DocumentsOperationContext context, IncomingReplicationStatsScope stats)
        {
            using (stats.For(ReplicationOperation.Incoming.TombstoneRead))
            {
                stats.RecordAttachmentTombstoneRead();
                LastModifiedTicks = *(long*)Reader.ReadExactly(sizeof(long));

                var size = *(int*)Reader.ReadExactly(sizeof(int));
                ToDispose(Slice.From(context.Allocator, Reader.ReadExactly(size), size, ByteStringType.Immutable, out Key));
            }
        }

        protected override ReplicationBatchItem CloneInternal(JsonOperationContext context)
        {
            var item = new AttachmentTombstoneReplicationItem();
            var keyMem = Key.CloneToJsonContext(context, out item.Key);

            item.ToDispose(new DisposableAction(() =>
            {
                context.ReturnMemory(keyMem);
            }));

            return item;
        }

        public override void InnerDispose()
        {
        }
    }
}
