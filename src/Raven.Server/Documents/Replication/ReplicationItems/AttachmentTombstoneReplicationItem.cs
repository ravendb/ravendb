using System.IO;
using Raven.Client.Util;
using Raven.Server.Documents.Replication.Stats;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Voron;

namespace Raven.Server.Documents.Replication.ReplicationItems
{
    public sealed class AttachmentTombstoneReplicationItem : ReplicationBatchItem
    {
        public Slice Key;
        public DocumentFlags Flags;
        public AttachmentTombstoneFlags TombstoneFlags;

        public override long Size => base.Size + // common 
                                     sizeof(long) + // Last modified ticks
                                     sizeof(int) + // size of key
                                     Key.Size +
                                     sizeof(AttachmentTombstoneFlags);

        public override DynamicJsonValue ToDebugJson()
        {
            var djv = base.ToDebugJson();
            djv[nameof(Key)] = CompoundKeyHelper.ExtractDocumentId(Key);
            djv[nameof(Flags)] = Flags;
            djv[nameof(TombstoneFlags)] = TombstoneFlags;
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

                *(long*)(pTemp + tempBufferPos) = LastModifiedTicks;
                tempBufferPos += sizeof(long);

                *(int*)(pTemp + tempBufferPos) = Key.Size;
                tempBufferPos += sizeof(int);

                Memory.Copy(pTemp + tempBufferPos, Key.Content.Ptr, Key.Size);
                tempBufferPos += Key.Size;

                *(AttachmentTombstoneFlags*)(pTemp + tempBufferPos) = TombstoneFlags;
                tempBufferPos += sizeof(AttachmentTombstoneFlags);

                stream.Write(tempBuffer, 0, tempBufferPos);

                stats.RecordAttachmentTombstoneOutput(Size);
            }
        }

        public override unsafe void Read(JsonOperationContext context, ByteStringContext allocator, IncomingReplicationStatsScope stats)
        {
            using (stats.For(ReplicationOperation.Incoming.TombstoneRead))
            {
                LastModifiedTicks = *(long*)Reader.ReadExactly(sizeof(long));

                var size = *(int*)Reader.ReadExactly(sizeof(int));
                ToDispose(Slice.From(allocator, Reader.ReadExactly(size), size, ByteStringType.Immutable, out Key));

                TombstoneFlags = *(AttachmentTombstoneFlags*)Reader.ReadExactly(sizeof(AttachmentTombstoneFlags)) | AttachmentTombstoneFlags.None;
                stats.RecordAttachmentTombstoneRead(Size);
            }
        }

        protected override ReplicationBatchItem CloneInternal(JsonOperationContext context, ByteStringContext allocator)
        {
            var item = new AttachmentTombstoneReplicationItem();
            item.Key = Key.Clone(allocator);
            item.TombstoneFlags = TombstoneFlags;
            item.ToDispose(new DisposableAction(() =>
            {
                item.Key.Release(allocator);
            }));

            return item;
        }

        protected override void InnerDispose()
        {
        }
    }
}
