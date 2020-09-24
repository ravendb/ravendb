using System;
using System.IO;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Voron;

namespace Raven.Server.Documents.Replication.ReplicationItems
{
    public class AttachmentReplicationItem : ReplicationBatchItem
    {
        public LazyStringValue Name;
        public LazyStringValue ContentType;
        public Slice Key;
        public Slice Base64Hash;
        public Stream Stream;

        public static unsafe AttachmentReplicationItem From(DocumentsOperationContext context, Attachment attachment)
        {
            var item = new AttachmentReplicationItem
            {
                Type = ReplicationItemType.Attachment,
                Etag = attachment.Etag,
                ChangeVector = attachment.ChangeVector,
                Name = attachment.Name,
                ContentType = attachment.ContentType,
                Base64Hash = attachment.Base64Hash,
                Stream = attachment.Stream,
                TransactionMarker = attachment.TransactionMarker
            };

            // although the key is LSV but is treated as slice and doesn't respect escaping
            item.ToDispose(Slice.From(context.Allocator, attachment.Key.Buffer, attachment.Key.Size, ByteStringType.Immutable, out item.Key));
            return item;
        }

        public override long AssertChangeVectorSize()
        {
            return sizeof(byte) + // type

                   sizeof(int) + // # of change vectors
                   Encodings.Utf8.GetByteCount(ChangeVector) +

                   sizeof(short) + // transaction marker
                   sizeof(int) + // size of ID

                   Key.Size +
                   sizeof(int) + // size of name

                   Name.Size +
                   sizeof(int) + // size of ContentType
                   ContentType.Size +
                   sizeof(byte) + // size of Base64Hash
                   Base64Hash.Size;
        }

        public override long Size => Stream?.Length ?? 0;

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

                *(int*)(pTemp + tempBufferPos) = Name.Size;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, Name.Buffer, Name.Size);
                tempBufferPos += Name.Size;

                *(int*)(pTemp + tempBufferPos) = ContentType.Size;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, ContentType.Buffer, ContentType.Size);
                tempBufferPos += ContentType.Size;

                pTemp[tempBufferPos++] = (byte)Base64Hash.Size;
                Base64Hash.CopyTo(pTemp + tempBufferPos);
                tempBufferPos += Base64Hash.Size;

                stream.Write(tempBuffer, 0, tempBufferPos);
            }
        }


        public override unsafe void Read(DocumentsOperationContext context, IncomingReplicationStatsScope stats)
        {
            using (stats.For(ReplicationOperation.Incoming.AttachmentRead))
            {
                stats.RecordAttachmentRead();

                var size = *(int*)Reader.ReadExactly(sizeof(int));
                ToDispose(Slice.From(context.Allocator, Reader.ReadExactly(size), size, ByteStringType.Immutable, out Key));

                SetLazyStringValueFromString(context, out Name);
                SetLazyStringValueFromString(context, out ContentType);

                var base64HashSize = *Reader.ReadExactly(sizeof(byte));
                ToDispose(Slice.From(context.Allocator, Reader.ReadExactly(base64HashSize), base64HashSize, out Base64Hash));
            }
        }

        protected override ReplicationBatchItem CloneInternal(JsonOperationContext context)
        {
            MemoryStream stream = null;
            if (Stream != null)
            {
                stream = new MemoryStream();
                Stream.CopyTo(stream);

                stream.Position = 0;
                Stream.Position = 0;
            }

            var item = new AttachmentReplicationItem
            {
                Base64Hash = Base64Hash,
                ContentType = ContentType,
                Name = Name,
                Key = Key,
                Stream = stream
            };

            return item;
        }

        public unsafe void ReadStream(DocumentsOperationContext context, StreamsTempFile attachmentStreamsTempFile)
        {
            var base64HashSize = *Reader.ReadExactly(sizeof(byte));
            ToDispose(Slice.From(context.Allocator, Reader.ReadExactly(base64HashSize), base64HashSize, out Base64Hash));

            var streamLength = *(long*)Reader.ReadExactly(sizeof(long));
            Stream = attachmentStreamsTempFile.StartNewStream();
            Reader.ReadExactly(streamLength, Stream);
            Stream.Flush();
        }
        public unsafe void WriteStream(Stream stream, byte[] tempBuffer)
        {
            fixed (byte* pTemp = tempBuffer)
            {
                int tempBufferPos = 0;
                pTemp[tempBufferPos++] = (byte)ReplicationItemType.AttachmentStream;

                // Hash size is 32, but it might be changed in the future
                pTemp[tempBufferPos++] = (byte)Base64Hash.Size;
                Base64Hash.CopyTo(pTemp + tempBufferPos);
                tempBufferPos += Base64Hash.Size;

                *(long*)(pTemp + tempBufferPos) = Stream.Length;
                tempBufferPos += sizeof(long);

                long readPos = 0;
                while (readPos < Stream.Length)
                {
                    var sizeToCopy = (int)Math.Min(Stream.Length - readPos, tempBuffer.Length - tempBufferPos);
                    if (sizeToCopy == 0) // buffer is full, need to flush it
                    {
                        stream.Write(tempBuffer, 0, tempBufferPos);
                        tempBufferPos = 0;
                        continue;
                    }
                    var readCount = Stream.Read(tempBuffer, tempBufferPos, sizeToCopy);
                    tempBufferPos += readCount;
                    readPos += readCount;
                }

                stream.Write(tempBuffer, 0, tempBufferPos);
            }
        }

        public override void InnerDispose()
        {
            Stream?.Dispose();
        }
    }
}
