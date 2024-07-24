using System;
using System.Globalization;
using System.IO;
using Raven.Client.Documents.Attachments;
using Raven.Client.Util;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Voron;

namespace Raven.Server.Documents.Replication.ReplicationItems
{
    public sealed class AttachmentReplicationItem : ReplicationBatchItem
    {
        public LazyStringValue Name;
        public LazyStringValue ContentType;
        public Slice Key;
        public Slice Base64Hash;
        public Stream Stream;
        public long AttachmentSize;
        public AttachmentFlags Flags;
        public DateTime? RetiredAtUtc;

        public override long Size => base.Size + // common

                                     sizeof(int) + // size of ID
                                     Key.Size +

                                     sizeof(int) + // size of name
                                     Name.Size +

                                     sizeof(int) + // size of ContentType
                                     ContentType.Size +

                                     sizeof(byte) + // size of Base64Hash
                                     Base64Hash.Size 
                                     + sizeof(long)
                                     + sizeof(int)
                                     + (RetiredAtUtc == null ? 0 : sizeof(long)); //TODO: egor DateTime is 8 bytes?
      

        public long StreamSize => sizeof(byte) + // type

                                  sizeof(byte) + // size of Base64Hash
                                  Base64Hash.Size +

                                  sizeof(long) + // size of stream
                                  Stream?.Length ?? 0;

        public override DynamicJsonValue ToDebugJson()
        {
            var djv = base.ToDebugJson();
            djv[nameof(Name)] = Name.ToString(CultureInfo.InvariantCulture);
            djv[nameof(ContentType)] = ContentType.ToString(CultureInfo.InvariantCulture);
            djv[nameof(Base64Hash)] = Base64Hash.ToString();
            djv[nameof(Key)] = CompoundKeyHelper.ExtractDocumentId(Key);
            return djv;
        }

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
                TransactionMarker = attachment.TransactionMarker,
                AttachmentSize = attachment.Size,
                Flags = attachment.Flags,
                RetiredAtUtc = attachment.RetiredAt
            };

            // although the key is LSV but is treated as slice and doesn't respect escaping
            item.ToDispose(Slice.From(context.Allocator, attachment.Key.Buffer, attachment.Key.Size, ByteStringType.Immutable, out item.Key));
            return item;
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

                *(long*)(pTemp + tempBufferPos) = AttachmentSize;
                tempBufferPos += sizeof(long);
                if (RetiredAtUtc.HasValue)
                {
                    *(long*)(pTemp + tempBufferPos) = RetiredAtUtc.Value.Ticks;
                    tempBufferPos += sizeof(long);
                }
                else
                {
                    *(long*)(pTemp + tempBufferPos) = -1L;
                    tempBufferPos += sizeof(long);
                }


                *(AttachmentFlags*)(pTemp + tempBufferPos) = Flags;
                tempBufferPos += sizeof(AttachmentFlags);


                stream.Write(tempBuffer, 0, tempBufferPos);
                stats.RecordAttachmentOutput(Size);
            }
        }

        public override unsafe void Read(JsonOperationContext context, ByteStringContext allocator, IncomingReplicationStatsScope stats)
        {
            using (stats.For(ReplicationOperation.Incoming.AttachmentRead))
            {
                var size = *(int*)Reader.ReadExactly(sizeof(int));
                ToDispose(Slice.From(allocator, Reader.ReadExactly(size), size, ByteStringType.Immutable, out Key));

                SetLazyStringValueFromString(context, out Name);
                SetLazyStringValueFromString(context, out ContentType);

                var base64HashSize = *Reader.ReadExactly(sizeof(byte));
                ToDispose(Slice.From(allocator, Reader.ReadExactly(base64HashSize), base64HashSize, out Base64Hash));


                AttachmentSize = *(long*)Reader.ReadExactly(sizeof(long));
                var ticks = *(long*)Reader.ReadExactly(sizeof(long));
                if (ticks != -1)
                    RetiredAtUtc = new DateTime(ticks, DateTimeKind.Utc);

                Flags = *(AttachmentFlags*)Reader.ReadExactly(sizeof(AttachmentFlags)) | AttachmentFlags.None; //TODO: egor do I want the | AttachmentFlags.None?

                stats.RecordAttachmentRead(Size);
            }
        }

        protected override ReplicationBatchItem CloneInternal(JsonOperationContext context, ByteStringContext allocator)
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
                ContentType = ContentType.Clone(context),
                Name = Name.Clone(context),
                Stream = stream
            };

            item.Base64Hash = Base64Hash.Clone(allocator);
            item.Key = Key.Clone(allocator);

            item.AttachmentSize = AttachmentSize;
            item.RetiredAtUtc = RetiredAtUtc;
            item.Flags = Flags;

            item.ToDispose(new DisposableAction(() =>
            {
                item.Base64Hash.Release(allocator);
                item.Key.Release(allocator);
            }));

            return item;
        }

        public unsafe void ReadStream(ByteStringContext allocator, StreamsTempFile attachmentStreamsTempFile, IncomingReplicationStatsScope stats)
        {
            try
            {
                var base64HashSize = *Reader.ReadExactly(sizeof(byte));
                ToDispose(Slice.From(allocator, Reader.ReadExactly(base64HashSize), base64HashSize, out Base64Hash));

                var streamLength = *(long*)Reader.ReadExactly(sizeof(long));
                Stream = attachmentStreamsTempFile.StartNewStream();
                Reader.ReadExactly(streamLength, Stream);
                Stream.Flush();

                stats.RecordAttachmentStreamRead(StreamSize);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to read the stream for attachment with hash: {Base64Hash}", e);
            }
        }

        public unsafe void WriteStream(Stream stream, byte[] tempBuffer, OutgoingReplicationStatsScope stats)
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
                stats.RecordAttachmentStreamOutput(StreamSize);
            }
        }

        protected override void InnerDispose()
        {
            Name?.Dispose();
            ContentType?.Dispose();
            Stream?.Dispose();
        }
    }
}
