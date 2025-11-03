using System;
using System.Globalization;
using System.IO;
using Raven.Client.Documents.Attachments;
using Raven.Client.ServerWide.Tcp;
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
        public RemoteAttachmentFlags Flags;
        public DateTime? RemoteAtUtc;
        public Slice RemoteIdentifier;

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
                                     + sizeof(int) // size of AttachmentSize
                                     + (RemoteAtUtc == null ? 0 : sizeof(long)) // size of RemoteAtUtc
                                     + sizeof(int) // size of Flags
                                     + sizeof(int) + //  size of RemoteIdentifier
                                     + RemoteIdentifier.Size;

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
            djv[nameof(Flags)] = Flags.ToString();
            djv[nameof(RemoteAtUtc)] = RemoteAtUtc;
            djv[nameof(RemoteIdentifier)] = RemoteIdentifier.ToString();
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
            };

            if (attachment.RemoteParameters != null)
            {
                item.Flags = attachment.RemoteParameters.Flags;
                item.RemoteAtUtc = attachment.RemoteParameters.At;
                item.ToDispose(Slice.From(context.Allocator, attachment.RemoteParameters.Identifier, ByteStringType.Immutable, out item.RemoteIdentifier));
            }
            else
            {
                item.Flags = RemoteAttachmentFlags.None;
                item.RemoteAtUtc = null;
                item.RemoteIdentifier = Slices.Empty;
            }

            // although the key is LSV but is treated as slice and doesn't respect escaping
            item.ToDispose(Slice.From(context.Allocator, attachment.Key.Buffer, attachment.Key.Size, ByteStringType.Immutable, out item.Key));
            return item;
        }

        public override long AssertChangeVectorSize() => Size;

        public override unsafe void Write(Slice changeVector, Stream stream, byte[] tempBuffer, OutgoingReplicationStatsScope stats,
            TcpConnectionHeaderMessage.SupportedFeatures.ReplicationFeatures supportedFeaturesReplication)
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

                tempBufferPos = WriteRemoteAttachmentsProperties(supportedFeaturesReplication, pTemp, tempBufferPos);

                stream.Write(tempBuffer, 0, tempBufferPos);
                stats.RecordAttachmentOutput(Size);
            }
        }

        private unsafe int WriteRemoteAttachmentsProperties(TcpConnectionHeaderMessage.SupportedFeatures.ReplicationFeatures supportedFeaturesReplication, byte* pTemp, int tempBufferPos)
        {
            if (supportedFeaturesReplication.RemoteAttachments == false)
                return tempBufferPos;

            *(long*)(pTemp + tempBufferPos) = AttachmentSize;
            tempBufferPos += sizeof(long);
            if (RemoteAtUtc.HasValue)
            {
                *(long*)(pTemp + tempBufferPos) = RemoteAtUtc.Value.Ticks;
                tempBufferPos += sizeof(long);
            }
            else
            {
                *(long*)(pTemp + tempBufferPos) = -1L;
                tempBufferPos += sizeof(long);
            }

            *(RemoteAttachmentFlags*)(pTemp + tempBufferPos) = Flags;
            tempBufferPos += sizeof(RemoteAttachmentFlags);

            *(int*)(pTemp + tempBufferPos) = RemoteIdentifier.Size;
            tempBufferPos += sizeof(int);
            if (RemoteIdentifier.Size != 0)
            {
                Memory.Copy(pTemp + tempBufferPos, RemoteIdentifier.Content.Ptr, RemoteIdentifier.Size);
                tempBufferPos += RemoteIdentifier.Size;
            }

            return tempBufferPos;
        }

        public override unsafe void Read(JsonOperationContext context, ByteStringContext allocator, IncomingReplicationStatsScope stats,
            TcpConnectionHeaderMessage.SupportedFeatures.ReplicationFeatures supportedFeaturesReplication)
        {
            using (stats.For(ReplicationOperation.Incoming.AttachmentRead))
            {
                var size = *(int*)Reader.ReadExactly(sizeof(int));
                ToDispose(Slice.From(allocator, Reader.ReadExactly(size), size, ByteStringType.Immutable, out Key));

                SetLazyStringValueFromString(context, out Name);
                SetLazyStringValueFromString(context, out ContentType);

                var base64HashSize = *Reader.ReadExactly(sizeof(byte));
                ToDispose(Slice.From(allocator, Reader.ReadExactly(base64HashSize), base64HashSize, out Base64Hash));

                if (supportedFeaturesReplication.RemoteAttachments)
                {
                    AttachmentSize = *(long*)Reader.ReadExactly(sizeof(long));
                    var ticks = *(long*)Reader.ReadExactly(sizeof(long));
                    if (ticks != -1)
                        RemoteAtUtc = new DateTime(ticks, DateTimeKind.Utc);

                    Flags = *(RemoteAttachmentFlags*)Reader.ReadExactly(sizeof(RemoteAttachmentFlags)) | RemoteAttachmentFlags.None;
                    size = *(int*)Reader.ReadExactly(sizeof(int));

                    if (size == 0)
                    {
                        RemoteIdentifier = Slices.Empty;
                    }
                    else
                    {
                        ToDispose(Slice.From(allocator, Reader.ReadExactly(size), size, ByteStringType.Immutable, out RemoteIdentifier));
                    }
                }
                else
                {
                    Flags = RemoteAttachmentFlags.None;
                    RemoteIdentifier = Slices.Empty;
                }

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
            item.RemoteAtUtc = RemoteAtUtc;
            item.Flags = Flags;
            item.RemoteIdentifier = RemoteIdentifier.Clone(allocator);

            item.ToDispose(new DisposableAction(() =>
            {
                item.Base64Hash.Release(allocator);
                item.Key.Release(allocator);
                item.RemoteIdentifier.Release(allocator);
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
                AttachmentSize = streamLength; // we populate the stream size here so we can use it in PreProcessAttachments method, when receiving replication from old versions, without remote attachments support
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
