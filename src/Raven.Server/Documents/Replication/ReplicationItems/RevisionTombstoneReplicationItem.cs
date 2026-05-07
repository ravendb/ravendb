using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using Raven.Client;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;

namespace Raven.Server.Documents.Replication.ReplicationItems
{
    public sealed class RevisionTombstoneReplicationItem : ReplicationBatchItem
    {
        public LazyStringValue Collection;
        public LazyStringValue Id;
        public DocumentFlags Flags;

        public override long Size => base.Size + // common

                                     sizeof(long) + // last modified

                                     sizeof(int) + // size of key
                                     Id.Size +

                                     sizeof(int) + // size of collection
                                     Collection.Size;

        public override DynamicJsonValue ToDebugJson()
        {
            var djv = base.ToDebugJson();
            djv[nameof(Collection)] = Collection?.ToString(CultureInfo.InvariantCulture) ?? Constants.Documents.Collections.EmptyCollection;
            djv[nameof(Id)] = Id.ToString(CultureInfo.InvariantCulture);
            return djv;
        }

        public override long AssertChangeVectorSize() => Size;

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

                stats.RecordRevisionTombstoneOutput(Size);
            }
        }

        public override unsafe void Read(JsonOperationContext context, ByteStringContext allocator, IncomingReplicationStatsScope stats)
        {
            using (stats.For(ReplicationOperation.Incoming.TombstoneRead))
            {
                LastModifiedTicks = *(long*)Reader.ReadExactly(sizeof(long));

                SetLazyStringValue(context, ref Id);
                SetLazyStringValueFromString(context, out Collection);
                Debug.Assert(Collection != null);

                stats.RecordRevisionTombstoneRead(Size);
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

        public static void TryExtractDocumentId(LazyStringValue key, out string docId)
        {
            var index = key.IndexOf((char)SpecialChars.RecordSeparator, StringComparison.OrdinalIgnoreCase);
            docId = index == -1 ? null : key.Substring(0, index);
        }

        public static void TryExtractDocumentIdAndRevisionKey(DocumentsOperationContext context, LazyStringValue key, out string docId, out string revisionKey)
        {
            var index = key.IndexOf((char)SpecialChars.RecordSeparator, StringComparison.OrdinalIgnoreCase);
            if (index == -1)
            {
                docId = null;
                revisionKey = key;
            }
            else
            {
                docId = key.Substring(0, index);
                revisionKey = key.Substring(index + 1);
            }

            // TODO: backward comp. the real hash should be less than 26 chars (less than the smallest change vector "A:1-<22chars>")
            if (revisionKey.Length != 32)
            {
                revisionKey = RevisionsStorage.GetRevisionKey(context, revisionKey);
            }
        }

        protected override void InnerDispose()
        {
            Id?.Dispose();
            Collection?.Dispose();
        }
    }
}
