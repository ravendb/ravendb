using System;
using System.Buffers;
using System.IO;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Sparrow.Server.Json.Sync
{
    internal static class JsonOperationContextSyncExtensions
    {
        internal static void Write(this JsonOperationContext.SyncJsonOperationContext syncContext, Stream stream, BlittableJsonReaderObject json)
        {
            syncContext.EnsureNotDisposed();

            using (var writer = new BlittableJsonTextWriter(syncContext.Context, stream))
            {
                writer.WriteObject(json);
            }
        }

        public static BlittableJsonReaderObject ReadForDisk(this JsonOperationContext.SyncJsonOperationContext syncContext, Stream stream, string documentId)
        {
            return ParseToMemory(syncContext, stream, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        }

        public static BlittableJsonReaderObject ReadForMemory(this JsonOperationContext.SyncJsonOperationContext syncContext, Stream stream, string documentId)
        {
            return ParseToMemory(syncContext, stream, documentId, BlittableJsonDocumentBuilder.UsageMode.None);
        }

        public static unsafe BlittableJsonReaderObject ReadForMemory(this JsonOperationContext.SyncJsonOperationContext syncContext, string jsonString, string documentId)
        {
            // todo: maybe use ManagedPinnedBuffer here
            var maxByteSize = Encodings.Utf8.GetMaxByteCount(jsonString.Length);

            fixed (char* val = jsonString)
            {
                var buffer = ArrayPool<byte>.Shared.Rent(maxByteSize);
                try
                {
                    fixed (byte* buf = buffer)
                    {
                        Encodings.Utf8.GetBytes(val, jsonString.Length, buf, maxByteSize);
                        using (var ms = new MemoryStream(buffer))
                        {
                            return ReadForMemory(syncContext, ms, documentId);
                        }
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }
        }

        private static BlittableJsonReaderObject ParseToMemory(JsonOperationContext.SyncJsonOperationContext syncContext, Stream stream, string debugTag, BlittableJsonDocumentBuilder.UsageMode mode, IBlittableDocumentModifier modifier = null)
        {
            using (syncContext.Context.GetMemoryBuffer(out var bytes))
                return ParseToMemory(syncContext, stream, debugTag, mode, bytes, modifier);
        }

        public static BlittableJsonReaderObject ParseToMemory(
            this JsonOperationContext.SyncJsonOperationContext syncContext,
            Stream stream,
            string debugTag,
            BlittableJsonDocumentBuilder.UsageMode mode,
            JsonOperationContext.MemoryBuffer bytes,
            IBlittableDocumentModifier modifier = null)
        {
            syncContext.EnsureNotDisposed();

            syncContext.JsonParserState.Reset();
            using (var parser = new UnmanagedJsonParser(syncContext.Context, syncContext.JsonParserState, debugTag))
            using (var builder = new BlittableJsonDocumentBuilder(syncContext.Context, mode, debugTag, parser, syncContext.JsonParserState, modifier: modifier))
            {
                syncContext.Context.CachedProperties.NewDocument();
                builder.ReadObjectDocument();
                while (true)
                {
                    if (bytes.Valid == bytes.Used)
                    {
                        var read = stream.Read(bytes.Memory.Memory.Span);
                        syncContext.EnsureNotDisposed();
                        if (read == 0)
                            throw new EndOfStreamException("Stream ended without reaching end of json content");
                        bytes.Valid = read;
                        bytes.Used = 0;
                    }
                    parser.SetBuffer(bytes);
                    var result = builder.Read();
                    bytes.Used += parser.BufferOffset;
                    if (result)
                        break;
                }
                builder.FinalizeDocument();

                var reader = builder.CreateReader();
                return reader;
            }
        }
    }
}
