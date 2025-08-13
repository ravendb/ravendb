using System.IO;
using Sparrow.Json;
using Sparrow.Json.Sync;

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

        public static BlittableJsonReaderObject ReadForDisk(this JsonOperationContext.SyncJsonOperationContext syncContext, Stream stream, string documentId) => 
            JsonOperationSyncExtensions.ParseToMemory(syncContext, stream, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);

        public static BlittableJsonReaderObject ReadForMemory(this JsonOperationContext.SyncJsonOperationContext syncContext, Stream stream, string documentId) => 
            JsonOperationSyncExtensions.ParseToMemory(syncContext, stream, documentId, BlittableJsonDocumentBuilder.UsageMode.None);

        public static BlittableJsonReaderObject ReadForMemory(this JsonOperationContext.SyncJsonOperationContext syncContext, string jsonString, string documentId)
            => JsonOperationSyncExtensions.ReadForMemory(syncContext, jsonString, documentId);
       
        public static BlittableJsonReaderObject ParseToMemory(
            this JsonOperationContext.SyncJsonOperationContext syncContext,
            Stream stream,
            string debugTag,
            BlittableJsonDocumentBuilder.UsageMode mode,
            JsonOperationContext.MemoryBuffer bytes,
            IBlittableDocumentModifier modifier = null)
            => JsonOperationSyncExtensions.ParseToMemory(syncContext, stream, debugTag, mode, bytes, modifier);
    }
}
