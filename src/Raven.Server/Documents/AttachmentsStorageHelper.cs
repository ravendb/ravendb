using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Server;

namespace Raven.Server.Documents
{
    public static class AttachmentsStorageHelper
    {
        public static async Task<string> CopyStreamToFileAndCalculateHash(DocumentsOperationContext context, Stream requestStream, Stream file, CancellationToken cancellationToken)
        {
            using (context.GetManagedBuffer(out JsonOperationContext.ManagedPinnedBuffer buffer))
            using (context.GetManagedBuffer(out JsonOperationContext.ManagedPinnedBuffer cryptoState))
            {
                if(cryptoState.Length < (int)Sodium.crypto_generichash_statebytes())
                    throw new InvalidOperationException("BUG: shouldn't happen, the size of a generic hash state was too large!");

                InitComputeHash(cryptoState);

                var bufferRead = 0;
                while (true)
                {
                    var count = await requestStream.ReadAsync(buffer.Buffer.Array, buffer.Buffer.Offset + bufferRead, buffer.Buffer.Count - bufferRead, cancellationToken);
                    if (count == 0)
                        break;

                    bufferRead += count;

                    if (bufferRead == buffer.Buffer.Count)
                    {
                        PartialComputeHash(cryptoState, buffer, bufferRead);
                        await file.WriteAsync(buffer.Buffer.Array, buffer.Buffer.Offset, bufferRead, cancellationToken);
                        bufferRead = 0;
                    }
                }
                await file.WriteAsync(buffer.Buffer.Array, buffer.Buffer.Offset, bufferRead, cancellationToken);
                PartialComputeHash(cryptoState, buffer, bufferRead);
                var hash = FinalizeGetHash(cryptoState, buffer);
                return hash;
            }
        }

        private static unsafe void InitComputeHash(JsonOperationContext.ManagedPinnedBuffer cryptoState)
        {
            var rc = Sodium.crypto_generichash_init(cryptoState.Pointer, null, UIntPtr.Zero, Sodium.crypto_generichash_bytes());
            if (rc != 0)
                throw new InvalidOperationException("Unable to hash attachment: " + rc);
        }

        private static unsafe string FinalizeGetHash(JsonOperationContext.ManagedPinnedBuffer cryptoState, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            var size = Sodium.crypto_generichash_bytes();
            var rc = Sodium.crypto_generichash_final(cryptoState.Pointer, buffer.Pointer, size);
            if (rc != 0)
                throw new InvalidOperationException("Unable to hash attachment: " + rc);

            return Convert.ToBase64String(buffer.Buffer.Array, buffer.Buffer.Offset, (int)size);
        }

        private static unsafe void PartialComputeHash(JsonOperationContext.ManagedPinnedBuffer cryptoState, JsonOperationContext.ManagedPinnedBuffer buffer, int bufferRead)
        {
            var rc = Sodium.crypto_generichash_update(cryptoState.Pointer,buffer.Pointer,(ulong)bufferRead);
            if(rc !=0)
                throw new InvalidOperationException("Unable to hash attachment: " + rc);
        }
    }
}
