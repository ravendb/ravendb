using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents
{
    public static class AttachmentsStorageHelper
    {
        public static async Task<string> CopyStreamToFileAndCalculateHash(DocumentsOperationContext context, Stream requestStream, Stream file, CancellationToken cancellationToken)
        {
            using (context.GetManagedBuffer(out JsonOperationContext.ManagedPinnedBuffer buffer))
            {
                var metroCtx = Hashing.Streamed.Metro128.BeginProcess();
                var xxhas64Ctx = Hashing.Streamed.XXHash64.BeginProcess();
                var bufferRead = 0;
                while (true)
                {
                    var count = await requestStream.ReadAsync(buffer.Buffer.Array, buffer.Buffer.Offset + bufferRead, buffer.Buffer.Count - bufferRead, cancellationToken);
                    if (count == 0)
                        break;

                    bufferRead += count;

                    if (bufferRead == buffer.Buffer.Count)
                    {
                        PartialComputeHash(metroCtx, xxhas64Ctx, buffer, bufferRead);
                        await file.WriteAsync(buffer.Buffer.Array, buffer.Buffer.Offset, bufferRead, cancellationToken);
                        bufferRead = 0;
                    }
                }
                await file.WriteAsync(buffer.Buffer.Array, buffer.Buffer.Offset, bufferRead, cancellationToken);
                file.Position = 0;
                PartialComputeHash(metroCtx, xxhas64Ctx, buffer, bufferRead);
                var hash = FinalizeGetHash(metroCtx, xxhas64Ctx);
                return hash;
            }
        }

        private static unsafe string FinalizeGetHash(Hashing.Streamed.Metro128Context metroCtx, Hashing.Streamed.XXHash64Context xxhas64Ctx)
        {
            var metro128Hash = Hashing.Streamed.Metro128.EndProcess(metroCtx);
            var xxHash64 = Hashing.Streamed.XXHash64.EndProcess(xxhas64Ctx);

            var hash = new byte[sizeof(ulong) * 3];
            fixed (byte* pHash = hash)
            {
                var longs = (ulong*)pHash;
                longs[0] = metro128Hash.H1;
                longs[1] = metro128Hash.H2;
                longs[2] = xxHash64;
            }
            return Convert.ToBase64String(hash);
        }

        private static unsafe void PartialComputeHash(Hashing.Streamed.Metro128Context metroCtx, Hashing.Streamed.XXHash64Context xxHash64Context, JsonOperationContext.ManagedPinnedBuffer buffer, int bufferRead)
        {
            Hashing.Streamed.Metro128.Process(metroCtx, buffer.Pointer, bufferRead);
            Hashing.Streamed.XXHash64.Process(xxHash64Context, buffer.Pointer, bufferRead);
        }
    }
}