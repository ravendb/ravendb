#if NETSTANDARD2_0

using System.Buffers;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace System.IO
{
    internal static class StreamExtensions
    {
        /// <summary>
        /// https://github.com/dotnet/runtime/blob/0f829188c6c1ca35a951214f8e9c43f377953b96/src/libraries/System.Private.CoreLib/src/System/IO/Stream.cs#L814-L828
        /// </summary>
        public static int Read(this Stream stream, Span<byte> buffer)
        {
            byte[] sharedBuffer = ArrayPool<byte>.Shared.Rent(buffer.Length);
            try
            {
                int numRead = stream.Read(sharedBuffer, 0, buffer.Length);
                if ((uint)numRead > (uint)buffer.Length)
                {
                    throw new IOException("Stream was too long.");
                }

                new Span<byte>(sharedBuffer, 0, numRead).CopyTo(buffer);
                return numRead;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(sharedBuffer);
            }
        }

        /// <summary>
        /// https://github.com/dotnet/runtime/blob/0f829188c6c1ca35a951214f8e9c43f377953b96/src/libraries/System.IO.Pipelines/src/System/IO/Pipelines/StreamExtensions.netstandard.cs#L18-L43
        /// </summary>
        public static ValueTask<int> ReadAsync(this Stream stream, Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (MemoryMarshal.TryGetArray(buffer, out ArraySegment<byte> array))
            {
                return new ValueTask<int>(stream.ReadAsync(array.Array!, array.Offset, array.Count, cancellationToken));
            }
            else
            {
                byte[] sharedBuffer = ArrayPool<byte>.Shared.Rent(buffer.Length);
                return FinishReadAsync(stream.ReadAsync(sharedBuffer, 0, buffer.Length, cancellationToken), sharedBuffer, buffer);

                static async ValueTask<int> FinishReadAsync(Task<int> readTask, byte[] localBuffer, Memory<byte> localDestination)
                {
                    try
                    {
                        int result = await readTask.ConfigureAwait(false);
                        new Span<byte>(localBuffer, 0, result).CopyTo(localDestination.Span);
                        return result;
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(localBuffer);
                    }
                }
            }
        }

        /// <summary>
        /// https://github.com/dotnet/runtime/blob/0f829188c6c1ca35a951214f8e9c43f377953b96/src/libraries/System.Private.CoreLib/src/System/IO/Stream.cs#L847-L856
        /// </summary>
        public static void Write(this Stream stream, ReadOnlySpan<byte> buffer)
        {
            byte[] sharedBuffer = ArrayPool<byte>.Shared.Rent(buffer.Length);
            try
            {
                buffer.CopyTo(sharedBuffer);
                stream.Write(sharedBuffer, 0, buffer.Length);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(sharedBuffer);
            }
        }

        /// <summary>
        /// https://github.com/dotnet/runtime/blob/0f829188c6c1ca35a951214f8e9c43f377953b96/src/libraries/System.Private.CoreLib/src/System/IO/Stream.cs#L762-L786
        /// </summary>
        public static ValueTask WriteAsync(this Stream stream, ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (MemoryMarshal.TryGetArray(buffer, out ArraySegment<byte> array))
            {
                return new ValueTask(stream.WriteAsync(array.Array!, array.Offset, array.Count, cancellationToken));
            }
            else
            {
                byte[] sharedBuffer = ArrayPool<byte>.Shared.Rent(buffer.Length);
                buffer.Span.CopyTo(sharedBuffer);
                return new ValueTask(FinishWriteAsync(stream.WriteAsync(sharedBuffer, 0, buffer.Length, cancellationToken), sharedBuffer));
                
                static async Task FinishWriteAsync(Task writeTask, byte[] localBuffer)
                {
                    try
                    {
                        await writeTask.ConfigureAwait(false);
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(localBuffer);
                    }
                }
            }
        }
    }
}

#endif
