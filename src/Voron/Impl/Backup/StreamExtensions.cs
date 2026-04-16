using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Voron.Global;
using Voron.Impl.FreeSpace;

namespace Voron.Impl.Backup
{
    public static class StreamExtensions
    {
        private const int DefaultBufferSize = 81920;

        public static void CopyTo(this Stream source, Stream destination, Action<int> onProgress, CancellationToken cancellationToken)
        {
            var readBuffer = ArrayPool<byte>.Shared.Rent(DefaultBufferSize);

            try
            {
                int count;
                while ((count = source.Read(readBuffer, 0, DefaultBufferSize)) != 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    onProgress?.Invoke(count);
                    destination.Write(readBuffer, 0, count);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(readBuffer);
            }
        }

        /// <summary>
        /// Copies from source to destination while detecting contiguous zero-page regions and skipping them to create a sparse file.
        /// The destination file must already be marked as sparse on Windows (via <see cref="SparseFileHelper.TryMarkFileAsSparse"/>).
        /// Zero runs shorter than <see cref="FreeSpaceHandling.NumberOfFreePagesForSparseConsideration"/> pages are written normally.
        /// </summary>
        public static void CopyToPreservingSparseRegions(this Stream source, Stream destination, Action<int> onProgress, CancellationToken cancellationToken)
        {
            int pageSize = Constants.Storage.PageSize;
            int bufferSize = DefaultBufferSize;

            var readBuffer = ArrayPool<byte>.Shared.Rent(bufferSize);
            var zeroBuffer = ArrayPool<byte>.Shared.Rent(DefaultBufferSize);

            try
            {
                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                Debug.Assert(bufferSize / pageSize < FreeSpaceHandling.NumberOfFreePagesForSparseConsideration,
                    $"Below code assumes buffer holds fewer pages ({bufferSize / pageSize}) than the sparse threshold ({FreeSpaceHandling.NumberOfFreePagesForSparseConsideration})");

                Array.Clear(zeroBuffer, 0, DefaultBufferSize);

                long logicalPosition = 0;
                long destinationPosition = 0;
                int contiguousZeroPages = 0;
                int subPageTail = 0;
                int subPageTailOffset = 0;

                int count;
                while ((count = source.ReadAtLeast(readBuffer.AsSpan(0, bufferSize), bufferSize, throwOnEndOfStream: false)) > 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    onProgress?.Invoke(count);

                    if (count < bufferSize)
                    {
                        // Final read — strip any sub-page tail so all code below works with page-aligned data only.
                        // The tail will be written after the loop.
                        subPageTail = count % pageSize;
                        subPageTailOffset = count - subPageTail;
                        count -= subPageTail;

                        if (count == 0)
                            break;
                    }

                    var span = new ReadOnlySpan<byte>(readBuffer, 0, count);

                    // Fast path: first and last bytes non-zero means no sparse region can start/end in this buffer
                    if (span[0] != 0 && span[count - 1] != 0)
                    {
                        if (contiguousZeroPages > 0)
                            HandleContiguousZeroPages(destination, zeroBuffer, ref contiguousZeroPages, logicalPosition, pageSize, ref destinationPosition);

                        FlushPendingWrite(destination, readBuffer, 0, count, logicalPosition, ref destinationPosition);
                        logicalPosition += count;
                        continue;
                    }

                    int firstNonZero = span.IndexOfAnyExcept((byte)0);
                    bool isEntireBufferZero = firstNonZero == -1;

                    if (isEntireBufferZero)
                    {
                        contiguousZeroPages += count / pageSize;
                        logicalPosition += count;
                        continue;
                    }

                    int lastNonZero = span.LastIndexOfAnyExcept((byte)0);

                    int leadingZeroPages = firstNonZero / pageSize;
                    contiguousZeroPages += leadingZeroPages;
                    logicalPosition += (long)leadingZeroPages * pageSize;

                    if (contiguousZeroPages > 0)
                        HandleContiguousZeroPages(destination, zeroBuffer, ref contiguousZeroPages, logicalPosition, pageSize, ref destinationPosition);

                    int contentStart = leadingZeroPages * pageSize;
                    int contentEnd = ((lastNonZero / pageSize) + 1) * pageSize;
                    int contentLength = contentEnd - contentStart;

                    FlushPendingWrite(destination, readBuffer, contentStart, contentLength, logicalPosition, ref destinationPosition);
                    logicalPosition += contentLength;

                    int trailingZeroPages = (count - contentEnd) / pageSize;
                    contiguousZeroPages = trailingZeroPages;
                    logicalPosition += (long)trailingZeroPages * pageSize;
                }

                if (contiguousZeroPages > 0)
                    HandleContiguousZeroPages(destination, zeroBuffer, ref contiguousZeroPages, logicalPosition, pageSize, ref destinationPosition);

                if (subPageTail > 0)
                {
                    FlushPendingWrite(destination, readBuffer, subPageTailOffset, subPageTail, logicalPosition, ref destinationPosition);
                    logicalPosition += subPageTail;
                }

                if (logicalPosition > 0)
                    destination.SetLength(logicalPosition);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(readBuffer);
                ArrayPool<byte>.Shared.Return(zeroBuffer);
            }
        }

        private static void FlushPendingWrite(Stream destination, byte[] buffer, int offset, int length,
            long logicalOffset, ref long destinationPosition)
        {
            if (destinationPosition != logicalOffset)
            {
                destination.Seek(logicalOffset, SeekOrigin.Begin);
                destinationPosition = logicalOffset;
            }

            destination.Write(buffer, offset, length);
            destinationPosition += length;
        }

        private static void HandleContiguousZeroPages(Stream destination, byte[] zeroBuffer, ref int contiguousZeroPages, long logicalPosition, int pageSize, ref long destinationPosition)
        {
            if (contiguousZeroPages is > 0 and < FreeSpaceHandling.NumberOfFreePagesForSparseConsideration)
            {
                long zeroStart = logicalPosition - ((long)contiguousZeroPages * pageSize);
                WriteZeros(destination, zeroBuffer, zeroStart, (long)contiguousZeroPages * pageSize, ref destinationPosition);
            }

            contiguousZeroPages = 0;
        }

        private static void WriteZeros(Stream destination, byte[] zeroBuffer, long logicalOffset, long length, ref long destinationPosition)
        {
            if (destinationPosition != logicalOffset)
            {
                destination.Seek(logicalOffset, SeekOrigin.Begin);
                destinationPosition = logicalOffset;
            }

            long remaining = length;
            while (remaining > 0)
            {
                int toWrite = (int)Math.Min(remaining, DefaultBufferSize);
                destination.Write(zeroBuffer, 0, toWrite);
                remaining -= toWrite;
            }

            destinationPosition += length;
        }

        public static async Task CopyToAsync(this Stream source, Stream destination, Action<int> onProgress, CancellationToken cancellationToken)
        {
            var readBuffer = ArrayPool<byte>.Shared.Rent(DefaultBufferSize);

            try
            {
                int count;
                while ((count = await source.ReadAsync(readBuffer, 0, DefaultBufferSize, cancellationToken)) != 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    onProgress?.Invoke(count);
                    await destination.WriteAsync(readBuffer, 0, count, cancellationToken);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(readBuffer);
            }
        }
    }
}
