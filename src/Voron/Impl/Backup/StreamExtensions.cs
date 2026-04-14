using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Voron.Global;

namespace Voron.Impl.Backup
{
    public static class StreamExtensions
    {
        private const int DefaultBufferSize = 81920;

        /// <summary>
        /// Minimum number of contiguous zero pages required to create a sparse hole.
        /// Matches the threshold in FreeSpaceHandling.GetSparseRegions (128 pages = 1 MB).
        /// </summary>
        internal const int MinContiguousZeroPagesForSparse = 128;

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
        /// Copies from source to destination while detecting contiguous zero-page regions
        /// and skipping them, creating a sparse file. The destination file must already be
        /// marked as sparse on Windows (via SparseFileHelper.TryMarkFileAsSparse).
        /// </summary>
        /// <remarks>
        /// Zero regions shorter than MinContiguousZeroPagesForSparse pages (1 MB) are written
        /// normally to avoid creating fragmented sparse holes with negligible benefit.
        /// This threshold matches the one used by Voron's flush pipeline in FreeSpaceHandling.GetSparseRegions.
        /// </remarks>
        public static void CopyToSparse(this Stream source, Stream destination, Action<int> onProgress, CancellationToken cancellationToken)
        {
            int pageSize = Constants.Storage.PageSize;
            int minZeroPagesForHole = MinContiguousZeroPagesForSparse;

            // Use a buffer that is a multiple of page size for aligned zero-detection.
            // DefaultBufferSize (81920) = 10 pages of 8192 bytes each.
            int bufferSize = DefaultBufferSize;
            var readBuffer = ArrayPool<byte>.Shared.Rent(bufferSize);

            try
            {
                long logicalPosition = 0;     // tracks the logical position in the output stream
                long destinationPosition = 0; // tracks where destination stream is actually positioned
                int contiguousZeroPages = 0;  // accumulated count of consecutive zero pages across buffer boundaries

                int count;
                while ((count = source.Read(readBuffer, 0, bufferSize)) != 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    onProgress?.Invoke(count);

                    int fullPages = count / pageSize;
                    int remainder = count - (fullPages * pageSize);

                    // Process full pages - detect zero pages and batch writes
                    int pendingWriteStart = -1; // start offset within buffer of pending non-zero data

                    for (int i = 0; i < fullPages; i++)
                    {
                        int pageOffset = i * pageSize;
                        bool isZeroPage = new ReadOnlySpan<byte>(readBuffer, pageOffset, pageSize).ContainsAnyExcept((byte)0) == false;

                        if (isZeroPage)
                        {
                            // Flush any pending non-zero data before accumulating zeros
                            if (pendingWriteStart >= 0)
                            {
                                int writeLength = pageOffset - pendingWriteStart;
                                FlushPendingWrite(destination, readBuffer, pendingWriteStart, writeLength,
                                    logicalPosition - (pageOffset - pendingWriteStart), ref destinationPosition);
                                pendingWriteStart = -1;
                            }

                            contiguousZeroPages++;
                        }
                        else
                        {
                            // Non-zero page encountered - check if we need to flush accumulated zeros
                            if (contiguousZeroPages > 0)
                            {
                                if (contiguousZeroPages < minZeroPagesForHole)
                                {
                                    // Small zero run - write it (not worth creating a sparse hole)
                                    long zeroStart = logicalPosition - ((long)contiguousZeroPages * pageSize);
                                    WriteZeros(destination, zeroStart, (long)contiguousZeroPages * pageSize, ref destinationPosition);
                                }
                                // else: large zero run - skip it (creates a sparse hole)

                                contiguousZeroPages = 0;
                            }

                            if (pendingWriteStart < 0)
                                pendingWriteStart = pageOffset;
                        }

                        logicalPosition += pageSize;
                    }

                    // Flush any pending non-zero writes from this buffer
                    if (pendingWriteStart >= 0)
                    {
                        int writeEnd = fullPages * pageSize;
                        int writeLength = writeEnd - pendingWriteStart;
                        FlushPendingWrite(destination, readBuffer, pendingWriteStart, writeLength,
                            logicalPosition - writeLength, ref destinationPosition);
                        pendingWriteStart = -1;
                    }

                    // Handle remainder (partial page at end of buffer or end of stream)
                    // Write partial pages normally without zero-detection
                    if (remainder > 0)
                    {
                        // First, flush any accumulated zeros before the remainder
                        if (contiguousZeroPages > 0)
                        {
                            if (contiguousZeroPages < minZeroPagesForHole)
                            {
                                long zeroStart = logicalPosition - ((long)contiguousZeroPages * pageSize);
                                WriteZeros(destination, zeroStart, (long)contiguousZeroPages * pageSize, ref destinationPosition);
                            }
                            contiguousZeroPages = 0;
                        }

                        int remainderOffset = fullPages * pageSize;
                        FlushPendingWrite(destination, readBuffer, remainderOffset, remainder,
                            logicalPosition, ref destinationPosition);
                        logicalPosition += remainder;
                    }
                }

                // After all data is read, flush any trailing zeros below threshold
                if (contiguousZeroPages > 0 && contiguousZeroPages < minZeroPagesForHole)
                {
                    long zeroStart = logicalPosition - ((long)contiguousZeroPages * pageSize);
                    WriteZeros(destination, zeroStart, (long)contiguousZeroPages * pageSize, ref destinationPosition);
                }

                // Set the correct file length (the file may end with a sparse hole)
                if (logicalPosition > 0)
                {
                    destination.SetLength(logicalPosition);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(readBuffer);
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

        private static void WriteZeros(Stream destination, long logicalOffset, long length, ref long destinationPosition)
        {
            // For small zero runs that don't meet the sparse threshold, we need to actually write them.
            // Seek to position and write zero bytes.
            if (destinationPosition != logicalOffset)
            {
                destination.Seek(logicalOffset, SeekOrigin.Begin);
                destinationPosition = logicalOffset;
            }

            // Write zeros in chunks using a stack-allocated or pooled buffer
            var zeroBuffer = ArrayPool<byte>.Shared.Rent(DefaultBufferSize);
            try
            {
                Array.Clear(zeroBuffer, 0, DefaultBufferSize);
                long remaining = length;
                while (remaining > 0)
                {
                    int toWrite = (int)Math.Min(remaining, DefaultBufferSize);
                    destination.Write(zeroBuffer, 0, toWrite);
                    remaining -= toWrite;
                }

                destinationPosition += length;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(zeroBuffer);
            }
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
