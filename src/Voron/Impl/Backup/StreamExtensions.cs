using System;
using System.Buffers;
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
        /// Copies from source to destination while detecting contiguous zero-page regions
        /// and skipping them, creating a sparse file. The destination file must already be
        /// marked as sparse on Windows (via SparseFileHelper.TryMarkFileAsSparse).
        /// </summary>
        /// <remarks>
        /// Zero regions shorter than FreeSpaceHandling.NumberOfFreePagesForSparseConsideration pages (1 MB) are written
        /// normally to avoid creating fragmented sparse holes with negligible benefit.
        /// This threshold matches the one used by Voron's flush pipeline in FreeSpaceHandling.GetSparseRegions.
        /// </remarks>
        public static void CopyToPreservingSparseRegions(this Stream source, Stream destination, Action<int> onProgress, CancellationToken cancellationToken)
        {
            int pageSize = Constants.Storage.PageSize;
            int minZeroPagesForHole = FreeSpaceHandling.NumberOfFreePagesForSparseConsideration;

            // Use a buffer that is a multiple of page size for aligned zero-detection.
            // DefaultBufferSize (81920) = 10 pages of 8192 bytes each.
            int bufferSize = DefaultBufferSize;
            var readBuffer = ArrayPool<byte>.Shared.Rent(bufferSize);
            var zeroBuffer = ArrayPool<byte>.Shared.Rent(DefaultBufferSize);
            var pageBuffer = ArrayPool<byte>.Shared.Rent(pageSize);

            try
            {
                Array.Clear(zeroBuffer, 0, DefaultBufferSize);

                long logicalPosition = 0;     // tracks the logical position in the output stream
                long destinationPosition = 0; // tracks where destination stream is actually positioned
                int contiguousZeroPages = 0;  // accumulated count of consecutive zero pages across buffer boundaries
                int partialPageBytes = 0;     // accumulated bytes of a page that spans multiple reads

                int count;
                while ((count = source.Read(readBuffer, 0, bufferSize)) != 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    onProgress?.Invoke(count);

                    int offset = 0;

                    if (partialPageBytes > 0)
                    {
                        int bytesNeeded = pageSize - partialPageBytes;
                        int bytesToCopy = Math.Min(bytesNeeded, count);
                        Buffer.BlockCopy(readBuffer, 0, pageBuffer, partialPageBytes, bytesToCopy);

                        partialPageBytes += bytesToCopy;
                        offset += bytesToCopy;

                        if (partialPageBytes == pageSize)
                        {
                            ProcessPage(destination, pageBuffer, 0, pageSize, ref contiguousZeroPages, minZeroPagesForHole,
                                zeroBuffer, ref logicalPosition, ref destinationPosition);
                            partialPageBytes = 0;
                        }
                    }

                    int remaining = count - offset;
                    int fullPages = remaining / pageSize;

                    if (fullPages > 0)
                    {
                        // Process full pages - detect zero pages and batch writes
                        int pendingWriteStart = -1; // start offset within buffer of pending non-zero data

                        for (int i = 0; i < fullPages; i++)
                        {
                            int pageOffset = offset + (i * pageSize);
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
                                        WriteZeros(destination, zeroBuffer, zeroStart, (long)contiguousZeroPages * pageSize, ref destinationPosition);
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
                            int writeEnd = offset + (fullPages * pageSize);
                            int writeLength = writeEnd - pendingWriteStart;
                            FlushPendingWrite(destination, readBuffer, pendingWriteStart, writeLength,
                                logicalPosition - writeLength, ref destinationPosition);
                        }
                    }

                    int remainderOffset = offset + (fullPages * pageSize);
                    int remainder = count - remainderOffset;
                    if (remainder > 0)
                    {
                        Buffer.BlockCopy(readBuffer, remainderOffset, pageBuffer, partialPageBytes, remainder);
                        partialPageBytes += remainder;
                    }
                }

                if (partialPageBytes > 0)
                {
                    if (contiguousZeroPages > 0)
                    {
                        if (contiguousZeroPages < minZeroPagesForHole)
                        {
                            long zeroStart = logicalPosition - ((long)contiguousZeroPages * pageSize);
                            WriteZeros(destination, zeroBuffer, zeroStart, (long)contiguousZeroPages * pageSize, ref destinationPosition);
                        }

                        contiguousZeroPages = 0;
                    }

                    FlushPendingWrite(destination, pageBuffer, 0, partialPageBytes, logicalPosition, ref destinationPosition);
                    logicalPosition += partialPageBytes;
                }

                // After all data is read, flush any trailing zeros below threshold
                if (contiguousZeroPages > 0 && contiguousZeroPages < minZeroPagesForHole)
                {
                    long zeroStart = logicalPosition - ((long)contiguousZeroPages * pageSize);
                    WriteZeros(destination, zeroBuffer, zeroStart, (long)contiguousZeroPages * pageSize, ref destinationPosition);
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
                ArrayPool<byte>.Shared.Return(zeroBuffer);
                ArrayPool<byte>.Shared.Return(pageBuffer);
            }
        }

        private static void ProcessPage(Stream destination, byte[] pageBuffer, int offset, int pageSize, ref int contiguousZeroPages,
            int minZeroPagesForHole, byte[] zeroBuffer, ref long logicalPosition, ref long destinationPosition)
        {
            bool isZeroPage = new ReadOnlySpan<byte>(pageBuffer, offset, pageSize).ContainsAnyExcept((byte)0) == false;

            if (isZeroPage)
            {
                contiguousZeroPages++;
                logicalPosition += pageSize;
                return;
            }

            if (contiguousZeroPages > 0)
            {
                if (contiguousZeroPages < minZeroPagesForHole)
                {
                    long zeroStart = logicalPosition - ((long)contiguousZeroPages * pageSize);
                    WriteZeros(destination, zeroBuffer, zeroStart, (long)contiguousZeroPages * pageSize, ref destinationPosition);
                }

                contiguousZeroPages = 0;
            }

            FlushPendingWrite(destination, pageBuffer, offset, pageSize, logicalPosition, ref destinationPosition);
            logicalPosition += pageSize;
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

        private static void WriteZeros(Stream destination, byte[] zeroBuffer, long logicalOffset, long length, ref long destinationPosition)
        {
            // For small zero runs that don't meet the sparse threshold, we need to actually write them.
            // Seek to position and write zero bytes.
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
