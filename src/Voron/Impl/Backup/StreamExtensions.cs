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
        /// Copies from source to destination while detecting contiguous zero-page regions and skipping them to create a sparse file.
        /// The destination file must already be marked as sparse on Windows (via <see cref="SparseFileHelper.TryMarkFileAsSparse"/>).
        /// Zero runs shorter than <see cref="FreeSpaceHandling.NumberOfFreePagesForSparseConsideration"/> pages are written normally.
        /// </summary>
        public static void CopyToPreservingSparseRegions(this Stream source, FileStream destination, Action<int> onProgress, CancellationToken cancellationToken)
        {
            int pageSize = Constants.Storage.PageSize;
            int minZeroPagesForHole = FreeSpaceHandling.NumberOfFreePagesForSparseConsideration;

            int bufferSize = DefaultBufferSize;
            var readBuffer = ArrayPool<byte>.Shared.Rent(bufferSize);
            var zeroBuffer = ArrayPool<byte>.Shared.Rent(DefaultBufferSize);
            var pageBuffer = ArrayPool<byte>.Shared.Rent(pageSize);

            try
            {
                Array.Clear(zeroBuffer, 0, DefaultBufferSize);

                long logicalPosition = 0;
                long destinationPosition = 0;
                int contiguousZeroPages = 0;
                int partialPageBytes = 0;

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
                        int pendingWriteStart = -1;

                        for (int i = 0; i < fullPages; i++)
                        {
                            int pageOffset = offset + (i * pageSize);
                            bool isZeroPage = new ReadOnlySpan<byte>(readBuffer, pageOffset, pageSize).ContainsAnyExcept((byte)0) == false;

                            if (isZeroPage)
                            {
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
                                if (contiguousZeroPages > 0)
                                {
                                    if (contiguousZeroPages < minZeroPagesForHole)
                                    {
                                        long zeroStart = logicalPosition - ((long)contiguousZeroPages * pageSize);
                                        WriteZeros(destination, zeroBuffer, zeroStart, (long)contiguousZeroPages * pageSize, ref destinationPosition);
                                    }

                                    contiguousZeroPages = 0;
                                }

                                if (pendingWriteStart < 0)
                                    pendingWriteStart = pageOffset;
                            }

                            logicalPosition += pageSize;
                        }

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

                if (contiguousZeroPages > 0 && contiguousZeroPages < minZeroPagesForHole)
                {
                    long zeroStart = logicalPosition - ((long)contiguousZeroPages * pageSize);
                    WriteZeros(destination, zeroBuffer, zeroStart, (long)contiguousZeroPages * pageSize, ref destinationPosition);
                }

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
