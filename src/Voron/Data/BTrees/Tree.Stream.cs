using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using Sparrow;
using Voron.Impl;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Data.BTrees
{
    public unsafe partial class Tree
    {
        private const int StreamSizeValue = -1;// known invalid value for the chunks

        [StructLayout(LayoutKind.Explicit, Size = 12)]
        public struct ChunkDetails
        {
            [FieldOffset(0)]
            public long PageNumber;
            [FieldOffset(8)]
            public int ChunkSize;
        }
        
        [ThreadStatic]
        private static byte[] _localBuffer;
        
        public void AddStream(Slice key, Stream stream, int chunkSize = 4 * 1024 * 1024)
        {
            var tree = FixedTreeFor(key, valSize: (byte)sizeof(ChunkDetails));

            int version = 0;
            Slice value;
            if (tree.NumberOfEntries != 0)
            {
                using (tree.Read(StreamSizeValue, out value))
                {
                    if (value.HasValue)
                    {
                        var chunkDetails = ((ChunkDetails*)value.Content.Ptr);
                        version = chunkDetails->ChunkSize;
                    }
                }
                DeleteStream(key);
            }

            if (_localBuffer == null)
                _localBuffer = new byte[16*1024];

            fixed (byte* pBuffer = _localBuffer)
            {
                var chunkDetails = new ChunkDetails();
                using (Slice.External(_tx.Allocator, (byte*)&chunkDetails, sizeof(ChunkDetails), out value))
                {
                    var remainingSize = stream.Length;
                    chunkDetails.PageNumber = remainingSize;
                    chunkDetails.ChunkSize = version + 1;
                    tree.Add(StreamSizeValue, value); // marker for the file size

                    long chunkNum = 0;
                    int read = 0;
                    int bufferPos = 0;

                    while (remainingSize > 0)
                    {
                        var currentChunkSize = Math.Min(remainingSize, chunkSize - sizeof(PageHeader));
                        var page = _tx.LowLevelTransaction.AllocateOverflowRawPage(currentChunkSize, zeroPage: false);

                        State.OverflowPages +=
                            _tx.LowLevelTransaction.DataPager.GetNumberOfOverflowPages(currentChunkSize);

                        chunkDetails.PageNumber = page.PageNumber;
                        chunkDetails.ChunkSize = (int)currentChunkSize;
                        tree.Add(chunkNum++, value);

                        remainingSize -= currentChunkSize;

                        var writtenBytes = 0;
                        do
                        {
                            var toWrite = Math.Min(read, chunkSize - sizeof(PageHeader) - writtenBytes);
                            Memory.Copy(page.DataPointer + writtenBytes, pBuffer + bufferPos, toWrite);
                            writtenBytes += read;
                            currentChunkSize -= read;
                            if (currentChunkSize <= 0)
                            {
                                read -= toWrite;
                                bufferPos = toWrite;
                                break;
                            }
                            
                            read = stream.Read(_localBuffer, 0, _localBuffer.Length);
                            bufferPos = 0;
                        } while (true);
                    }
                }
            }
        }

        public ChunkedSparseMmapStream ReadStream(Slice key)
        {
            var tree = FixedTreeFor(key, valSize: (byte)sizeof(ChunkDetails));
            var numberOfChunks = tree.NumberOfEntries - 1; //-1 for the StreamSize entry

            if (numberOfChunks <= 0)
                return null;

            var chunksDetails = new ChunkDetails[numberOfChunks];

            var i = 0;
            using (var it = tree.Iterate())
            {
                if (it.Seek(0) == false)
                    return null;

                do
                {
                    Slice slice;
                    using (it.Value(out slice))
                    {
                        chunksDetails[i++] = *(ChunkDetails*)slice.Content.Ptr;
                    }
                } while (it.MoveNext());

            }
            return new ChunkedSparseMmapStream(tree.Name, chunksDetails, _llt);
        }

        public int TouchStream(Slice key)
        {
            var tree = FixedTreeFor(key, valSize: (byte)sizeof(ChunkDetails));

            if (tree.NumberOfEntries == 0)
            {
                return 0;
            }

            Slice slice;
            using (tree.Read(StreamSizeValue, out slice))
            {
                if (slice.HasValue == false)
                    return 0;

                var chunkDetails = *((ChunkDetails*)slice.Content.Ptr);
                chunkDetails.ChunkSize++;
                Slice val;
                using (Slice.External(_tx.Allocator, (byte*)&chunkDetails, sizeof(ChunkDetails), out val))
                {
                    tree.Add(StreamSizeValue, val);
                }
                return chunkDetails.ChunkSize;
            }
        }

        public void GetStreamLengthAndVersion(Slice key, out long length, out int version)
        {
            var tree = FixedTreeFor(key, valSize: (byte)sizeof(ChunkDetails));
            if (tree.NumberOfEntries == 0)
            {
                length = -1;
                version = 0;
                return;
            }

            Slice slice;
            using (tree.Read(StreamSizeValue, out slice))
            {
                if (slice.HasValue == false)
                {
                    length = -1;
                    version = 0;
                    return;
                }

                var chunkDetails = ((ChunkDetails*) slice.Content.Ptr);
                length = chunkDetails->PageNumber;
                version = chunkDetails->ChunkSize;
            }
        }

        public void DeleteStream(Slice key)
        {
            var tree = FixedTreeFor(key, valSize: (byte)sizeof(ChunkDetails));
            tree.Delete(StreamSizeValue);

            while (true)
            {
                using (var it = tree.Iterate())// we can use the iterator exactly once
                {
                    var llt = _tx.LowLevelTransaction;

                    if (!it.SeekToLast())
                        break;

                    var chunkDetails = ((ChunkDetails*) it.CreateReaderForCurrent().Base);
                    var pageNumber = chunkDetails->PageNumber;
                    var numberOfPages = llt.DataPager.GetNumberOfOverflowPages(chunkDetails->ChunkSize);
                    for (int i = 0; i < numberOfPages; i++)
                    {
                        llt.FreePage(pageNumber + i);
                        _pageLocator.Reset(pageNumber + i);
                    }
                    State.OverflowPages -= numberOfPages;
                    tree.Delete(it.CurrentKey);
                }
            }
        }
    }
}