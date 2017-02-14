using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Sparrow;
using Voron.Data.Fixed;
using Voron.Global;
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

        private struct StreamToPageWriter
        {
            private int _chunkNumber;

            private byte* _writePos;
            private byte* _writePosEnd;
            private int _numberOfPagesPerChunk;
            private long _totalSize;
            private Page _currentPage;

            private Tree _parent;

            private FixedSizeTree _tree;
            private int _version;

            public void Init(Tree parent, Slice key, int? initialNumberOfPagesPerChunk)
            {
                _parent = parent;
                _numberOfPagesPerChunk = 1;
                _tree = _parent.FixedTreeFor(key, valSize: (byte)sizeof(ChunkDetails));
                _version = _parent.DeleteStream(key);
                _numberOfPagesPerChunk = initialNumberOfPagesPerChunk ?? 1;
            }

            public void Write(Stream stream)
            {
                if (_localBuffer == null)
                    _localBuffer = new byte[4 * Constants.Storage.PageSize];

                AllocateMorePages();

                fixed (byte* pBuffer = _localBuffer)
                {
                    while (true)
                    {
                        var read = stream.Read(_localBuffer, 0, _localBuffer.Length);
                        if (read == 0)
                            break;

                        var toWrite = 0L;
                        while (true)
                        {
                            toWrite += WriteBufferToPage(pBuffer + toWrite, read - toWrite);
                            if (toWrite == read)
                                break;
                            // run out of room, need to allocate more
                            FlushPage(_currentPage.PageNumber,  (int)(_writePos - _currentPage.DataPointer));
                            AllocateMorePages();
                        }
                    }
                    var chunkSize = (int)(_writePos - _currentPage.DataPointer);
                    FlushPage(_currentPage.PageNumber, chunkSize);
                    _parent._tx.LowLevelTransaction.ShrinkOverflowPage(_currentPage.PageNumber, chunkSize);
                    RecordVersionAndSize();
                }

            }

            private long WriteBufferToPage(byte* pBuffer, long size)
            {
                var remaining = _writePosEnd - _writePos;
                var toWrite = Math.Min(size, remaining);
                Memory.Copy(_writePos, pBuffer, toWrite);
                _writePos += toWrite;
                _totalSize += toWrite;
                return toWrite;
            }

            private void FlushPage(long pageNumber, int chunkSize)
            {
                var chunkDetails = new ChunkDetails
                {
                    PageNumber = pageNumber,
                    ChunkSize = chunkSize
                };
                Slice value;
                using (Slice.External(_parent._tx.Allocator, (byte*)&chunkDetails, sizeof(ChunkDetails), out value))
                {
                    _tree.Add(_chunkNumber++, value);
                }
            }

            private void RecordVersionAndSize()
            {
                var chunkDetails = new ChunkDetails
                {
                    PageNumber = _totalSize,
                    ChunkSize = _version + 1
                };
                Slice value;
                using (Slice.External(_parent._tx.Allocator, (byte*)&chunkDetails, sizeof(ChunkDetails), out value))
                {
                    _tree.Add(StreamSizeValue, value);
                }
            }

            private void AllocateMorePages()
            {
                var overflowSize = (_numberOfPagesPerChunk * Constants.Storage.PageSize) - PageHeader.SizeOf;
                int _;
                _currentPage = _parent._tx.LowLevelTransaction.AllocateOverflowRawPage(overflowSize, out _, zeroPage: false);
                _parent.State.OverflowPages += _numberOfPagesPerChunk;
                _writePos = _currentPage.DataPointer;
                _writePosEnd = _currentPage.Pointer + (_numberOfPagesPerChunk * Constants.Storage.PageSize);
                _numberOfPagesPerChunk = Math.Min(_numberOfPagesPerChunk * 2, 4096);
            }
        }

        public void AddStream(string key, Stream stream, int? initialNumberOfPagesPerChunk = null)
        {
            Slice str;
            using (Slice.From(_tx.Allocator, key, out str))
                AddStream(str, stream, initialNumberOfPagesPerChunk);
        }

        public void AddStream(Slice key, Stream stream, int? initialNumberOfPagesPerChunk = null)
        {
            var writer = new StreamToPageWriter();
            writer.Init(this, key, initialNumberOfPagesPerChunk);
            writer.Write(stream);
        }

        public VoronStream ReadStream(string key)
        {
            Slice str;
            using (Slice.From(_tx.Allocator, key, out str))
                return ReadStream(str);
        }

        public VoronStream ReadStream(Slice key)
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
            return new VoronStream(tree.Name, chunksDetails, _llt);
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

                var chunkDetails = ((ChunkDetails*)slice.Content.Ptr);
                length = chunkDetails->PageNumber;
                version = chunkDetails->ChunkSize;
            }
        }

        public int DeleteStream(string key)
        {
            Slice str;
            using (Slice.From(_tx.Allocator, key, out str))
                return DeleteStream(str);
        }

        public int DeleteStream(Slice key)
        {
            var tree = FixedTreeFor(key, valSize: (byte)sizeof(ChunkDetails));
            int version = 0;
            Slice value;
            using (tree.Read(StreamSizeValue, out value))
            {
                if (value.HasValue)
                {
                    var chunkDetails = (ChunkDetails*)value.Content.Ptr;
                    version = chunkDetails->ChunkSize;
                }
            }
            tree.Delete(StreamSizeValue);

            while (true)
            {
                using (var it = tree.Iterate())// we can use the iterator exactly once
                {
                    var llt = _tx.LowLevelTransaction;

                    if (!it.SeekToLast())
                        break;

                    var chunkDetails = ((ChunkDetails*)it.CreateReaderForCurrent().Base);
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
            return version;
        }
    }
}