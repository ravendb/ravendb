using System;
using System.IO;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Utils;
using Voron.Data.Fixed;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;

namespace Voron.Data.BTrees
{
    public unsafe partial class Tree
    {
        [StructLayout(LayoutKind.Explicit, Size = SizeOf)]
        public struct ChunkDetails
        {
            public const byte SizeOf = 12;

            [FieldOffset(0)]
            public long PageNumber;

            [FieldOffset(8)]
            public int ChunkSize;
        }

        [StructLayout(LayoutKind.Explicit, Size = SizeOf)]
        public struct StreamInfo
        {
            public const int SizeOf = 16;

            [FieldOffset(0)]
            public long TotalSize;

            [FieldOffset(8)]
            public int Version;

            [FieldOffset(12)]
            public int TagSize;

            public static byte* GetTagPtr(StreamInfo* info)
            {
                return (byte*)info + SizeOf;
            }
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
            private Slice? _tag;

            public void Init(Tree parent, Slice key, Slice? tag, int? initialNumberOfPagesPerChunk)
            {
                _parent = parent;
                _numberOfPagesPerChunk = 1;
                _tree = _parent.FixedTreeFor(key, ChunkDetails.SizeOf);
                _version = _parent.DeleteStream(key);
                _numberOfPagesPerChunk = initialNumberOfPagesPerChunk ?? 1;
                _tag = tag;
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

                    var remaining = _writePosEnd - _writePos;
                    var infoSize = StreamInfo.SizeOf;

                    if (_tag != null)
                        infoSize += _tag.Value.Size;

                    if (remaining < infoSize)
                    {
                        AllocateMorePages();
                        chunkSize = 0;
                        FlushPage(_currentPage.PageNumber, chunkSize);
                    }

                    RecordStreamInfo();

                    _parent._tx.LowLevelTransaction.ShrinkOverflowPage(_currentPage.PageNumber, chunkSize + infoSize); 
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
                using (Slice.External(_parent._tx.Allocator, (byte*)&chunkDetails, ChunkDetails.SizeOf, out value))
                {
                    _tree.Add(_chunkNumber++, value);
                }
            }

            private void RecordStreamInfo()
            {
                var info = (StreamInfo*)_writePos;

                info->TotalSize = _totalSize;
                info->Version = _version + 1;

                if (_tag != null)
                {
                    _tag.Value.CopyTo(StreamInfo.GetTagPtr(info));
                    info->TagSize = _tag.Value.Size;
                }
                else
                    info->TagSize = 0;
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

        public void AddStream(string key, Stream stream, string tag = null, int? initialNumberOfPagesPerChunk = null)
        {
            Slice str;
            
            using (Slice.From(_tx.Allocator, key, out str))
            {
                if (tag != null)
                {
                    Slice tagStr;
                    using (Slice.From(_tx.Allocator, tag, out tagStr))
                        AddStream(str, stream, tagStr, initialNumberOfPagesPerChunk);
                }
                else
                    AddStream(str, stream, null, initialNumberOfPagesPerChunk);
            }
        }

        public void AddStream(Slice key, Stream stream, Slice? tag = null, int? initialNumberOfPagesPerChunk = null)
        {
            var writer = new StreamToPageWriter();
            writer.Init(this, key, tag, initialNumberOfPagesPerChunk);
            writer.Write(stream);

            State.Flags |= TreeFlags.Streams;
        }

        public VoronStream ReadStream(string key)
        {
            Slice str;
            using (Slice.From(_tx.Allocator, key, out str))
                return ReadStream(str);
        }

        public VoronStream ReadStream(Slice key)
        {
            var tree = FixedTreeFor(key, ChunkDetails.SizeOf);
            var numberOfChunks = tree.NumberOfEntries;

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
            var info = GetStreamInfo(key, writeable: true);

            if (info == null)
                return 0;

            return ++info->Version;
        }

        public StreamInfo* GetStreamInfo(Slice key, bool writeable)
        {
            var tree = FixedTreeFor(key, ChunkDetails.SizeOf);

            if (tree.NumberOfEntries == 0)
                return null;

            ChunkDetails lastChunk;
            using (var it = tree.Iterate())
            {
                if (it.SeekToLast() == false)
                    return null;

                Slice slice;
                using (tree.Read(it.CurrentKey, out slice))
                {
                    if (slice.HasValue == false)
                        return null;

                    lastChunk = *(ChunkDetails*) slice.Content.Ptr;
                }
            }

            var page = _llt.GetPage(lastChunk.PageNumber);

            if (writeable)
                page = _llt.ModifyPage(page.PageNumber);

            return (StreamInfo*)(page.DataPointer + lastChunk.ChunkSize);
        }

        internal FixedSizeTree GetStreamChunksTree(Slice key)
        {
            return FixedTreeFor(key, ChunkDetails.SizeOf);
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

            var info = GetStreamInfo(key, writeable: false);

            if (info != null)
                version = info->Version;

            using (var it = tree.Iterate())// we can use the iterator exactly once
            {
                var llt = _tx.LowLevelTransaction;

                if (it.Seek(0) == false)
                    return version;

                do
                {
                    var chunkDetails = ((ChunkDetails*)it.CreateReaderForCurrent().Base);
                    var pageNumber = chunkDetails->PageNumber;
                    var numberOfPages = llt.DataPager.GetNumberOfOverflowPages(chunkDetails->ChunkSize);
                    for (int i = 0; i < numberOfPages; i++)
                    {
                        llt.FreePage(pageNumber + i);
                    }
                    State.OverflowPages -= numberOfPages;
                } while (it.MoveNext());
            }

            DeleteFixedTreeFor(key, ChunkDetails.SizeOf);

            return version;
        }

        public string GetStreamTag(Slice key)
        {
            var info = GetStreamInfo(key, writeable: false);

            if (info == null || info->TagSize == 0)
                return null;

            Slice result;
            using (Slice.From(_tx.Allocator, StreamInfo.GetTagPtr(info), info->TagSize, out result))
            {
                return result.ToString().Replace((char)SpecialChars.RecordSeperator, '|');
            }
        }

        public string GetStreamTag(string key)
        {
            Slice str;
            using (Slice.From(_tx.Allocator, key, out str))
                return GetStreamTag(str);
        }
    }
}