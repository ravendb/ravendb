using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Utils;
using Voron.Data.Fixed;
using Voron.Exceptions;
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

        private const int MaxNumberOfPagerPerChunk = 4 * Constants.Size.Megabyte / Constants.Storage.PageSize;

        [ThreadStatic]
        private static byte[] _localBuffer;

        static Tree()
        {
            ThreadLocalCleanup.ReleaseThreadLocalState += () => _localBuffer = null;
        }

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
                _version = _parent.DeleteStream(key).Version;
                _numberOfPagesPerChunk = initialNumberOfPagesPerChunk ?? 1;
                _tag = tag;
            }

            public void Write(Stream stream)
            {
                _localBuffer = ArrayPool<byte>.Shared.Rent(512 * Constants.Size.Kilobyte);
                try
                {
                    AllocateNextPage();

                    ((StreamPageHeader*)_currentPage.Pointer)->StreamPageFlags |= StreamPageFlags.First;

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
                                RecordChunkPage(_currentPage.PageNumber, (int)(_writePos - _currentPage.DataPointer));
                                AllocateNextPage();
                            }
                        }

                        var chunkSize = (int)(_writePos - _currentPage.DataPointer);
                        RecordChunkPage(_currentPage.PageNumber, chunkSize);

                        var remaining = _writePosEnd - _writePos;
                        var infoSize = StreamInfo.SizeOf;

                        if (_tag != null)
                            infoSize += _tag.Value.Size;

                        if (remaining < infoSize)
                        {
                            _numberOfPagesPerChunk = 1;
                            AllocateNextPage();
                            chunkSize = 0;
                            RecordChunkPage(_currentPage.PageNumber, chunkSize);
                        }

                        RecordStreamInfo();

                        _parent._tx.LowLevelTransaction.ShrinkOverflowPage(_currentPage.PageNumber, chunkSize + infoSize, _parent);
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(_localBuffer);
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

            private void RecordChunkPage(long pageNumber, int chunkSize)
            {
                var chunkDetails = new ChunkDetails
                {
                    PageNumber = pageNumber,
                    ChunkSize = chunkSize
                };
                ((StreamPageHeader*)_currentPage.Pointer)->ChunkSize = chunkSize;
                using (Slice.External(_parent._tx.Allocator, (byte*)&chunkDetails, ChunkDetails.SizeOf, out Slice value))
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

            /// <summary>
            /// Allocates next stream page ahead of time so we can flush the old page with its page number
            /// </summary>
            /// <returns></returns>
            private void AllocateNextPage()
            {
                var overflowSize = (_numberOfPagesPerChunk * Constants.Storage.PageSize) - PageHeader.SizeOf;
                var nextPage = _parent._tx.LowLevelTransaction.AllocateOverflowRawPage(overflowSize, out _, zeroPage: false);
                if (_currentPage.Pointer != null)
                {
                    var streamHeaderPtr = (StreamPageHeader*)_currentPage.Pointer;
                    streamHeaderPtr->StreamNextPageNumber = nextPage.PageNumber;
                }

                _currentPage = nextPage;
                _currentPage.Flags |= PageFlags.Stream;

                ref var header = ref _parent.ModifyHeader();
                header.OverflowPages += _numberOfPagesPerChunk;
                _writePos = _currentPage.DataPointer;

                ((StreamPageHeader*)_currentPage.Pointer)->StreamNextPageNumber = 0;
                ((StreamPageHeader*)_currentPage.Pointer)->ChunkSize = 0;
                _writePosEnd = _currentPage.Pointer + (_numberOfPagesPerChunk * Constants.Storage.PageSize);
                _numberOfPagesPerChunk = Math.Min(_numberOfPagesPerChunk * 2, MaxNumberOfPagerPerChunk);
            }
        }

        public void AddStream(string key, Stream stream, string tag = null, int? initialNumberOfPagesPerChunk = null)
        {
            using (Slice.From(_tx.Allocator, key, out Slice str))
            {
                if (tag != null)
                {
                    using (Slice.From(_tx.Allocator, tag, out Slice tagStr))
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

            if ((ReadHeader().Flags & TreeFlags.Streams) != TreeFlags.Streams)
            {
                ref var header = ref ModifyHeader();
                header.Flags |= TreeFlags.Streams;
            }
        }

        public VoronStream ReadStream(string key)
        {
            using (Slice.From(_tx.Allocator, key, out Slice str))
                return ReadStream(str);
        }

        public VoronStream ReadStream(Slice key)
        {
            var pieces = ReadTreeChunks(key, out var tree);
            if (pieces == null)
                return null;
            return new VoronStream(tree.Name, pieces, _llt);
        }

        public ChunkDetails[] ReadTreeChunks(Slice key, out FixedSizeTree tree)
        {
            tree = FixedTreeFor(key, ChunkDetails.SizeOf);
            var numberOfChunks = tree.NumberOfEntries;

            if (numberOfChunks <= 0)
                return null;

            var chunksDetails = new ChunkDetails[numberOfChunks];

            var i = 0;
            using (var it = tree.Iterate())
            {
                if (it.Seek(0) == false)
                {
                    Debug.Assert(false, "ReadTreeChunks failed to find any chunks, but we checked that the fst is not empty");
                    return null; // can never happen
                }

                do
                {
                    using (it.Value(out Slice slice))
                    {
                        chunksDetails[i++] = *(ChunkDetails*)slice.Content.Ptr;
                    }
                } while (it.MoveNext());
            }

            return chunksDetails;
        }

        public bool StreamExist(Slice key)
        {
            var tree = FixedTreeFor(key, ChunkDetails.SizeOf);
            return tree.NumberOfEntries > 0;
        }

        public int TouchStream(Slice key)
        {
            var info = GetStreamInfo(key, writable: true);

            if (info == null)
                return 0;

            return ++info->Version;
        }

        public StreamInfo? GetStreamInfoForReporting(Slice key, out string tag)
        {
            tag = null;

            if (TryGetLastChunkDetailsForStream(key, out var lastChunk) == false)
                return null;

            var canRemovePage = CanRemovePage(lastChunk.PageNumber);
            var page = _llt.GetPage(lastChunk.PageNumber);

            try
            {
                var info = (StreamInfo*)(page.DataPointer + lastChunk.ChunkSize);
                tag = GetStreamTag(info);

                return new StreamInfo
                {
                    TagSize = info->TagSize,
                    TotalSize = info->TotalSize,
                    Version = info->Version
                };
            }
            finally
            {
                RemovePage();
            }

            bool CanRemovePage(long pageNumber)
            {
                // this methods checks if page was not used elsewhere prior executing GetStreamInfoForReporting method
                // if yes then we cannot remove it to avoid releasing used memory

                var states = _llt.PagerTransactionState.ForCrypto;
                if (states == null)
                    return false; // not encrypted

                if (states.Count == 0) 
                    return true; // no states yet

                foreach (var kvp in states)
                {
                    var pagerStates = kvp.Value;
                    if (pagerStates.TryGetValue(pageNumber, out _))
                        return false;
                }

                return true;
            }

            void RemovePage()
            {
                if (canRemovePage == false)
                    return;

                var states = _llt.PagerTransactionState.ForCrypto;
                if (states == null || states.Count == 0)
                    return;

                foreach (var kvp in states)
                {
                    var pager = kvp.Key;
                    var pagerStates = kvp.Value;
                    if (pagerStates.TryGetValue(page.PageNumber, out var buffer) == false)
                        continue;

                    if (buffer.Pointer != page.Pointer)
                        continue;

                    buffer.ReleaseRef();

                    _llt._pageLocator.Reset(page.PageNumber);
                    pagerStates.RemoveBuffer(pager, page.PageNumber);
                    return;
                }
            }
        }

        public StreamInfo* GetStreamInfo(Slice key, bool writable)
        {
            if (TryGetLastChunkDetailsForStream(key, out var lastChunk) == false)
                return null;

            var page = _llt.GetPage(lastChunk.PageNumber);

            if (writable)
                page = _llt.ModifyPage(page.PageNumber);

            return (StreamInfo*)(page.DataPointer + lastChunk.ChunkSize);
        }

        private bool TryGetLastChunkDetailsForStream(Slice key, out ChunkDetails lastChunk)
        {
            lastChunk = default;
            var tree = FixedTreeFor(key, ChunkDetails.SizeOf);

            if (tree.NumberOfEntries == 0)
                return false;

            using (var it = tree.Iterate())
            {
                if (it.SeekToLast() == false)
                    return false;

                using (tree.Read(it.CurrentKey, out Slice slice))
                {
                    if (slice.HasValue == false)
                        return false;

                    lastChunk = *(ChunkDetails*)slice.Content.Ptr;
                    return true;
                }
            }
        }

        internal FixedSizeTree GetStreamChunksTree(Slice key)
        {
            return FixedTreeFor(key, ChunkDetails.SizeOf);
        }

        public int DeleteStream(string key)
        {
            using (Slice.From(_tx.Allocator, key, out Slice str))
                return DeleteStream(str).Version;
        }

        public (int Version, long Size) DeleteStream(Slice key)
        {
            int version = 0;
            long size = 0;

            var info = GetStreamInfo(key, writable: false);

            if (info != null)
            {
                version = info->Version;
                size = info->TotalSize;
            }

            var llt = _tx.LowLevelTransaction;

            var streamPages = GetStreamPages(GetStreamChunksTree(key), info);

            for (var i = 0; i < streamPages.Count; i++)
            {
                llt.FreePage(streamPages[i]);
            }

            ref var header = ref ModifyHeader();
            header.OverflowPages -= streamPages.Count;

            DeleteFixedTreeFor(key, ChunkDetails.SizeOf);

            return (version, size);
        }

        internal List<long> GetStreamPages(FixedSizeTree chunksTree, StreamInfo* info)
        {
            var pages = new List<long>();

            var chunkIndex = 0;

            using (var it = chunksTree.Iterate())
            {
                if (it.Seek(0) == false)
                    return pages;

                var totalSize = 0L;

                do
                {
                    var chunk = (ChunkDetails*)it.CreateReaderForCurrent().Base;

                    totalSize += chunk->ChunkSize;

                    long size = chunk->ChunkSize;

                    if (chunkIndex == chunksTree.NumberOfEntries - 1)
                    {
                        // stream info is put after the last chunk

                        size += StreamInfo.SizeOf + info->TagSize;
                    }

                    var numberOfPages = Paging.GetNumberOfOverflowPages(size);

                    for (int i = 0; i < numberOfPages; i++)
                    {
                        pages.Add(chunk->PageNumber + i);
                    }

                    chunkIndex++;

                } while (it.MoveNext());

                if (totalSize != info->TotalSize)
                    ThrowStreamSizeMismatch(chunksTree.Name, totalSize, info);

                return pages;
            }
        }

        public string GetStreamTag(Slice key)
        {
            var info = GetStreamInfo(key, writable: false);

            return GetStreamTag(info);
        }

        public ByteStringContext.InternalScope GetStreamTag(Slice key, out Slice tag)
        {
            var info = GetStreamInfo(key, writable: false);

            tag = default;
            if (info == null || info->TagSize == 0)
                return default;

            return Slice.From(_tx.Allocator, StreamInfo.GetTagPtr(info), info->TagSize, out tag);
        }

        public string GetStreamTag(string key)
        {
            using (Slice.From(_tx.Allocator, key, out Slice str))
                return GetStreamTag(str);
        }

        private string GetStreamTag(StreamInfo* info)
        {
            if (info == null || info->TagSize == 0)
                return null;

            using (Slice.From(_tx.Allocator, StreamInfo.GetTagPtr(info), info->TagSize, out Slice result))
            {
                return result.ToString().Replace((char)SpecialChars.RecordSeparator, '|');
            }
        }

        [DoesNotReturn]
        private void ThrowStreamSizeMismatch(Slice name, long totalChunksSize, StreamInfo* info)
        {
            VoronUnrecoverableErrorException.Raise(_tx.LowLevelTransaction.Environment,
                $"Stream size mismatch of '{name}' stream. Sum of chunks size is {totalChunksSize} while stream info has {info->TotalSize}");
        }
    }

    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = PageHeader.SizeOf)]
    public unsafe struct StreamPageHeader
    {
        public const int SizeOf = PageHeader.SizeOf;

        static StreamPageHeader()
        {
            Debug.Assert(sizeof(StreamPageHeader) == SizeOf);
        }

        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(12)]
        public PageFlags Flags;

        [FieldOffset(13)]
        public StreamPageFlags StreamPageFlags;

        //This field is for use of the DR tool only 
        [FieldOffset(14)]
        public long StreamNextPageNumber;

        //This field should be the same as the overflow size except 
        //for the last page that contains some data at the end of the stream
        //This is needed for the DR tool so we could properly calculate the stream hash
        [FieldOffset(22)]
        public long ChunkSize;
    }

    [Flags]
    public enum StreamPageFlags : byte
    {
        None = 0,
        First = 1,
        Reserved1 = 2,
        Reserved2 = 4
    }
}
