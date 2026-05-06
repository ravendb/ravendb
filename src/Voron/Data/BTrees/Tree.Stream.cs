using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Utils;
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

        /// <summary>
        /// Header for streams stored inline in the tree node.
        /// Layout: [RootObjectType (1 byte)][StreamInfo (16 bytes)][Tag (TagSize bytes)][Data (TotalSize bytes)]
        /// Note: Unlike chunked streams (which store data first, then metadata), inline streams store metadata first, then data.
        /// </summary>
        [StructLayout(LayoutKind.Explicit, Size = SizeOf)]
        public struct InlineStreamHeader
        {
            public const int SizeOf = 1 + StreamInfo.SizeOf; // 17 bytes

            [FieldOffset(0)]
            public RootObjectType Type; // = RootObjectType.InlineStream

            [FieldOffset(1)]
            public StreamInfo Info;
        }

        private const int MaxNumberOfPagerPerChunk = 4 * Constants.Size.Megabyte / Constants.Storage.PageSize;
        
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
                _version = _parent.DeleteStream(key).Version;
                _tree = _parent.FixedTreeFor(key, ChunkDetails.SizeOf);
                _numberOfPagesPerChunk = initialNumberOfPagesPerChunk ?? 1;
                _tag = tag;
            }

            public void Write(Stream stream)
            {
                AllocateNextPage();

                ((StreamPageHeader*)_currentPage.Pointer)->StreamPageFlags |= StreamPageFlags.First;

                var buffer = stream != Stream.Null ? _parent.Llt.Transaction.StreamBuffer : StreamBufferAllocator.Buffer.Null;
                var localBuffer = buffer.AsSpan();

                {
                    while (true)
                    {
                        var read = stream.Read(localBuffer);
                        if (read == 0)
                            break;

                        var toWrite = 0L;
                        while (true)
                        {
                            toWrite += WriteBufferToPage(buffer.Pointer + toWrite, read - toWrite);
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

                    _parent._tx.LowLevelTransaction.ShrinkOverflowPage(_currentPage.PageNumber, chunkSize + infoSize, _parent.State);
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

                ref var state = ref _parent.State.Modify();
                state.OverflowPages += _numberOfPagesPerChunk;
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
            Debug.Assert(stream.CanSeek, "Stream must be seekable, we need it to simplify reseting position when stream is too large for inline storage");
            
            if ((State.Header.Flags & TreeFlags.Streams) != TreeFlags.Streams)
            {
                ref var state = ref State.Modify();
                state.Flags |= TreeFlags.Streams;
            }

            if (TryAddInlineStream(key, stream, tag))
                return;

            var writer = new StreamToPageWriter();
            writer.Init(this, key, tag, initialNumberOfPagesPerChunk);
            writer.Write(stream);
        }

        [SkipLocalsInit]
        private bool TryAddInlineStream(Slice key, Stream stream, Slice? tag)
        {
            var tagSize = tag?.Size ?? 0;
            // maxInlineSize calculates space available for the value (header + tag + data).
            // Key.Size is not subtracted because DirectAdd(key, len, ...) only accounts for value length.
            var maxInlineSize = _llt.DataPager.NodeMaxSize - Constants.Tree.NodeHeaderSize - InlineStreamHeader.SizeOf - tagSize;
            if (maxInlineSize <= 0)
                return false;


            var nodeMaxSize = _llt.DataPager.NodeMaxSize;
            Span<byte> buffer = stackalloc byte[nodeMaxSize];
            var totalRead = 0;
            while (totalRead < nodeMaxSize)
            {
                var read = stream.Read(buffer.Slice(totalRead));
                if (read == 0)
                    break;
                totalRead += read;
            }

            if (totalRead > maxInlineSize)
            {
                // Stream is too large for inline storage, reset and fall back
                stream.Position = 0;
                return false;
            }

            // The entire stream fits inline
            var version = DeleteStream(key).Version;
            var inlineSize = InlineStreamHeader.SizeOf + tagSize + totalRead;
            using (DirectAdd(key, inlineSize, out byte* ptr))
            {
                var header = (InlineStreamHeader*)ptr;
                header->Type = RootObjectType.InlineStream;
                header->Info.TotalSize = totalRead;
                header->Info.Version = version + 1;
                header->Info.TagSize = tagSize;

                var dest = ptr + InlineStreamHeader.SizeOf;
                if (tag != null)
                {
                    tag.Value.CopyTo(dest);
                    dest += tagSize;
                }

                fixed (byte* src = buffer)
                {
                    Memory.Copy(dest, src, totalRead);
                }
            }

            return true;
        }

        public Stream ReadStream(string key)
        {
            using (Slice.From(_tx.Allocator, key, out Slice str))
                return ReadStream(str);
        }

        public Stream ReadStream(Slice key)
        {
            if (IsInlineStream(key, out var inlineData, out _, out _))
            {
                var header = (InlineStreamHeader*)inlineData;
                var tagSize = header->Info.TagSize;
                var dataSize = (int)header->Info.TotalSize;
                var dataPtr = inlineData + InlineStreamHeader.SizeOf + tagSize;

                return new UnmanagedVoronStream(dataPtr, dataSize);
            }

            var pieces = ReadTreeChunks(key, out var tree);
            if (pieces == null)
                return null;
            return new VoronStream(pieces, _llt);
        }

        public bool IsInlineStream(string key, out byte* data, out int dataSize, out TreePage treePage)
        {
            using (Slice.From(_tx.Allocator, key, out Slice str))
                return IsInlineStream(str, out data, out dataSize, out treePage);
        }

        public bool IsInlineStream(Slice key, out byte* data, out int dataSize, out TreePage treePage)
        {
            data = null;
            dataSize = 0;
            treePage = default;

            var p = FindPageFor(key, out TreeNodeHeader* node);
            if (p.LastMatch != 0 || node == null)
                return false;

            if (node->Flags == TreeNodeFlags.PageRef)
                return false;

            var nodeData = (byte*)node + node->KeySize + Constants.Tree.NodeHeaderSize;
            var nodeDataSize = node->DataSize;

            if (nodeDataSize < InlineStreamHeader.SizeOf)
                return false;

            // Defensive check: verify we have the Streams flag set 
            // This prevents misidentification if another structure's first byte happens to match RootObjectType.InlineStream.
            if ((State.Header.Flags & TreeFlags.Streams) == 0)
                return false;

            if (((RootObjectType*)nodeData)[0] != RootObjectType.InlineStream)
                return false;

            data = nodeData;
            dataSize = nodeDataSize;
            treePage = p;
            return true;
        }

        public ChunkDetails[] ReadTreeChunks(Slice key, out FixedSizeTree tree)
        {
            if (IsInlineStream(key, out _, out _, out _))
            {
                tree = null;
                return null;
            }

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
            return Exists(key);
        }

        public int TouchStream(Slice key)
        {
            if (IsInlineStream(key, out var inlineData, out _, out var inlinePage))
            {
                // The page from IsInlineStream may be read-only, so call ModifyPage to get
                // a writable copy (preserving search position).
                var p = ModifyPage(inlinePage);
                var node = p.GetNode(p.LastSearchPosition);
                var data = (byte*)node + node->KeySize + Constants.Tree.NodeHeaderSize;
                var header = (InlineStreamHeader*)data;
                return ++header->Info.Version;
            }

            var info = GetStreamInfo(key, writable: true);

            if (info == null)
                return 0;

            return ++info->Version;
        }

        public StreamInfo? GetStreamInfoForReporting(Slice key, out string tag)
        {
            tag = null;

            if (IsInlineStream(key, out var inlineData, out _, out _))
            {
                var header = (InlineStreamHeader*)inlineData;
                var infoPtr = &header->Info;
                tag = GetStreamTag(infoPtr);

                return new StreamInfo
                {
                    TagSize = infoPtr->TagSize,
                    TotalSize = infoPtr->TotalSize,
                    Version = infoPtr->Version
                };
            }

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

                var lltState = (IPagerLevelTransactionState)_llt;
                var states = lltState.CryptoPagerTransactionState;
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

                var lltState = (IPagerLevelTransactionState)_llt;
                var states = lltState.CryptoPagerTransactionState;
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

                    if (CryptoPager.CanReturnBuffer(buffer) == false)
                        return;

                    buffer.ReleaseRef();

                    _llt._pageLocator.Reset(page.PageNumber);
                    pagerStates.RemoveBuffer(page.PageNumber);

                    var cryptoPager = (CryptoPager)pager;
                    cryptoPager.ReturnBuffer(buffer);
                    return;
                }
            }
        }

        public StreamInfo* GetStreamInfo(Slice key, bool writable)
        {
            if (IsInlineStream(key, out var inlineData, out _, out var inlinePage))
            {
                if (writable)
                {
                    // The page from IsInlineStream may be read-only, so call ModifyPage to get
                    // a writable copy (preserving search position).
                    var p = ModifyPage(inlinePage);
                    var node = p.GetNode(p.LastSearchPosition);
                    inlineData = (byte*)node + node->KeySize + Constants.Tree.NodeHeaderSize;
                }

                return &((InlineStreamHeader*)inlineData)->Info;
            }

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
            if (IsInlineStream(key, out var inlineData, out _, out _))
            {
                var header = (InlineStreamHeader*)inlineData;
                version = header->Info.Version;
                size = header->Info.TotalSize;
                Delete(key); // Regular tree key deletion — no overflow pages to free
                return (version, size);
            }


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

            ref var state = ref State.Modify();
            state.OverflowPages -= streamPages.Count;

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

                    var numberOfPages = VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(size);

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
