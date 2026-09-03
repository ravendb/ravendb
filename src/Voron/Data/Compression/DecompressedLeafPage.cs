using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Server;
using Voron.Data.BTrees;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.Compression
{
    public sealed unsafe class DecompressedLeafPage : IDisposable
    {
        public DecompressedLeafPage(byte* basePtr, int pageSize, DecompressionUsage usage, TreePage original, ByteStringContext.InternalScope disposable)
        {
            Page = new TreePage(basePtr, pageSize);
            Original = original;
            _disposable = disposable;
            Usage = usage;

            PageNumber = Original.PageNumber;
            TreeFlags = Original.TreeFlags;
            Flags = Original.Flags & ~PageFlags.Compressed;
        }

        public TreePage Page;

        public TreePage Original;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static implicit operator TreePage(DecompressedLeafPage page) => page.Page;

        public long PageNumber
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Page.PageNumber;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => Page.PageNumber = value;
        }

        public ushort NumberOfEntries
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Page.NumberOfEntries;
        }

        public byte* Base
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Page.Base;
        }

        public int PageSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Page.PageSize;
        }

        public sbyte LastMatch
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Page.LastMatch;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => Page.LastMatch = value;
        }

        public short LastSearchPosition
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Page.LastSearchPosition;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => Page.LastSearchPosition = value;
        }

        public ushort Lower
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Page.Lower;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => Page.Lower = value;
        }

        public ushort Upper
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Page.Upper;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => Page.Upper = value;
        }

        public PageFlags Flags
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Page.Flags;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => Page.Flags = value;
        }

        public TreePageFlags TreeFlags
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Page.TreeFlags;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => Page.TreeFlags = value;
        }

        public ushort* KeysOffsets
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Page.KeysOffsets;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TreeNodeHeader* GetNode(int n) => Page.GetNode(n);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TreeNodeHeader* Search(LowLevelTransaction tx, Slice key, bool backward = false) => Page.Search(tx, key, backward);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable GetNodeKey(LowLevelTransaction tx, int nodeNumber, out Slice key) => Page.GetNodeKey(tx, nodeNumber, out key);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RemoveNode(int index) => Page.RemoveNode(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CalcSizeUsed() => Page.CalcSizeUsed();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool HasSpaceFor(LowLevelTransaction tx, int len) => Page.HasSpaceFor(tx, len);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int NodePositionFor(LowLevelTransaction tx, Slice key) => Page.NodePositionFor(tx, key);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* AddPageRefNode(int index, Slice key, long pageNumber) => Page.AddPageRefNode(index, key, pageNumber);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* AddDataNode(int index, Slice key, int dataSize) => Page.AddDataNode(index, key, dataSize);

        [System.Diagnostics.Conditional("VALIDATE")]
        public void DebugValidate(Tree tree, long rootPageNumber) => Page.DebugValidate(tree, rootPageNumber);

        private ByteStringContext.InternalScope _disposable;

        public bool Cached;

        public DecompressionUsage Usage;

        public void Dispose()
        {
            if (Cached)
                return;

            _disposable.Dispose();
        }

        public void CopyToOriginal(LowLevelTransaction tx, bool defragRequired, bool wasModified, Tree tree)
        {
            if (CalcSizeUsed() < Original.PageMaxSpace)
            {
                // no need to compress
                Original.Lower = (ushort)Constants.Tree.PageHeaderSize;
                Original.Upper = (ushort)Original.PageSize;
                Original.Flags &= ~PageFlags.Compressed;

                for (var i = 0; i < NumberOfEntries; i++)
                {
                    var node = GetNode(i);
                    using (TreeNodeHeader.ToSlicePtr(tx.Allocator, node, out var slice))
                        Original.CopyNodeDataToEndOfPage(node, slice);
                }

                tree.DecompressionsCache.Invalidate(PageNumber, DecompressionUsage.Write);
            }
            else
            {
                using (LeafPageCompressor.TryGetCompressedTempPage(tx, this, out var compressed, defrag: defragRequired))
                {
                    if (compressed == null)
                    {
                        if (wasModified == false)
                            return;

                        if (NumberOfEntries > 0)
                        {
                            // we aren't able to compress the page back to 8KB page
                            // let's split it and try to copy it then

                            SplitPage(tx, tree);
                        }
                        else
                        {
                            ThrowCouldNotCompressEmptyDecompressedPage(PageNumber);
                        }

                        CopyToOriginal(tx, defragRequired: true, wasModified: true, tree);

                        return;
                    }

                    LeafPageCompressor.CopyToPage(compressed, Original);
                }
            }
        }

        private void SplitPage(LowLevelTransaction tx, Tree tree)
        {
            // let's take a node from the middle and add it again with the page splitting
            // this way we'll copy half of the page to a new page

            var middleNodeIndex = NumberOfEntries / 2;

            using (GetNodeKey(tx, middleNodeIndex, out var middleNodeKey))
            {
                tree.FindPageFor(middleNodeKey, node: out _, cursor: out var treeCursor, allowCompressed: true);

                // let's copy key and data of a node that we'll remove

                var key = middleNodeKey.Clone(tx.Allocator);

                var node = GetNode(middleNodeIndex);

                var flags = node->Flags;
                var valueReader = tree.GetValueReaderFromHeader(node);

                using (tx.Allocator.Allocate(valueReader.Length, out var tempValueOutput))
                {
                    Memory.Copy(tempValueOutput.Ptr, valueReader.Base, valueReader.Length);

                    RemoveNode(middleNodeIndex);

                    Search(tx, key);

                    {
                        ref var cursor = ref treeCursor;
                        cursor.SetTopPage(this); // we need to use uncompressed page here because it might have some modifications (e.g. deleted node)

                        var pageSplitter = new TreePageSplitter(tx, tree, key, valueReader.Length, PageNumber, flags, ref cursor,
                            splittingOnDecompressed: true);

                        var pos = pageSplitter.Execute();

                        tempValueOutput.CopyTo(pos);
                    }
                }
            }
        }

        [DoesNotReturn]
        private static void ThrowCouldNotCompressEmptyDecompressedPage(long pageNumber)
        {
            throw new InvalidOperationException($"Empty decompressed page #{pageNumber} could not be compressed back. Should never happen");
        }
    }
}
