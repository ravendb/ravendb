using System;
using Sparrow;
using Voron.Data.BTrees;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;

namespace Voron.Data.Compression
{
    public unsafe class DecompressedLeafPage : TreePage, IDisposable
    {
        private readonly TemporaryPage _tempPage;

        private bool _disposed;

        public DecompressedLeafPage(byte* basePtr, int pageSize, DecompressionUsage usage, TreePage original, TemporaryPage tempPage) : base(basePtr, pageSize)
        {
            Original = original;
            Usage = usage;
            _tempPage = tempPage;

            PageNumber = Original.PageNumber;
            TreeFlags = Original.TreeFlags;
            Flags = Original.Flags & ~PageFlags.Compressed;
        }

        public TreePage Original;

        public bool Cached;

        public DecompressionUsage Usage;

        public void Dispose()
        {
            if (Cached)
                return;

            if (_disposed)
                return;

            _tempPage.ReturnTemporaryPageToPool.Dispose();

            _disposed = true;
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
            }
            else
            {
                using (LeafPageCompressor.TryGetCompressedTempPage(tx, this, out var compressed, defrag: defragRequired))
                {
                    if (compressed == null)
                    {
                        if (wasModified == false)
                            return;

                        // we aren't able to compress the page back to 8KB page
                        // let's split it and try to copy it then

                        SplitPage(tx, tree);

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
                tree.FindPageFor(middleNodeKey, node: out _, cursor: out var cursorConstructor, allowCompressed: true);

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

                    using (var cursor = cursorConstructor.Build(key))
                    {
                        cursor.Update(cursor.Pages, this); // we need to use uncompressed page here because it might have some modifications (e.g. deleted node)

                        var pageSplitter = new TreePageSplitter(tx, tree, key, valueReader.Length, PageNumber, flags, cursor,
                            splittingOnDecompressed: true);

                        var pos = pageSplitter.Execute();

                        tempValueOutput.CopyTo(pos);
                    }
                }
            }
        }
    }
}
