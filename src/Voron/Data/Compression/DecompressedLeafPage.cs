using System;
using System.Diagnostics.CodeAnalysis;
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

            Page.PageNumber = Original.PageNumber;
            Page.TreeFlags = Original.TreeFlags;
            Page.Flags = Original.Flags & ~PageFlags.Compressed;
        }

        /// <summary>
        /// The decompressed page itself, held by composition rather than inheritance so that
        /// TreePage can stay a pointer-free value type.
        /// </summary>
        public TreePage Page;

        public TreePage Original;
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
            if (Page.CalcSizeUsed() < Original.PageMaxSpace)
            {
                // no need to compress
                Original.Lower = (ushort)Constants.Tree.PageHeaderSize;
                Original.Upper = (ushort)Original.PageSize;
                Original.Flags &= ~PageFlags.Compressed;

                for (var i = 0; i < Page.NumberOfEntries; i++)
                {
                    var node = Page.GetNode(i);
                    using (TreeNodeHeader.ToSlicePtr(tx.Allocator, node, out var slice))
                        Original.CopyNodeDataToEndOfPage(node, slice);
                }

                tree.DecompressionsCache.Invalidate(Page.PageNumber, DecompressionUsage.Write);
            }
            else
            {
                using (LeafPageCompressor.TryGetCompressedTempPage(tx, Page, out var compressed, defrag: defragRequired))
                {
                    if (compressed == null)
                    {
                        if (wasModified == false)
                            return;

                        if (Page.NumberOfEntries > 0)
                        {
                            // we aren't able to compress the page back to 8KB page
                            // let's split it and try to copy it then

                            SplitPage(tx, tree);
                        }
                        else
                        {
                            ThrowCouldNotCompressEmptyDecompressedPage(Page.PageNumber);
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

            var middleNodeIndex = Page.NumberOfEntries / 2;

            using (Page.GetNodeKey(tx, middleNodeIndex, out var middleNodeKey))
            {
                tree.FindPageFor(middleNodeKey, node: out _, cursor: out var cursorConstructor, allowCompressed: true);

                // let's copy key and data of a node that we'll remove

                var key = middleNodeKey.Clone(tx.Allocator);

                var node = Page.GetNode(middleNodeIndex);

                var flags = node->Flags;
                var valueReader = tree.GetValueReaderFromHeader(node);

                using (tx.Allocator.Allocate(valueReader.Length, out var tempValueOutput))
                {
                    Memory.Copy(tempValueOutput.Ptr, valueReader.Base, valueReader.Length);

                    Page.RemoveNode(middleNodeIndex);

                    Page.Search(tx, key);

                    using (var cursor = cursorConstructor.Build(key))
                    {
                        cursor.SetTopPage(Page); // we need to use uncompressed page here because it might have some modifications (e.g. deleted node)

                        var pageSplitter = new TreePageSplitter(tx, tree, key, valueReader.Length, Page.PageNumber, flags, cursor,
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
