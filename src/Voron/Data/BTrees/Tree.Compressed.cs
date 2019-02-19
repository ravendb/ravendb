using System;
using Sparrow;
using Voron.Global;
using Voron.Impl;
using Sparrow.Binary;
using Sparrow.Compression;
using Voron.Data.Compression;

namespace Voron.Data.BTrees
{
    public unsafe partial class Tree
    {
        internal DecompressedPagesCache DecompressionsCache;

        public void InitializeCompression()
        {
            DecompressionsCache = new DecompressedPagesCache();
        }

        private bool TryCompressPageNodes(Slice key, int len, TreePage page)
        {
            var alreadyCompressed = page.IsCompressed;

            if (alreadyCompressed && page.NumberOfEntries == 0) // there isn't any entry what we could compress
                return false;

            var pageToCompress = page;

            if (alreadyCompressed)
            {
                pageToCompress = DecompressPage(page, usage: DecompressionUsage.Write); // no need to dispose, it's going to be cached anyway
            }

            CompressionResult result;
            using (LeafPageCompressor.TryGetCompressedTempPage(_llt, pageToCompress, out result, defrag: alreadyCompressed == false))
            {
                if (result == null || result.CompressedPage.GetRequiredSpace(key, len) > result.CompressedPage.SizeLeft)
                {
                    // need to check if the compressed page has space for entry that we want to insert
                    // we don't use HasSpaceFor here intentionally because underneath CalcSizeUsed could be called
                    // since we put compressed entries at the beginning of that page (temporarily) then props like NumberOfEntries and KeysOffsets
                    // return incorrect values and AccessViolationException could be thrown
                    // instead we can explicitly check SizeLeft because the page isn't fragmented

                    if (alreadyCompressed)
                    {
                        // we've just put a decompressed page to the cache however we aren't going to compress it
                        // need to invalidate it from the cache
                        DecompressionsCache.Invalidate(page.PageNumber, DecompressionUsage.Write);
                    }

                    return false;
                }

                LeafPageCompressor.CopyToPage(result, page);

                return true;
            }
        }

        public DecompressedLeafPage DecompressPage(TreePage p, DecompressionUsage usage = DecompressionUsage.Read, bool skipCache = false)
        {
            var input = new DecompressionInput(p.CompressionHeader, p);

            DecompressedLeafPage decompressedPage;
            DecompressedLeafPage cached = null;

            if (skipCache == false && DecompressionsCache.TryGet(p.PageNumber, usage, out cached))
            {
                decompressedPage = ReuseCachedPage(cached, usage, ref input);

                if (usage == DecompressionUsage.Read)
                    return decompressedPage;
            }
            else
            {
                decompressedPage = DecompressFromBuffer(usage, ref input);
            }

            try
            {
                if (p.NumberOfEntries == 0)
                    return decompressedPage;

                HandleUncompressedNodes(decompressedPage, p, usage);

                return decompressedPage;
            }
            finally
            {
                decompressedPage.DebugValidate(this, State.RootPageNumber);

                if (skipCache == false && decompressedPage != cached)
                {
                    DecompressionsCache.Invalidate(p.PageNumber, usage);
                    DecompressionsCache.Add(decompressedPage);
                }
            }
        }

        private DecompressedLeafPage DecompressFromBuffer(DecompressionUsage usage, ref DecompressionInput input)
        {
            var result = _llt.Environment.DecompressionBuffers.GetPage(_llt, input.DecompressedPageSize, usage, input.Page);

            var decompressedNodesOffset = (ushort)(result.PageSize - input.DecompressedSize);

            LZ4.Decode64LongBuffers(
                input.Data,
                input.CompressedSize,
                result.Base + decompressedNodesOffset,
                input.DecompressedSize, true);

            result.Lower += input.KeysOffsetsSize;
            result.Upper = decompressedNodesOffset;

            for (var i = 0; i < input.NumberOfEntries; i++)
            {
                result.KeysOffsets[i] = (ushort)(input.KeysOffsets[i] + result.Upper);
            }
            return result;
        }

        private DecompressedLeafPage ReuseCachedPage(DecompressedLeafPage cached, DecompressionUsage usage, ref DecompressionInput input)
        {
            DecompressedLeafPage result;

            var sizeDiff = input.DecompressedPageSize - cached.PageSize;
            if (sizeDiff > 0)
            {
                result = _llt.Environment.DecompressionBuffers.GetPage(_llt, input.DecompressedPageSize, usage, input.Page);

                Memory.Copy(result.Base, cached.Base, cached.Lower);
                Memory.Copy(result.Base + cached.Upper + sizeDiff,
                    cached.Base + cached.Upper,
                    cached.PageSize - cached.Upper);

                result.Upper += (ushort)sizeDiff;

                for (var i = 0; i < result.NumberOfEntries; i++)
                {
                    result.KeysOffsets[i] += (ushort)sizeDiff;
                }
            }
            else
                result = cached;

            return result;
        }

        private void HandleUncompressedNodes(DecompressedLeafPage decompressedPage, TreePage p, DecompressionUsage usage)
        {
            int numberOfEntries = p.NumberOfEntries;
            for (var i = 0; i < numberOfEntries; i++)
            {
                var uncompressedNode = p.GetNode(i);

                Slice nodeKey;
                using (TreeNodeHeader.ToSlicePtr(_tx.Allocator, uncompressedNode, out nodeKey))
                {
                    if (uncompressedNode->Flags == TreeNodeFlags.CompressionTombstone)
                    {
                        HandleTombstone(decompressedPage, nodeKey, usage);
                        continue;
                    }

                    if (decompressedPage.HasSpaceFor(_llt, TreeSizeOf.NodeEntry(uncompressedNode)) == false)
                        throw new InvalidOperationException("Could not add uncompressed node to decompressed page");

                    int index;

                    if (decompressedPage.NumberOfEntries > 0)
                    {
                        Slice lastKey;
                        using (decompressedPage.GetNodeKey(_llt, decompressedPage.NumberOfEntries - 1, out lastKey))
                        {
                            // optimization: it's very likely that uncompressed nodes have greater keys than compressed ones 
                            // when we insert sequential keys

                            var cmp = SliceComparer.CompareInline(nodeKey, lastKey);

                            if (cmp > 0)
                                index = decompressedPage.NumberOfEntries;
                            else
                            {
                                if (cmp == 0)
                                {
                                    // update of the last entry, just decrement NumberOfEntries in the page and
                                    // put it at the last position

                                    index = decompressedPage.NumberOfEntries - 1;
                                    decompressedPage.Lower -= Constants.Tree.NodeOffsetSize;
                                }
                                else
                                {
                                    index = decompressedPage.NodePositionFor(_llt, nodeKey);

                                    if (decompressedPage.LastMatch == 0) // update
                                    {
                                        decompressedPage.RemoveNode(index);

                                        if (usage == DecompressionUsage.Write)
                                            State.NumberOfEntries--;
                                    }
                                }
                            }
                        }
                    }
                    else
                    {
                        // all uncompressed nodes were compresion tombstones which deleted all entries from the decompressed page
                        index = 0;
                    }

                    switch (uncompressedNode->Flags)
                    {
                        case TreeNodeFlags.PageRef:
                            decompressedPage.AddPageRefNode(index, nodeKey, uncompressedNode->PageNumber);
                            break;
                        case TreeNodeFlags.Data:
                            var pos = decompressedPage.AddDataNode(index, nodeKey, uncompressedNode->DataSize);
                            var nodeValue = TreeNodeHeader.Reader(_llt, uncompressedNode);
                            Memory.Copy(pos, nodeValue.Base, nodeValue.Length);
                            break;
                        case TreeNodeFlags.MultiValuePageRef:
                            throw new NotSupportedException("Multi trees do not support compression");

                        default:
                            throw new NotSupportedException("Invalid node type to copye: " + uncompressedNode->Flags);
                    }
                }
            }
        }

        private void HandleTombstone(DecompressedLeafPage decompressedPage, Slice nodeKey, DecompressionUsage usage)
        {
            decompressedPage.Search(_llt, nodeKey);

            if (decompressedPage.LastMatch != 0)
                return;

            var node = decompressedPage.GetNode(decompressedPage.LastSearchPosition);

            if (usage == DecompressionUsage.Write)
            {
                State.NumberOfEntries--;

                if (node->Flags == TreeNodeFlags.PageRef)
                {
                    var overflowPage = GetReadOnlyTreePage(node->PageNumber);
                    FreePage(overflowPage);
                }
            }

            decompressedPage.RemoveNode(decompressedPage.LastSearchPosition);
        }

        private void DeleteOnCompressedPage(TreePage page, Slice keyToDelete, ref TreeCursorConstructor cursorConstructor)
        {
            var tombstoneNodeSize = page.GetRequiredSpace(keyToDelete, 0);

            page = ModifyPage(page);

            if (page.HasSpaceFor(_llt, tombstoneNodeSize))
            {
                if (page.LastMatch == 0)
                    RemoveLeafNode(page);

                page.AddCompressionTombstoneNode(page.LastSearchPosition, keyToDelete);
                return;
            }

            var decompressed = DecompressPage(page, usage: DecompressionUsage.Write);

            try
            {
                decompressed.Search(_llt, keyToDelete);

                if (decompressed.LastMatch != 0)
                    return;

                State.NumberOfEntries--;

                RemoveLeafNode(decompressed);

                using (var cursor = cursorConstructor.Build(keyToDelete))
                {
                    var treeRebalancer = new TreeRebalancer(_llt, this, cursor);
                    var changedPage = (TreePage)decompressed;
                    while (changedPage != null)
                    {
                        changedPage = treeRebalancer.Execute(changedPage);
                    }
                }

                page.DebugValidate(this, State.RootPageNumber);
            }
            finally
            {
                decompressed.CopyToOriginal(_llt, defragRequired: true, wasModified: true, this);
            }
        }

        public DecompressedReadResult ReadDecompressed(Slice key)
        {
            DecompressedLeafPage decompressed;
            TreeNodeHeader* node;

            if (DecompressionsCache.TryFindPageForReading(key, _llt, out decompressed))
            {
                node = decompressed.Search(_llt, key);

                if (decompressed.LastMatch != 0)
                    return null;
            }
            else
            {
                TreeCursorConstructor _;
                var page = SearchForPage(key, true, out _, out node, addToRecentlyFoundPages: false);

                if (page.IsCompressed)
                {
                    page = decompressed = DecompressPage(page);
                    node = page.Search(_llt, key);
                }

                if (page.LastMatch != 0)
                    return null;
            }

            return new DecompressedReadResult(GetValueReaderFromHeader(node), decompressed);
        }

        public struct DecompressionInput
        {
            public DecompressionInput(CompressedNodesHeader* header, TreePage p)
            {
                Page = p;
                CompressedSize = header->CompressedSize;
                DecompressedSize = header->UncompressedSize;
                NumberOfEntries = header->NumberOfCompressedEntries;

                var compressionSectionSize = header->SectionSize;

                KeysOffsetsSize = (ushort)(header->NumberOfCompressedEntries * Constants.Tree.NodeOffsetSize);
                KeysOffsets = (short*)((byte*)header - compressionSectionSize);

                Data = (byte*)header - compressionSectionSize + KeysOffsetsSize;

                var necessarySize = p.SizeUsed - compressionSectionSize - Constants.Compression.HeaderSize + DecompressedSize + KeysOffsetsSize;

                if (necessarySize > Constants.Compression.MaxPageSize)
                    DecompressedPageSize = Constants.Compression.MaxPageSize; // we are guranteed that after decompression a page won't exceed max size
                else
                    DecompressedPageSize = Bits.NextPowerOf2(necessarySize);
            }

            public readonly TreePage Page;

            public readonly int DecompressedPageSize;

            public readonly byte* Data;

            public readonly short* KeysOffsets;

            public readonly ushort KeysOffsetsSize;

            public readonly ushort CompressedSize;

            public readonly ushort DecompressedSize;

            public readonly ushort NumberOfEntries;
        }
    }
}
