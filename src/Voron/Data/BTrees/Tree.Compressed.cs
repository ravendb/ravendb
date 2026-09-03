using System;
using System.Diagnostics;
using Sparrow;
using Voron.Global;
using Sparrow.Binary;
using Sparrow.Compression;
using Sparrow.Server;
using Voron.Data.Compression;

namespace Voron.Data.BTrees
{
    public unsafe partial class Tree
    {
        internal DecompressedPagesCache DecompressionsCache;

        public void InitializeCompression()
        {
            DecompressionsCache = new DecompressedPagesCache();
            _llt.RegisterDisposable(DecompressionsCache);
        }

        private bool TryCompressPageNodes(Slice key, int len, TreePage page)
        {
            var alreadyCompressed = page.IsCompressed;
            if (alreadyCompressed && page.NumberOfEntries == 0) // there isn't any entry what we could compress
                return false;

            var pageToCompress = page;

            if (alreadyCompressed) 
                pageToCompress = DecompressPage(page, usage: DecompressionUsage.Write, skipCache: false).Page; // no need to dispose, it's going to be cached anyway

            using (LeafPageCompressor.TryGetCompressedTempPage(_llt, pageToCompress, out CompressionResult result, defrag: alreadyCompressed == false))
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

                if (result.InvalidateFromCache)
                    DecompressionsCache.Invalidate(page.PageNumber, DecompressionUsage.Write);

                return true;
            }
        }

        public DecompressedLeafPage DecompressPage(TreePage p, DecompressionUsage usage, bool skipCache)
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
                decompressedPage.Page.DebugValidate(this, ReadHeader().RootPageNumber);

                if (skipCache == false && decompressedPage != cached)
                {
                    DecompressionsCache.Invalidate(p.PageNumber, usage);
                    DecompressionsCache.Add(decompressedPage);
                }
            }
        }

        internal DecompressedLeafPage GetDecompressedPage(int pageSize, DecompressionUsage usage, TreePage original)
        {
            if (pageSize < Constants.Storage.PageSize)
            {
                throw new ArgumentException(
                    $"Page cannot be smaller than {Constants.Storage.PageSize} bytes while {pageSize} bytes were requested.");
            }

            if (pageSize > Constants.Compression.MaxPageSize)
            {
                throw new ArgumentException($"Max page size is {Constants.Compression.MaxPageSize} while you requested {pageSize} bytes");
            }

            Debug.Assert(pageSize == Bits.PowerOf2(pageSize));
            
            var disposable = _llt.Allocator.Allocate(pageSize, out ByteString buffer);
            TreePage.Initialize(buffer.Ptr, pageSize);
            return new DecompressedLeafPage(buffer.Ptr, pageSize,usage, original, disposable);

        }

        private DecompressedLeafPage DecompressFromBuffer(DecompressionUsage usage, ref DecompressionInput input)
        {
            var result = GetDecompressedPage(input.DecompressedPageSize, usage, input.Page);

            var decompressedNodesOffset = (ushort)(result.Page.PageSize - input.DecompressedSize);

            if (input.CompressedSize > 0)
            {
                LZ4.Decode64LongBuffers(
                    input.Data,
                    input.CompressedSize,
                    result.Page.Base + decompressedNodesOffset,
                    input.DecompressedSize, true);
            }

            result.Page.Lower += input.KeysOffsetsSize;
            result.Page.Upper = decompressedNodesOffset;

            for (var i = 0; i < input.NumberOfEntries; i++)
            {
                result.Page.KeysOffsets[i] = (ushort)(input.KeysOffsets[i] + result.Page.Upper);
            }
            return result;
        }

        private DecompressedLeafPage ReuseCachedPage(DecompressedLeafPage cached, DecompressionUsage usage, ref DecompressionInput input)
        {

            var sizeDiff = input.DecompressedPageSize - cached.Page.PageSize;
            if (sizeDiff <= 0)
                return cached;

            var result = GetDecompressedPage(input.DecompressedPageSize, usage, input.Page);

            Memory.Copy(result.Page.Base, cached.Page.Base, cached.Page.Lower);
            Memory.Copy(result.Page.Base + cached.Page.Upper + sizeDiff,
                cached.Page.Base + cached.Page.Upper,
                cached.Page.PageSize - cached.Page.Upper);

            result.Page.Upper += (ushort)sizeDiff;

            for (var i = 0; i < result.Page.NumberOfEntries; i++)
            {
                result.Page.KeysOffsets[i] += (ushort)sizeDiff;
            }

            return result;
        }

        private void HandleUncompressedNodes(DecompressedLeafPage decompressedPage, TreePage p, DecompressionUsage usage)
        {
            int numberOfEntries = p.NumberOfEntries;
            for (var i = 0; i < numberOfEntries; i++)
            {
                var uncompressedNode = p.GetNode(i);

                using (TreeNodeHeader.ToSlicePtr(_tx.Allocator, uncompressedNode, out Slice nodeKey))
                {
                    if (uncompressedNode->Flags == TreeNodeFlags.CompressionTombstone)
                    {
                        HandleTombstone(decompressedPage, nodeKey, usage);
                        continue;
                    }

                    if (decompressedPage.Page.HasSpaceFor(_llt, TreeSizeOf.NodeEntry(uncompressedNode)) == false)
                        throw new InvalidOperationException("Could not add uncompressed node to decompressed page");

                    int index;

                    if (decompressedPage.Page.NumberOfEntries > 0)
                    {
                        using (decompressedPage.Page.GetNodeKey(_llt, decompressedPage.Page.NumberOfEntries - 1, out Slice lastKey))
                        {
                            // optimization: it's very likely that uncompressed nodes have greater keys than compressed ones 
                            // when we insert sequential keys

                            var cmp = SliceComparer.CompareInline(nodeKey, lastKey);

                            switch (cmp)
                            {
                                case > 0:
                                    index = decompressedPage.Page.NumberOfEntries;
                                    break;
                                case 0:

                                    // update of the last entry, just decrement NumberOfEntries in the page and
                                    // put it at the last position
                                    index = decompressedPage.Page.NumberOfEntries - 1;
                                    decompressedPage.Page.Lower -= Constants.Tree.NodeOffsetSize;
                                    break;
                                default:
                                {
                                    index = decompressedPage.Page.NodePositionFor(_llt, nodeKey);
                                    if (decompressedPage.Page.LastMatch == 0) // update
                                    {
                                        decompressedPage.Page.RemoveNode(index);

                                        if (usage == DecompressionUsage.Write)
                                        {
                                            ref var header = ref ModifyHeader();
                                            header.NumberOfEntries--;
                                        }
                                    }

                                    break;
                                }
                            }
                        }
                    }
                    else
                    {
                        // all uncompressed nodes were compression tombstones which deleted all entries from the decompressed page
                        index = 0;
                    }

                    switch (uncompressedNode->Flags)
                    {
                        case TreeNodeFlags.PageRef:
                            decompressedPage.Page.AddPageRefNode(index, nodeKey, uncompressedNode->PageNumber);
                            break;
                        case TreeNodeFlags.Data:
                            var pos = decompressedPage.Page.AddDataNode(index, nodeKey, uncompressedNode->DataSize);
                            var nodeValue = TreeNodeHeader.Reader(_llt, uncompressedNode);
                            Memory.Copy(pos, nodeValue.Base, nodeValue.Length);
                            break;
                        case TreeNodeFlags.MultiValuePageRef:
                            throw new NotSupportedException("Multi trees do not support compression");

                        default:
                            throw new NotSupportedException("Invalid node type to copy: " + uncompressedNode->Flags);
                    }
                }
            }
        }

        private void HandleTombstone(DecompressedLeafPage decompressedPage, Slice nodeKey, DecompressionUsage usage)
        {
            decompressedPage.Page.Search(_llt, nodeKey);

            if (decompressedPage.Page.LastMatch != 0)
                return;

            var node = decompressedPage.Page.GetNode(decompressedPage.Page.LastSearchPosition);

            if (usage == DecompressionUsage.Write)
            {
                ref var header = ref ModifyHeader();
                header.NumberOfEntries--;

                if (node->Flags == TreeNodeFlags.PageRef)
                {
                    var overflowPage = GetReadOnlyTreePage(node->PageNumber);
                    FreePage(overflowPage);
                }
            }

            decompressedPage.Page.RemoveNode(decompressedPage.Page.LastSearchPosition);
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

            var decompressed = DecompressPage(page, usage: DecompressionUsage.Write, skipCache: false);

            try
            {
                decompressed.Page.Search(_llt, keyToDelete);
                if (decompressed.Page.LastMatch != 0)
                    return;

                ref var header = ref ModifyHeader();
                header.NumberOfEntries--;

                RemoveLeafNode(decompressed.Page);

                using (var cursor = cursorConstructor.Build(keyToDelete))
                {
                    var treeRebalancer = new TreeRebalancer(_llt, this, cursor);
                    var changedPage = decompressed.Page;
                    while (changedPage.IsValid)
                    {
                        changedPage = treeRebalancer.Execute(changedPage);
                    }
                }

                page.DebugValidate(this, ReadHeader().RootPageNumber);
            }
            finally
            {
                decompressed.CopyToOriginal(_llt, defragRequired: true, wasModified: true, this);
            }
        }

        public DecompressedReadResult ReadDecompressed(Slice key)
        {
            TreeNodeHeader* node;

            if (DecompressionsCache.TryFindPageForReading(key, _llt, out DecompressedLeafPage decompressed))
            {
                node = decompressed.Page.Search(_llt, key);

                if (decompressed.Page.LastMatch != 0)
                    return null;
            }
            else
            {
                var page = SearchForPage(key, true, out _, out node, addToRecentlyFoundPages: false);

                if (page.IsCompressed)
                {
                    page = (decompressed = DecompressPage(page, DecompressionUsage.Read, skipCache: false)).Page;
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

                // we are guaranteed that after decompression a page won't exceed max size
                DecompressedPageSize = necessarySize > Constants.Compression.MaxPageSize ? Constants.Compression.MaxPageSize : Bits.PowerOf2(necessarySize);
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
