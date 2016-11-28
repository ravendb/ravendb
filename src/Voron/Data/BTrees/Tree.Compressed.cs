using System;
using System.Diagnostics;
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
            ushort version = 0;

            if (alreadyCompressed)
            {
                version = (ushort)(page.CompressionHeader->Version + 1);
                pageToCompress = DecompressPage(page); // no need to dispose, it's going to be cached anyway
            }

            CompressionResult result;
            using (LeafPageCompressor.TryGetCompressedTempPage(_llt, pageToCompress, version, out result, defrag: alreadyCompressed == false))
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
                        DecompressionsCache.Invalidate(page.PageNumber, (ushort) (version - 1));
                    }

                    return false;
                }
                
                LeafPageCompressor.CopyToPage(result, page);

                return true;
            }
        }

        public DecompressedLeafPage DecompressPage(TreePage p, bool skipCache = false)
        {
            var input = new DecompressionInput(p.CompressionHeader, p);

            DecompressedLeafPage decompressedPage;
            DecompressedLeafPage cached = null;

            if (skipCache == false && DecompressionsCache.TryGet(p.PageNumber, p.CompressionHeader->Version, out cached))
                decompressedPage = ReuseCachedPage(cached, ref input);
            else
            {
                decompressedPage = DecompressFromBuffer(ref input);
            }

            Debug.Assert(decompressedPage.NumberOfEntries > 0);

            if (p.NumberOfEntries == 0)
            {
                decompressedPage.DebugValidate(this, State.RootPageNumber);
                return decompressedPage;
            }

            AppendUncompressedNodes(decompressedPage, p);

            decompressedPage.Version++;
            
            if (skipCache == false && decompressedPage != cached)
                DecompressionsCache.Add(decompressedPage);

            decompressedPage.DebugValidate(this, State.RootPageNumber);
            return decompressedPage;
        }

        private DecompressedLeafPage DecompressFromBuffer(ref DecompressionInput input)
        {
            var result = _llt.Environment.DecompressionBuffers.GetPage(_llt, input.DecompressedPageSize, input.Page.CompressionHeader->Version, input.Page);

            var decompressedNodesOffset = (ushort)(result.PageSize - input.DecompressedSize); // TODO arek - aligntment

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

        private DecompressedLeafPage ReuseCachedPage(DecompressedLeafPage cached, ref DecompressionInput input)
        {
            DecompressedLeafPage result;

            var sizeDiff = input.DecompressedPageSize - cached.PageSize;
            if (sizeDiff > 0)
            {
                result = _llt.Environment.DecompressionBuffers.GetPage(_llt, input.DecompressedPageSize,
                    input.Page.CompressionHeader->Version, input.Page);

                Memory.Copy(result.Base, cached.Base, cached.Lower);
                Memory.Copy(result.Base + cached.Upper + sizeDiff,
                    cached.Base + cached.Upper,
                    cached.PageSize - cached.Upper);

                result.Upper += (ushort) sizeDiff;

                for (var i = 0; i < result.NumberOfEntries; i++)
                {
                    result.KeysOffsets[i] += (ushort) sizeDiff;
                }
            }
            else
                result = cached;

            return result;
        }

        private void AppendUncompressedNodes(DecompressedLeafPage decompressedPage, TreePage p)
        {
            for (var i = 0; i < p.NumberOfEntries; i++)
            {
                var uncompressedNode = p.GetNode(i);

                Slice nodeKey;
                using (TreeNodeHeader.ToSlicePtr(_tx.Allocator, uncompressedNode, out nodeKey))
                {
                    if (decompressedPage.HasSpaceFor(_llt, TreeSizeOf.NodeEntry(uncompressedNode)) == false)
                        throw new InvalidOperationException("Could not add uncompressed node to decompressed page");

                    int index;

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
                                decompressedPage.Lower -= Constants.NodeOffsetSize;
                            }
                            else
                            {
                                index = decompressedPage.NodePositionFor(_llt, nodeKey);

                                if (decompressedPage.LastMatch == 0) // update
                                    decompressedPage.RemoveNode(index);
                            }
                        }
                    }

                    switch (uncompressedNode->Flags)
                    {
                        case TreeNodeFlags.PageRef:
                            decompressedPage.AddPageRefNode(index, nodeKey, uncompressedNode->PageNumber);
                            break;
                        case TreeNodeFlags.Data:
                            var pos = decompressedPage.AddDataNode(index, nodeKey, uncompressedNode->DataSize,
                                (ushort)(uncompressedNode->Version - 1));
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

        public struct DecompressionInput
        {
            public DecompressionInput(CompressedNodesHeader* header, TreePage p)
            {
                Page = p;
                Data = (byte*)header - header->CompressedSize;

                KeysOffsetsSize = (ushort)(header->NumberOfCompressedEntries * Constants.NodeOffsetSize);
                KeysOffsets = (short*)((byte*)header - header->CompressedSize - KeysOffsetsSize);

                CompressedSize = header->CompressedSize;
                DecompressedSize = header->UncompressedSize;
                NumberOfEntries = header->NumberOfCompressedEntries;

                var necessarySize = p.SizeUsed - CompressedSize - Constants.Compression.HeaderSize + DecompressedSize;

                if (necessarySize > Constants.Storage.MaxPageSize)
                    DecompressedPageSize = Constants.Storage.MaxPageSize; // we are guranteed that after decompression a page won't exceed max size
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
