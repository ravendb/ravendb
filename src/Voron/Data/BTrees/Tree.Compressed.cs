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
        private bool TryCompressPageNodes(Slice key, int len, TreePage page)
        {
            var alreadyCompressed = page.IsCompressed;

            if (alreadyCompressed && page.NumberOfEntries == 0) // there isn't any entry what we could compress
                return false;

            var pageToCompress = page;

            using (alreadyCompressed ? (DecompressedLeafPage)(pageToCompress = DecompressPage(page)) : null)
            {
                CompressionResult result;
                using (LeafPageCompressor.TryGetCompressedTempPage(_llt, pageToCompress, out result, defrag: alreadyCompressed == false))
                {
                    if (result == null)
                        return false;

                    // need to check if the compressed page has space for entry that we want to insert
                    // we don't use HasSpaceFor here intentionally because underneath CalcSizeUsed could be called
                    // since we put compressed entries at the beginning of that page (temporarily) then props like NumberOfEntries and KeysOffsets
                    // return incorrect values and AccessViolationException could be thrown
                    // instead we can explicitly check SizeLeft because the page isn't fragmented

                    if (result.CompressedPage.GetRequiredSpace(key, len) > result.CompressedPage.SizeLeft)
                        return false;

                    LeafPageCompressor.CopyToPage(result, page);

                    return true;
                }
            }

        }

        public DecompressedLeafPage DecompressPage(TreePage p)
        {
            var input = new DecompressionInput(p.CompressionHeader);

            var decompressedPageSize = p.SizeUsed - input.CompressedSize - Constants.Compression.HeaderSize +
                                input.DecompressedSize;

            if (decompressedPageSize > Constants.Storage.MaxPageSize)
                decompressedPageSize = Constants.Storage.MaxPageSize;
            else
                decompressedPageSize = Bits.NextPowerOf2(decompressedPageSize);

            var decompressedPage = _llt.Environment.DecompressionBuffers.GetPage(_llt, decompressedPageSize, p);

            var decompressedNodesOffset = (ushort)(decompressedPage.PageSize - input.DecompressedSize); // TODO arek - aligntment
            
            LZ4.Decode64LongBuffers(
                input.Data,
                input.CompressedSize,
                decompressedPage.Base + decompressedNodesOffset,
                input.DecompressedSize, true);

            decompressedPage.Lower += input.KeysOffsetsSize;
            decompressedPage.Upper = decompressedNodesOffset;

            for (var i = 0; i < input.NumberOfEntries; i++)
            {
                decompressedPage.KeysOffsets[i] = (ushort) (input.KeysOffsets[i] + decompressedPage.Upper);
            }

            if (p.NumberOfEntries == 0)
            {
                decompressedPage.DebugValidate(this, State.RootPageNumber);
                return decompressedPage;
            }
                
            // copy uncompressed nodes

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
                            var pos = decompressedPage.AddDataNode(index, nodeKey, uncompressedNode->DataSize, (ushort)(uncompressedNode->Version - 1));
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

            decompressedPage.DebugValidate(this, State.RootPageNumber);

            return decompressedPage;
        }

        public struct DecompressionInput
        {
            public DecompressionInput(CompressedNodesHeader* header)
            {
                Data = (byte*)header - header->CompressedSize;

                KeysOffsetsSize = (ushort) (header->NumberOfCompressedEntries * Constants.NodeOffsetSize);
                KeysOffsets = (short*)((byte*)header - header->CompressedSize - KeysOffsetsSize);

                CompressedSize = header->CompressedSize;
                DecompressedSize = header->UncompressedSize;
                NumberOfEntries = header->NumberOfCompressedEntries;
            }

            public readonly byte* Data;

            public readonly short* KeysOffsets;

            public readonly ushort KeysOffsetsSize;

            public readonly ushort CompressedSize;

            public readonly ushort DecompressedSize;

            public readonly ushort NumberOfEntries;
        }
    }
}
