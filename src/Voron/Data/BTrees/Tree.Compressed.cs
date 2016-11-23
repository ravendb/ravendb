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
        private UncompressedEntry _uncompressedEntry;

        private bool TryCompressPageNodes(Slice key, int len, TreePage page)
        {
            var alreadyCompressed = page.IsCompressed;

            if (alreadyCompressed && page.NumberOfEntries == 0) // there isn't any entry what we could compress
                return false;

            var pageToCompress = page;

            using (alreadyCompressed ? (DecompressedLeafPage)(pageToCompress = DecompressPage(page)) : null)
            {
                if (_uncompressedEntry == null)
                    _uncompressedEntry = new UncompressedEntry();

                CompressionResult result;
                using (LeafPageCompressor.TryGetCompressedTempPage(_llt, pageToCompress, out result, defrag: alreadyCompressed == false, aboutToAdd: _uncompressedEntry.Set(key, len)))
                {
                    if (result == null)
                        return false;

                    // need to check if the compressed page has space for entry that we want to insert
                    // we don't use HasSpaceFor here intentionally because underneath CalcSizeUsed could be called
                    // since we put compressed entries at the beginning of that page (temporarily) then props like NumberOfEntries and KeysOffsets
                    // return incorrect values and AccessViolationException could be thrown
                    // instead we can explicitly check SizeLeft because the page isn't fragmented

                    if (result.CompressedPage.GetRequiredSpace(key, len) > result.CompressedPage.SizeLeft) // intentionally don't use HasSpaceFor here because the
                        return false;

                    LeafPageCompressor.CopyToPage(result, page);

                    return true;
                }
            }

        }

        public DecompressedLeafPage DecompressPage(TreePage p)
        {
            var compressionHeader = p.CompressionHeader;

            var decompressedPageSize = p.SizeUsed - compressionHeader->CompressedSize +
                                compressionHeader->UncompressedSize +
                                Constants.NodeOffsetSize * compressionHeader->NumberOfCompressedEntries;

            if (decompressedPageSize > Constants.Storage.MaxPageSize)
                decompressedPageSize = Constants.Storage.MaxPageSize;
            else
                decompressedPageSize = Bits.NextPowerOf2(decompressedPageSize);

            var decompressedPage = _llt.Environment.DecompressionBuffers.GetPage(_llt, decompressedPageSize, p);

            byte* decompressedPtr;
            using (_llt.Environment.DecompressionBuffers.GetTemporaryBuffer(_llt, Bits.NextPowerOf2(compressionHeader->UncompressedSize), out decompressedPtr))
            {
                LZ4.Decode64LongBuffers(
                    (byte*)compressionHeader - compressionHeader->CompressedSize,
                    compressionHeader->CompressedSize,
                    decompressedPtr,
                    compressionHeader->UncompressedSize, true);
                
                var decompressedSize = compressionHeader->UncompressedSize;

                var offsetsSize = compressionHeader->NumberOfCompressedEntries * Constants.NodeOffsetSize;

                var decompressedOffsets = (short*) ((byte*) compressionHeader - compressionHeader->CompressedSize -
                                                       offsetsSize);

                var nodeOffset = (ushort)(decompressedPage.PageSize - decompressedSize); // TODO arek - aligntment

                // copy all decompressed nodes at once

                Memory.Copy(decompressedPage.Base + nodeOffset, decompressedPtr, decompressedSize);

                decompressedPage.Lower += (ushort)offsetsSize;
                decompressedPage.Upper = nodeOffset;

                for (var i = 0; i < compressionHeader->NumberOfCompressedEntries; i++)
                {
                    decompressedPage.KeysOffsets[i] = (ushort) (decompressedOffsets[i] + decompressedPage.Upper);
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
                                throw new NotImplementedException("TODO arek");

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
            }

            decompressedPage.DebugValidate(this, State.RootPageNumber);

            return decompressedPage;
        }

    }
}
