using System;
using Sparrow;
using Sparrow.Compression;
using Voron.Data.BTrees;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;

namespace Voron.Data.Compression
{
    public unsafe class LeafPageCompressor
    {
        public static IDisposable TryGetCompressedTempPage(LowLevelTransaction tx, TreePage page, out CompressionResult result, bool defrag = true)
        {
            if (defrag)
            {
                if (page.SizeUsed == page.SizeUsed - Constants.TreePageHeaderSize) // check if the page relly requires defrag
                    page.Defrag(tx);
            }
            
            var valuesSize = page.PageSize - page.Upper;

            TemporaryPage temp;
            var returnTempPage = tx.Environment.GetTemporaryPage(tx, out temp);

            var tempPage = temp.GetTempPage();

            var compressionInput = page.Base + page.Upper;
            var compressionResult = tempPage.Base + Constants.TreePageHeaderSize + Constants.Compression.HeaderSize; // temp compression result has compressed values at the beginning of the page
            var offsetsSize = page.NumberOfEntries * Constants.NodeOffsetSize;

            var compressionOutput = compressionResult + offsetsSize;

            var compressedSize = LZ4.Encode64(
                compressionInput,
                compressionOutput,
                valuesSize,
                tempPage.PageSize - (Constants.TreePageHeaderSize + Constants.Compression.HeaderSize) - offsetsSize);

            if (compressedSize == 0)
            {
                // output buffer size not enough

                result = null;
                return returnTempPage;
            }

            var compressedOffsets = (ushort*) compressionResult;
            var offsets = page.KeysOffsets;

            for (var i = 0; i < page.NumberOfEntries; i++)
            {
                compressedOffsets[i] = (ushort) (offsets[i] - page.Upper);
            }
            
            Memory.Copy(tempPage.Base, page.Base, Constants.TreePageHeaderSize);

            tempPage.Lower = (ushort)(Constants.TreePageHeaderSize + Constants.Compression.HeaderSize + compressedSize + offsetsSize);
            tempPage.Upper = (ushort)tempPage.PageSize;
            
            var decompressedPageSize = page.SizeUsed + // header, node offsets, existing entries
                                       (tx.Environment.Options.PageSize - (Constants.TreePageHeaderSize + compressedSize + Constants.Compression.HeaderSize + offsetsSize)); // space that can be still used to insert next uncompressed entries


            if (decompressedPageSize > Constants.Storage.MaxPageSize)
            {
                // if we decompressed such page then it would exceed the maximum page size
                result = null;
                return returnTempPage;
            }

            result = new CompressionResult
            {
                CompressedPage = tempPage,
                CompressionOutputPtr = compressionResult,
                Header = new CompressedNodesHeader
                {
                    CompressedSize = (ushort) compressedSize,
                    UncompressedSize = (ushort) valuesSize,
                    NumberOfCompressedEntries = page.NumberOfEntries,
                }
            };

            return returnTempPage;
        }

        public static void CopyToPage(CompressionResult compressed, TreePage dest)
        {
            // let us copy the compressed values at the end of the page
            // so we will handle next writes as usual

            var writePtr = dest.Base + dest.PageSize - Constants.Compression.HeaderSize;

            var header = (CompressedNodesHeader*)writePtr;
            *header = compressed.Header;

            var offsetsSize = compressed.Header.NumberOfCompressedEntries * Constants.NodeOffsetSize;

            writePtr -= compressed.Header.CompressedSize + offsetsSize;

            Memory.Copy(writePtr, compressed.CompressionOutputPtr, compressed.Header.CompressedSize + offsetsSize);

            dest.Flags |= PageFlags.Compressed;
            dest.Lower = (ushort)Constants.TreePageHeaderSize;
            dest.Upper = (ushort)(writePtr - dest.Base); // TODO arek - alignment
        }
    }
}