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
        public static IDisposable TryGetCompressedTempPage(LowLevelTransaction tx, TreePage page, ushort version, out CompressionResult result, bool defrag = true)
        {
            if (defrag)
            {
                if (page.CalcSizeUsed() == page.SizeUsed - Constants.TreePageHeaderSize) // check if the page relly requires defrag
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

            if (compressedSize == 0 || compressedSize > valuesSize)
            {
                // output buffer size not enough or compressed output size is greater than uncompressed input

                result = null;
                return returnTempPage;
            }

            var compressedOffsets = (ushort*)compressionResult;
            var offsets = page.KeysOffsets;

            for (var i = 0; i < page.NumberOfEntries; i++)
            {
                compressedOffsets[i] = (ushort)(offsets[i] - page.Upper);
            }

            Memory.Copy(tempPage.Base, page.Base, Constants.TreePageHeaderSize);

            var alignment = compressedSize & 1;

            var compressionDataSize = compressedSize + offsetsSize + alignment;  // ensure 2-byte alignment

            tempPage.Lower = (ushort)(Constants.TreePageHeaderSize + Constants.Compression.HeaderSize + compressionDataSize);
            tempPage.Upper = (ushort)tempPage.PageSize;

            var decompressedPageSize = page.SizeUsed + // header, node offsets, existing entries
                                       (tx.Environment.Options.PageSize - (Constants.TreePageHeaderSize + Constants.Compression.HeaderSize + compressionDataSize)); // space that can be still used to insert next uncompressed entries


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
                    Version = version,
                    CompressedSize = (ushort)compressedSize,
                    UncompressedSize = (ushort)valuesSize,
                    NumberOfCompressedEntries = page.NumberOfEntries,
                }
            };

            return returnTempPage;
        }

        public static void CopyToPage(CompressionResult compressed, TreePage dest)
        {
            // let us copy the compressed values at the end of the page
            // so we will handle additional, uncompressed values as usual

            var writePtr = dest.Base + dest.PageSize - Constants.Compression.HeaderSize;

            var header = (CompressedNodesHeader*)writePtr;
            *header = compressed.Header;

            var offsetsSize = compressed.Header.NumberOfCompressedEntries * Constants.NodeOffsetSize;

            var alignment = compressed.Header.CompressedSize & 1;
            var compressionDataSize = compressed.Header.CompressedSize + offsetsSize;

            writePtr -= compressionDataSize + alignment; // ensure 2-byte alignment

            Memory.Copy(writePtr, compressed.CompressionOutputPtr, compressionDataSize);

            dest.Flags |= PageFlags.Compressed;
            dest.Lower = (ushort)Constants.TreePageHeaderSize;
            dest.Upper = (ushort)(writePtr - dest.Base);
        }
    }
}