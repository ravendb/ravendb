using System;
using System.Diagnostics;
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
            var returnTempPage = tx.Environment.GetTemporaryPage(tx, out TemporaryPage temp);

            var tempPage = temp.GetTempPage();

            if (page.NumberOfEntries == 0)
            {
                Memory.Copy(tempPage.Base, page.Base, Constants.Tree.PageHeaderSize);
                tempPage.Lower = Constants.Tree.PageHeaderSize;
                tempPage.Upper = (ushort)tempPage.PageSize;

                Debug.Assert(tempPage.Lower <= tempPage.Upper);

                result = new CompressionResult
                {
                    CompressedPage = tempPage,
                    CompressionOutputPtr = null,
                    Header = new CompressedNodesHeader
                    {
                        SectionSize = 0,
                        CompressedSize = 0,
                        UncompressedSize = 0,
                        NumberOfCompressedEntries = 0,
                    },
                    InvalidateFromCache = true
                };

                return returnTempPage;
            }

            if (defrag)
            {
                if (page.CalcSizeUsed() != page.SizeUsed - Constants.Tree.PageHeaderSize) // check if the page really requires defrag
                    page.Defrag(tx);
            }

            var valuesSize = page.PageSize - page.Upper;

            var compressionInput = page.Base + page.Upper;
            var compressionResult = tempPage.Base + Constants.Tree.PageHeaderSize + Constants.Compression.HeaderSize; // temp compression result has compressed values at the beginning of the page
            var offsetsSize = page.NumberOfEntries * Constants.Tree.NodeOffsetSize;

            var compressionOutput = compressionResult + offsetsSize;

            var compressedSize = LZ4.Encode64(
                compressionInput,
                compressionOutput,
                valuesSize,
                tempPage.PageSize - (Constants.Tree.PageHeaderSize + Constants.Compression.HeaderSize) - offsetsSize);

            if (compressedSize == 0 || compressedSize > valuesSize)
            {
                // output buffer size not enough or compressed output size is greater than uncompressed input

                result = null;
                return returnTempPage;
            }

            var compressedOffsets = (ushort*)compressionResult;
            var offsets = page.KeysOffsets;

            int numberOfEntries = page.NumberOfEntries;
            ushort upper = page.Upper;
            for (var i = 0; i < numberOfEntries; i++)
            {
                compressedOffsets[i] = (ushort)(offsets[i] - upper);
            }
            
            var compressionSectionSize = compressedSize + offsetsSize;

            var sizeLeftInDecompressedPage = Constants.Compression.MaxPageSize - page.SizeUsed;
            var sizeLeftForUncompressedEntries = Constants.Storage.PageSize - (Constants.Tree.PageHeaderSize + Constants.Compression.HeaderSize + compressionSectionSize);

            if (sizeLeftForUncompressedEntries > sizeLeftInDecompressedPage)
            {
                // expand compression section to prevent from adding next uncompressed entries what would result in
                // exceeding MaxPageSize after the decompression

                compressionSectionSize += sizeLeftForUncompressedEntries - sizeLeftInDecompressedPage;
            }
            
            compressionSectionSize += compressionSectionSize & 1; // ensure 2-byte alignment

            // check that after decompression we won't exceed MaxPageSize
            Debug.Assert(page.SizeUsed + // page header, node offsets, existing entries
                         (Constants.Storage.PageSize - // space that can be still used to insert next uncompressed entries
                          (Constants.Tree.PageHeaderSize + Constants.Compression.HeaderSize + compressionSectionSize)) 
                         <= Constants.Compression.MaxPageSize);

            Memory.Copy(tempPage.Base, page.Base, Constants.Tree.PageHeaderSize);
            tempPage.Lower = (ushort)(Constants.Tree.PageHeaderSize + Constants.Compression.HeaderSize + compressionSectionSize);
            tempPage.Upper = (ushort)tempPage.PageSize;

            Debug.Assert(tempPage.Lower <= tempPage.Upper);

            result = new CompressionResult
            {
                CompressedPage = tempPage,
                CompressionOutputPtr = compressionResult,
                Header = new CompressedNodesHeader
                {
                    SectionSize = (ushort)compressionSectionSize,
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

            if (compressed.CompressionOutputPtr != null)
            {
                Debug.Assert(compressed.Header.CompressedSize > 0, "compressed.Header.CompressedSize > 0, value: " + compressed.Header.CompressedSize);

                var writePtr = dest.Base + dest.PageSize - Constants.Compression.HeaderSize;

                var header = (CompressedNodesHeader*)writePtr;
                *header = compressed.Header;

                writePtr -= header->SectionSize;

                Memory.Copy(writePtr, compressed.CompressionOutputPtr,
                    compressed.Header.CompressedSize + header->NumberOfCompressedEntries * Constants.Tree.NodeOffsetSize);

                dest.Flags |= PageFlags.Compressed;
                dest.Lower = (ushort)Constants.Tree.PageHeaderSize;
                dest.Upper = (ushort)(writePtr - dest.Base);

                Debug.Assert((dest.Upper & 1) == 0);
            }
            else
            {
                Debug.Assert(compressed.Header.CompressedSize == 0, "compressed.Header.CompressedSize == 0, value: " + compressed.Header.CompressedSize);

                if (compressed.CompressedPage.NumberOfEntries != 0) 
                    ThrowNullCompressionOutputButNonEmptyPage(compressed.CompressedPage);

                dest.Lower = (ushort)Constants.Tree.PageHeaderSize;
                dest.Upper = (ushort)dest.PageSize;
                dest.Flags &= ~PageFlags.Compressed;
            }
        }

        private static void ThrowNullCompressionOutputButNonEmptyPage(TreePage page)
        {
            throw new InvalidOperationException($"{nameof(CompressionResult.CompressionOutputPtr)} was null but the page was not empty: {page}. Should never happen");
        }
    }
}
