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
        public class CompressionResult
        {
            public TreePage CompressedPage;

            public byte* CompressionOutputPtr;

            public CompressedValuesHeader Header;
        }

        public static IDisposable TryGetCompressedTempPage(LowLevelTransaction tx, TreePage page, out CompressionResult result, bool defrag = true)
        {
            if (defrag) // TODO arek
                page.Defrag(tx); // TODO arek no need to call it on every time probably - need to check if a page really requires defrag
            
            var valuesSize = page.PageSize - page.Upper;

            TemporaryPage temp;
            var returnTempPage = tx.Environment.GetTemporaryPage(tx, out temp);

            var tempPage = temp.GetTempPage();

            var compressionInput = page.Base + page.Upper;
            var compressionOutput = tempPage.Base + Constants.TreePageHeaderSize + Constants.CompressedValuesHeaderSize; // temp compression result has compressed values at the beginning of the page

            var compressedSize = LZ4.Encode64(
                compressionInput,
                compressionOutput,
                valuesSize,
                page.PageSize - Constants.TreePageHeaderSize + Constants.CompressedValuesHeaderSize);

            if (compressedSize == 0)
            {
                // output buffer size not enough
                result = null;
                return returnTempPage;
            }

            Memory.Copy(tempPage.Base, page.Base, Constants.TreePageHeaderSize);

            tempPage.Lower = (ushort)(Constants.TreePageHeaderSize + Constants.CompressedValuesHeaderSize + compressedSize);
            tempPage.Upper = (ushort)tempPage.PageSize;

            result = new CompressionResult
            {
                CompressedPage = tempPage,
                CompressionOutputPtr = compressionOutput,
                Header = new CompressedValuesHeader
                {
                    CompressedSize = (short) compressedSize,
                    UncompressedSize = (short) valuesSize,
                    NumberOfCompressedEntries = page.NumberOfEntries
                }
            };

            return returnTempPage;
        }

        public static void CopyToPage(CompressionResult compressed, TreePage dest)
        {
            // let us copy the compressed values at the end of the page
            // so we will handle next writes as usual

            var writePtr = dest.Base + dest.PageSize - Constants.CompressedValuesHeaderSize;

            var header = (CompressedValuesHeader*)writePtr;
            *header = compressed.Header;

            writePtr -= compressed.Header.CompressedSize;

            Memory.Copy(writePtr, compressed.CompressionOutputPtr, compressed.Header.CompressedSize);

            dest.Flags |= PageFlags.Compressed;
            dest.Lower = (ushort)Constants.TreePageHeaderSize;
            dest.Upper = (ushort)(writePtr - dest.Base);
        }
    }
}