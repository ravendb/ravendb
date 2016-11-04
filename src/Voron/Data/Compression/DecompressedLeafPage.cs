using System;
using Voron.Data.BTrees;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;

namespace Voron.Data.Compression
{
    public unsafe class DecompressedLeafPage : TreePage, IDisposable
    {
        private readonly TreePage _original;
        private readonly TemporaryPage _tempPage;

        public DecompressedLeafPage(byte* basePtr, int pageSize, TreePage original, TemporaryPage tempPage) : base(basePtr, pageSize)
        {
            _original = original;
            _tempPage = tempPage;

            PageNumber = _original.PageNumber;
            TreeFlags = _original.TreeFlags;
            Flags = _original.Flags ^ PageFlags.Compressed;
        }

        public void Dispose()
        {
            _tempPage.ReturnTemporaryPageToPool.Dispose();
        }

        public void CopyToOriginalPage(LowLevelTransaction tx)
        {
            if (CalcSizeUsed() < _original.PageMaxSpace)
            {
                _original.Lower = (ushort)Constants.TreePageHeaderSize;
                _original.Upper = (ushort)_original.PageSize;
                _original.Flags ^= PageFlags.Compressed;

                for (var i = 0; i < NumberOfEntries; i++)
                {
                    var node = GetNode(i);
                    Slice slice;
                    using (TreeNodeHeader.ToSlicePtr(tx.Allocator, node, out slice))
                        _original.CopyNodeDataToEndOfPage(node, slice);
                }
            }
            else
            {
                LeafPageCompressor.CompressionResult compressed;
                using (LeafPageCompressor.TryGetCompressedTempPage(tx, this, out compressed, defrag: false))
                {
                    if (compressed == null)
                        throw new InvalidOperationException("Could not compress a page which was already compressed. Should never happen");

                    LeafPageCompressor.CopyToPage(compressed, _original);
                }
            }
        }
    }
}