using System;
using Voron.Data.BTrees;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;

namespace Voron.Data.Compression
{
    public unsafe class DecompressedLeafPage : TreePage, IDisposable
    {
        private enum Usage
        {
            None,
            PageSplitter
        }

        private readonly TreePage _original;
        private readonly TemporaryPage _tempPage;

        private Usage _usage;
        private LowLevelTransaction _tx;

        public DecompressedLeafPage(byte* basePtr, int pageSize, TreePage original, TemporaryPage tempPage) : base(basePtr, pageSize)
        {
            _original = original;
            _tempPage = tempPage;

            PageNumber = _original.PageNumber;
            TreeFlags = _original.TreeFlags;
            Flags = _original.Flags & ~PageFlags.Compressed;
        }

        public void Dispose()
        {
            switch (_usage)
            {
                case Usage.PageSplitter:
                    CopyToOriginal(defragRequired: false); // page was truncated during page split so it isn't fragmented
                    break;
                case Usage.None:
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(_usage), $"Unknown usage: {_usage}");
            }
            
            _tempPage.ReturnTemporaryPageToPool.Dispose();
        }

        public DecompressedLeafPage ForPageSplitter(LowLevelTransaction tx)
        {
            _usage = Usage.PageSplitter;
            _tx = tx;

            return this;
        }

        private void CopyToOriginal(bool defragRequired)
        {
            if (CalcSizeUsed() < _original.PageMaxSpace)
            {
                // no need to compress
                _original.Lower = (ushort)Constants.TreePageHeaderSize;
                _original.Upper = (ushort)_original.PageSize;
                _original.Flags &= ~PageFlags.Compressed;

                for (var i = 0; i < NumberOfEntries; i++)
                {
                    var node = GetNode(i);
                    Slice slice;
                    using (TreeNodeHeader.ToSlicePtr(_tx.Allocator, node, out slice))
                        _original.CopyNodeDataToEndOfPage(node, slice);
                }
            }
            else
            {
                LeafPageCompressor.CompressionResult compressed;
                using (LeafPageCompressor.TryGetCompressedTempPage(_tx, this, out compressed, defrag: defragRequired))
                {
                    if (compressed == null)
                        throw new InvalidOperationException("Could not compress a page which was already compressed. Should never happen");

                    LeafPageCompressor.CopyToPage(compressed, _original);
                }
            }
        }
    }
}