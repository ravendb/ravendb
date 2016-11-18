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

        public readonly TreePage Original;
        private readonly TemporaryPage _tempPage;

        private Usage _usage;
        private LowLevelTransaction _tx;
        private bool _disposed;
        
        public DecompressedLeafPage(byte* basePtr, int pageSize, TreePage original, TemporaryPage tempPage) : base(basePtr, pageSize)
        {
            Original = original;
            _tempPage = tempPage;

            PageNumber = Original.PageNumber;
            TreeFlags = Original.TreeFlags;
            Flags = Original.Flags & ~PageFlags.Compressed;
        }

        public void Dispose()
        {
            if (_disposed)
                return;

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

            _disposed = true;
        }

        public DecompressedLeafPage ForPageSplitter(LowLevelTransaction tx)
        {
            _usage = Usage.PageSplitter;
            _tx = tx;

            return this;
        }

        private void CopyToOriginal(bool defragRequired)
        {
            if (CalcSizeUsed() < Original.PageMaxSpace)
            {
                // no need to compress
                Original.Lower = (ushort)Constants.TreePageHeaderSize;
                Original.Upper = (ushort)Original.PageSize;
                Original.Flags &= ~PageFlags.Compressed;

                for (var i = 0; i < NumberOfEntries; i++)
                {
                    var node = GetNode(i);
                    Slice slice;
                    using (TreeNodeHeader.ToSlicePtr(_tx.Allocator, node, out slice))
                        Original.CopyNodeDataToEndOfPage(node, slice);
                }
            }
            else
            {
                LeafPageCompressor.CompressionResult compressed;
                using (LeafPageCompressor.TryGetCompressedTempPage(_tx, this, out compressed, defrag: defragRequired))
                {
                    if (compressed == null)
                        throw new InvalidOperationException("Could not compress a page which was already compressed. Should never happen");

                    LeafPageCompressor.CopyToPage(compressed, Original);
                }
            }
        }
    }
}