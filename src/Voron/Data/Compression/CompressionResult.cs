using Voron.Data.BTrees;

namespace Voron.Data.Compression
{
    public unsafe class CompressionResult
    {
        public TreePage CompressedPage;

        public byte* CompressionOutputPtr;

        public CompressedNodesHeader Header;

        public bool InvalidateFromCache;
    }
}
