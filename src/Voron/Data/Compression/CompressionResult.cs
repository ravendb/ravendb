using Voron.Data.BTrees;

namespace Voron.Data.Compression
{
    public sealed unsafe class CompressionResult
    {
        public TreePage CompressedPage;

        public byte* CompressionOutputPtr;

        public CompressedNodesHeader Header;

        public bool InvalidateFromCache;
    }
}
