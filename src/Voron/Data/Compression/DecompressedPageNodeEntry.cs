using Sparrow;
using Voron.Data.BTrees;

namespace Voron.Data.Compression
{
    public unsafe struct DecompressedPageNodeEntry
    {
        public Slice Key;
        public TreeNodeHeader* Node;
        public ByteStringContext<ByteStringMemoryCache>.ExternalScope KeyScope;

        public override string ToString()
        {
            return $"Key: {Key}";
        }
    }
}