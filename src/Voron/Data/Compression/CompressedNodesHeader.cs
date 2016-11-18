using System.Runtime.InteropServices;

namespace Voron.Data.Compression
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct CompressedNodesHeader
    {
        [FieldOffset(0)]
        public ushort CompressedSize;

        [FieldOffset(2)]
        public ushort UncompressedSize;

        [FieldOffset(4)]
        public int NumberOfCompressedEntries;
    }
}