using System.Runtime.InteropServices;

namespace Voron.Data.Compression
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct CompressedNodesHeader
    {
        [FieldOffset(0)]
        public ushort SectionSize;

        [FieldOffset(2)]
        public ushort CompressedSize;

        [FieldOffset(4)]
        public ushort UncompressedSize;

        [FieldOffset(6)]
        public ushort NumberOfCompressedEntries;
    }
}