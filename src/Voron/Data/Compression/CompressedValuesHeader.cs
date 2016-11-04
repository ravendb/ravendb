using System.Runtime.InteropServices;

namespace Voron.Data.Compression
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct CompressedValuesHeader
    {
        [FieldOffset(0)]
        public short CompressedSize;

        [FieldOffset(2)]
        public short UncompressedSize;
    }
}