using System.Runtime.InteropServices;

namespace Raven.Server.Documents.TimeSeries
{
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public unsafe struct BitsBufferHeader
    {
        [FieldOffset(0)]
        public int UncompressedBitsPosition;
        [FieldOffset(4)]
        public ushort CompressedSize;
        [FieldOffset(6)]
        public ushort UncompressedSize;
    }
}
