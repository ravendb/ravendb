using System.Runtime.InteropServices;

namespace Raven.Server.Documents.TimeSeries
{
    [StructLayout(LayoutKind.Explicit, Size = 4)]
    public unsafe struct BitsBufferHeader
    {
        [FieldOffset(0)]
        public ushort BitsPosition;
        [FieldOffset(2)]
        public fixed byte Reserved[2];
    }
}
