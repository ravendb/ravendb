using System.Runtime.InteropServices;

namespace Voron.Data.Fixed
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public unsafe struct FixedSizeTreeEntry
    {
        [FieldOffset(0)]
        public long Key;

        [FieldOffset(8)]
        public byte* Value;

        [FieldOffset(8)]
        public long PageNumber;
    }
}