using System.Runtime.InteropServices;

namespace Voron.Trees
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public unsafe  struct PageHeader
    {
        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public int OverflowSize;
        
        [FieldOffset(12)]
        public PageFlags Flags;

        [FieldOffset(13)] 
        public fixed byte Padding[3]; // to 16 bytes
    }
}