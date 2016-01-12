using System.Runtime.InteropServices;

namespace Voron.Data
{
    /// <summary>
    /// The PageHeader is the base information we can find in any voron allocated page. It is important to note 
    /// that specific data structures may add data to this structure and therefore every time we modify this,
    /// we should check every struct that ends with "PageHeader" to ensure no colisions happen in structures
    /// that share this layout.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 16, Pack = 1)]
    public unsafe struct PageHeader
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