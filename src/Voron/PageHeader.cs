using System.Runtime.InteropServices;

namespace Voron.Data
{
    /// <summary>
    /// The PageHeader is the base information we can find in any voron allocated page. It is important to note 
    /// that specific data structures may add data to this structure and therefore every time we modify this,
    /// we should check every struct that ends with "PageHeader" to ensure no colisions happen in structures
    /// that share this layout.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = SizeOf, Pack = 1)]
    public unsafe struct PageHeader
    {
        // Checksum field must be 32 bits alligned.
        // Everything before the nonce/checksum offset is considered "additional data" in the encryption algorithm and must be contiguous
        // It's important for validation, so any addition to the header should come before NonceOffset
        public static int ChecksumOffset = (int)Marshal.OffsetOf<PageHeader>(nameof(Checksum));
        public static int NonceOffset = (int)Marshal.OffsetOf<PageHeader>(nameof(Nonce));
        public static int MacOffset = (int)Marshal.OffsetOf<PageHeader>(nameof(Mac));

        public const int SizeOf = 64;

        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public int OverflowSize;

        [FieldOffset(12)]
        public PageFlags Flags;

        [FieldOffset(32)]
        public ulong Checksum;

        [FieldOffset(32)]
        public ulong Nonce;

        [FieldOffset(40)]
        public fixed byte Reserved[8];

        [FieldOffset(48)]
        public fixed byte Mac[16];
    }
}