using System.Runtime.InteropServices;

namespace Voron.Data
{
    [StructLayout(LayoutKind.Explicit)]
    internal struct BucketStats
    {
        [FieldOffset(0)]
        public long Size;

        [FieldOffset(8)]
        public long NumberOfItems;

        [FieldOffset(16)]
        public long LastAccessedTicks;
    }
}
