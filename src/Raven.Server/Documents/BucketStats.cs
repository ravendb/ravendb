using System.Runtime.InteropServices;

namespace Raven.Server.Documents
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    internal struct BucketStats
    {
        [FieldOffset(0)]
        public long Size;

        [FieldOffset(8)]
        public long NumberOfDocuments;

        [FieldOffset(16)]
        public long LastModifiedTicks;
    }
}
