using System;
using System.Runtime.InteropServices;

namespace Raven.Server.Documents
{
    [StructLayout(LayoutKind.Explicit)]
    internal struct BucketStats
    {
        [FieldOffset(0)]
        public int BucketId;

        [FieldOffset(4)]
        public int Size;

        [FieldOffset(8)]
        public int Count;

        [FieldOffset(12)]
        public long LastAccessed;
    }
}
