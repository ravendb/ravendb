using System.Runtime.InteropServices;
using Sparrow;
using Voron;

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

        public static unsafe string GetMergedChangeVector(ValueReader reader)
        {
            if (reader.Length <= sizeof(BucketStats))
                return default;
            
            return Encodings.Utf8.GetString(reader.Base + sizeof(BucketStats), reader.Length - sizeof(BucketStats));
        }

    }
}
