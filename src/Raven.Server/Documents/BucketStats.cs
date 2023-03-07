using System.Runtime.InteropServices;
using Sparrow.Server;
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

        public unsafe ByteStringContext.InternalScope GetMergedChangeVector(ByteStringContext context, ValueReader reader, out Slice changeVector)
        {
            if (reader.Length <= sizeof(BucketStats))
            {
                changeVector = default;
                return default;
            }

            return Slice.From(context,
                reader.Base + sizeof(BucketStats),
                reader.Length - sizeof(BucketStats),
                out changeVector);
        }
    }
}
