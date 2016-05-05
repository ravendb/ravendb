using Sparrow;
using System.Diagnostics;
using System.Text;

namespace Voron
{
    public static class SliceExtensions
    {
        public static Slice ToSlice(this string str, ByteStringContext context, ByteStringType type = ByteStringType.Mutable)
        {
            var size = Encoding.UTF8.GetByteCount(str);
            Debug.Assert(size <= ushort.MaxValue);

            var sliceWriter = new SliceWriter(size);
            sliceWriter.Write(str);

            return sliceWriter.CreateSlice(context, type);
        }

        public static Slice ToSliceUsingBuffer(this string str, ByteStringContext context, byte[] buffer, ByteStringType type = ByteStringType.Mutable)
        {
            var sliceWriter = new SliceWriter(buffer);
            sliceWriter.Write(str);

            return sliceWriter.CreateSlice(context, type);
        }
    }
}
