using Sparrow;
using System.Diagnostics;
using System.Text;

namespace Voron
{
    public static class SliceExtensions
    {
        public static ByteStringContext.InternalScope ToSlice(this string str, ByteStringContext context, out Slice slice)
        {
            return ToSlice(str, context, ByteStringType.Immutable, out slice);
        }

        public static ByteStringContext.InternalScope ToSlice(this string str, ByteStringContext context, ByteStringType type, out Slice slice)
        {
            var size = Encoding.UTF8.GetByteCount(str);
            Debug.Assert(size <= ushort.MaxValue);

            var sliceWriter = new SliceWriter(size);
            sliceWriter.Write(str);

            return sliceWriter.CreateSlice(context, type, out slice);
        }

        public static ByteStringContext.InternalScope ToSliceUsingBuffer(this string str, ByteStringContext context, byte[] buffer, out Slice slice)
        {
            return ToSliceUsingBuffer(str, context, buffer, ByteStringType.Immutable, out slice);
        }

        public static ByteStringContext.InternalScope ToSliceUsingBuffer(this string str, ByteStringContext context, byte[] buffer, ByteStringType type , out Slice slice)
        {
            var sliceWriter = new SliceWriter(buffer);
            sliceWriter.Write(str);

            return sliceWriter.CreateSlice(context, type, out slice);
        }
    }
}
