using System.Diagnostics;
using System.Text;

namespace Voron
{
    public static class SliceExtensions
    {
        public static Slice ToSlice(this string str)
        {
            var size = Encoding.UTF8.GetByteCount(str);
            Debug.Assert(size <= ushort.MaxValue);

            var sliceWriter = new SliceWriter(size);
            sliceWriter.Write(str);

            return sliceWriter.CreateSlice();
        }

        public static Slice ToSliceUsingBuffer(this string str, byte[] buffer)
        {
            var sliceWriter = new SliceWriter(buffer);
            sliceWriter.Write(str);

            return sliceWriter.CreateSlice();
        }
    }
}
