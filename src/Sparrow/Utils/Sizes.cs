using System;

namespace Sparrow.Utils
{
    public static class Sizes
    {
        public static string Humane(long? size)
        {
            if (size == null)
                return null;

            var absSize = Math.Abs(size.Value);
            const double GB = 1024 * 1024 * 1024;
            const double MB = 1024 * 1024;
            const double KB = 1024;

            if (absSize >= GB) // GB
                return string.Format("{0:#,#.##} GBytes", size / GB);
            if (absSize >= MB)
                return string.Format("{0:#,#.##} MBytes", size / MB);
            if (absSize >= KB)
                return string.Format("{0:#,#.##} KBytes", size / KB);
            return string.Format("{0:#,#} Bytes", size);
        }
    }
}