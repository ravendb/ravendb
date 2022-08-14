using System;

namespace Raven.Client.Util
{
    public class Size
    {
        public Size()
        {
            SizeInBytes = 0;
        }
        
        public Size(long sizeInBytes)
        {
            SizeInBytes = sizeInBytes;
        }

        public long SizeInBytes { get; set; }

        public string HumaneSize => Humane(SizeInBytes);

        public static string Humane(long? size)
        {
            if (size == null)
                return null;
            
            var absSize = Math.Abs(size.Value);
            const double GB = 1024 * 1024 * 1024;
            const double MB = 1024 * 1024;
            const double KB = 1024;

            if (absSize == 0)
                return "0 Bytes";

            if (absSize > GB) // GB
                return string.Format("{0:#,#.##} GBytes", size / GB);
            if (absSize > MB)
                return string.Format("{0:#,#.##} MBytes", size / MB);
            if (absSize > KB)
                return string.Format("{0:#,#.##} KBytes", size / KB);
            return string.Format("{0:#,#0} Bytes", size);
        }
    }
}
