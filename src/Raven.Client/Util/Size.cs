using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Util
{
    public class Size : IDynamicJson
    {
        private static readonly string ZeroHumaneSize = Humane(0);

        public Size()
        {
            HumaneSize = ZeroHumaneSize;
        }

        public Size(long sizeInBytes)
        {
            SizeInBytes = sizeInBytes;
        }

        private long _sizeInBytes;

        public long SizeInBytes
        {
            get
            {
                return _sizeInBytes;
            }

            set
            {
                _sizeInBytes = value;
                HumaneSize = Humane(value);
            }
        }

        public string HumaneSize { get; private set; }

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

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(SizeInBytes)] = SizeInBytes, 
                [nameof(HumaneSize)] = HumaneSize
            };
        }
    }
}
