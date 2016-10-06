using Raven.Abstractions.FileSystem;

namespace Raven.Client.Data
{
    public class Size
    {
        private static readonly string ZeroHumaneSize = FileHeader.Humane(0);

        public Size()
        {
            HumaneSize = ZeroHumaneSize;
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
                HumaneSize = FileHeader.Humane(value);
            }
        }

        public string HumaneSize { get; private set; }
    }
}