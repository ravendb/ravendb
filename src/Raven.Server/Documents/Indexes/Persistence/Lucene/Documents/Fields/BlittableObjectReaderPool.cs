using Sparrow;
using Sparrow.LowMemory;
using Sparrow.Platform;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.Fields
{
    public class BlittableObjectReaderPool : ILowMemoryHandler
    {
        public static BlittableObjectReaderPool Instance;

        private static readonly int PoolSize;

        private static readonly Size LowMemoryCapacityThreshold;

        private bool _isLowMemory;

        private readonly ObjectPool<BlittableObjectReader> _pool;

        static BlittableObjectReaderPool()
        {
            if (PlatformDetails.Is32Bits)
            {
                PoolSize = 128;
                LowMemoryCapacityThreshold = new Size(128, SizeUnit.Kilobytes);
            }
            else
            {
                PoolSize = 512;
                LowMemoryCapacityThreshold = new Size(256, SizeUnit.Kilobytes);
            }

            Instance = new BlittableObjectReaderPool();
        }

        private BlittableObjectReaderPool()
        {
            _pool = new ObjectPool<BlittableObjectReader>(() => new BlittableObjectReader(), PoolSize);

            LowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);
        }

        public BlittableObjectReader Allocate()
        {
            return _pool.Allocate();
        }

        public void Free(BlittableObjectReader reader)
        {
            if (_isLowMemory)
            {
                if (reader.Capacity > LowMemoryCapacityThreshold.GetValue(SizeUnit.Bytes))
                    reader.ResetCapacity();
            }

            _pool.Free(reader);
        }

        public void LowMemory()
        {
            _isLowMemory = true;
        }

        public void LowMemoryOver()
        {
            _isLowMemory = false;
        }
    }
}
