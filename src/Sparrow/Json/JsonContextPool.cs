using Sparrow.LowMemory;

namespace Sparrow.Json
{
    public sealed class JsonContextPool : JsonContextPoolBase<JsonOperationContext>
    {
        public static JsonContextPool Shared = new JsonContextPool(2048, 128);

        public JsonContextPool() : base(64, 16)
        { }

        public JsonContextPool(int poolSize, int bucketSize) : base(poolSize, bucketSize)
        { }

        protected override JsonOperationContext CreateContext()
        {
            if (Platform.PlatformDetails.Is32Bits)
                return new JsonOperationContext(4096, 16 * 1024, LowMemoryFlag);
                
            return new JsonOperationContext(1024*1024, 16*1024, LowMemoryFlag);
        }
    }
}