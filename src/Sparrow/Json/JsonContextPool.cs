using Sparrow.Platform;

namespace Sparrow.Json
{
    public class JsonContextPool : JsonContextPoolBase<JsonOperationContext>
    {
        private readonly int _maxNumberOfAllocatedStringValuesPerContext;

        public JsonContextPool()
        {
        }

        public JsonContextPool(Size? maxContextSizeToKeep)
            : this(maxContextSizeToKeep, null, PlatformDetails.Is32Bits == false ? 32 * 1024 : 8 * 1024)
        {
        }

        internal JsonContextPool(Size? maxContextSizeToKeep, long? maxNumberOfContextsToKeepInGlobalStack, int maxNumberOfAllocatedStringValuesPerContext)
            : base(maxContextSizeToKeep, maxNumberOfContextsToKeepInGlobalStack)
        {
            _maxNumberOfAllocatedStringValuesPerContext = maxNumberOfAllocatedStringValuesPerContext;
        }

        protected override JsonOperationContext CreateContext()
        {
            if (Platform.PlatformDetails.Is32Bits)
                return new JsonOperationContext(4096, 16 * 1024, _maxNumberOfAllocatedStringValuesPerContext, LowMemoryFlag);

            return new JsonOperationContext(32 * 1024, 16 * 1024, _maxNumberOfAllocatedStringValuesPerContext, LowMemoryFlag);
        }
    }
}
