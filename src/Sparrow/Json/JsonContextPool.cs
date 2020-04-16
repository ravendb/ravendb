namespace Sparrow.Json
{
    public class JsonContextPool : JsonContextPoolBase<JsonOperationContext>
    {
        public JsonContextPool()
        {
        }

        public JsonContextPool(Size? maxContextSizeToKeep)
            : this(maxContextSizeToKeep, null)
        {
        }

        internal JsonContextPool(Size? maxContextSizeToKeep, long? maxNumberOfContextsToKeepInGlobalStack)
            : base(maxContextSizeToKeep, maxNumberOfContextsToKeepInGlobalStack)
        {
        }

        protected override JsonOperationContext CreateContext()
        {
            if (Platform.PlatformDetails.Is32Bits)
                return new JsonOperationContext(4096, 16 * 1024, LowMemoryFlag);

            return new JsonOperationContext(32 * 1024, 16 * 1024, LowMemoryFlag);
        }
    }
}
