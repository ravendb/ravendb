namespace Sparrow.Json
{
    public class JsonContextPool : JsonContextPoolBase<JsonOperationContext>
    {
        public JsonContextPool()
        {
        }

        public JsonContextPool(Size? maxContextSizeToKeepInMb = null) : base(maxContextSizeToKeepInMb)
        {
        }

        protected override JsonOperationContext CreateContext()
        {
            if (Platform.PlatformDetails.Is32Bits)
                return new JsonOperationContext(4096, 16 * 1024, LowMemoryFlag);
                
            return new JsonOperationContext(32*1024, 16*1024, LowMemoryFlag);
        }
    }
}
