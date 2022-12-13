using Jint;
using Jint.Native;
using Jint.Runtime.Interop;
using Raven.Server.Documents.Indexes.Static;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch
{
    public sealed class JintStringConverter : IObjectConverter
    {
        public bool TryConvert(Engine engine, object value, out JsValue result)
        {
            if (value is StringSegment)
            {
                result = value.ToString();
                return true;
            }

            if (value is LazyStringValue lazyStringValue)
            {
                result = new LazyJsString(lazyStringValue);
                return true;
            }

            if (value is LazyCompressedStringValue lazyCompressedStringValue)
            {
                result = new LazyCompressedJsString(lazyCompressedStringValue);
                return true;
            }

            result = null;
            return false;
        }
    }
}
