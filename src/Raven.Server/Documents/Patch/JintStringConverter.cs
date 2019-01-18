using Jint;
using Jint.Native;
using Jint.Runtime.Interop;
using Microsoft.Extensions.Primitives;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch
{
    public class JintStringConverter : IObjectConverter
    {
        public bool TryConvert(Engine engine, object value, out JsValue result)
        {
            if (value is StringSegment ||
                value is LazyStringValue ||
                value is LazyCompressedStringValue)
            {
                result = value.ToString();
                return true;
            }

            result = null;
            return false;
        }
    }
}
