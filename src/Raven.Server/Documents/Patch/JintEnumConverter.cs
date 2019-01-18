using Jint;
using Jint.Native;
using Jint.Runtime.Interop;

namespace Raven.Server.Documents.Patch
{
    public class JintEnumConverter : IObjectConverter
    {
        public bool TryConvert(Engine engine, object value, out JsValue result)
        {
            if (value.GetType().IsEnum)
            {
                result = value.ToString();
                return true;
            }

            result = null;
            return false;
        }
    }
}
