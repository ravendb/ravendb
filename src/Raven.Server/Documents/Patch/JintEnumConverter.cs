using Jint.Native;
using Jint.Runtime.Interop;

namespace Raven.Server.Documents.Patch
{
    public class JintEnumConverter : IObjectConverter
    {
        public bool TryConvert(object value, out JsValue result)
        {
            if (value.GetType().IsEnum)
            {
                result = new JsValue(value.ToString());
                return true;
            }

            result = null;
            return false;
        }
    }
}
