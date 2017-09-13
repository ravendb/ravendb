using System;
using Jint.Native;
using Jint.Runtime.Interop;

namespace Raven.Server.Documents.Patch
{
    public class JintGuidConverter : IObjectConverter
    {
        public bool TryConvert(object value, out JsValue result)
        {
            if (value is Guid guid)
            {
                result = new JsValue(guid.ToString());
                return true;
            }

            result = null;
            return false;
        }
    }
}
