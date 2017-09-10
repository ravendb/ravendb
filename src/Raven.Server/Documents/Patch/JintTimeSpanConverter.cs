using System;
using Jint.Native;
using Jint.Runtime.Interop;

namespace Raven.Server.Documents.Patch
{
    public class JintTimeSpanConverter : IObjectConverter
    {
        public bool TryConvert(object value, out JsValue result)
        {
            if (value is TimeSpan timeSpan)
            {
                result = new JsValue(timeSpan.ToString());
                return true;
            }

            result = null;
            return false;
        }
    }
}
