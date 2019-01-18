using System;
using Jint;
using Jint.Native;
using Jint.Runtime.Interop;

namespace Raven.Server.Documents.Patch
{
    public class JintTimeSpanConverter : IObjectConverter
    {
        public bool TryConvert(Engine engine, object value, out JsValue result)
        {
            if (value is TimeSpan timeSpan)
            {
                result = timeSpan.ToString();
                return true;
            }

            result = null;
            return false;
        }
    }
}
