using System;
using Jint.Native;
using Jint.Runtime.Interop;

namespace Raven.Server.Documents.Patch
{
    public class JintDateTimeConverter : IObjectConverter
    {
        public bool TryConvert(object value, out JsValue result)
        {
            if (value is DateTime dateTime)
            {
                result = new JsValue(dateTime.ToString("O"));
                return true;
            }
            if (value is DateTimeOffset dateTimeOffset)
            {
                result = new JsValue(dateTimeOffset.ToString("O"));
                return true;
            }

            result = null;
            return false;
        }
    }
}