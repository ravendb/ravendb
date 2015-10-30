using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Utilities;

namespace Raven.Abstractions.Json
{
    public class RavenJsonTextReader : JsonTextReader
    {
        public RavenJsonTextReader(TextReader reader)
            : base(reader)
        {
            DateParseHandling = DateParseHandling.None;
        }

        public RavenJsonTextReader(char[] externalBuffer) : base(externalBuffer)
        {
            DateParseHandling = DateParseHandling.None;
        }

        public static DateTime ParseDateMicrosoft(string text)
        {
            string value = text.Substring(6, text.Length - 8);

            int index = value.IndexOf('+', 1);

            if (index == -1)
                index = value.IndexOf('-', 1);

            if (index != -1)
            {
                value = value.Substring(0, index);
            }

            long javaScriptTicks = long.Parse(value, NumberStyles.Integer, CultureInfo.InvariantCulture);

            DateTime utcDateTime = DateTimeUtils.ConvertJavaScriptTicksToDateTime(javaScriptTicks);
            return utcDateTime;
        }
    }
}
