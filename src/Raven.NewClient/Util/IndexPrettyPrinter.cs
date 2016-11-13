using System;
using System.Collections.Generic;
using System.IO;

namespace Raven.Abstractions.Util
{
    public static class IndexPrettyPrinter
    {
        public static string TryFormat(string code)
        {
            if (string.IsNullOrEmpty(code))
                return code;

            try
            {
                return FormatOrError(code);
            }
            catch (Exception)
            {
                return code;
            }
        }

        public static string FormatOrError(string code)
        {
            return code;
        }
    }
}
